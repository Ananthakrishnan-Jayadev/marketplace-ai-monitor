import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from time import monotonic
from typing import Any

import schedule
from dotenv import load_dotenv

from ai_filter import evaluate_listing
from browser import close_browser, launch_browser, open_listing_detail, open_search
from db import (
    add_match_history,
    add_run_error,
    ensure_watch_state,
    finish_run,
    get_connection,
    is_seen,
    is_watch_paused,
    listing_key,
    mark_seen,
    set_watch_paused,
    start_run,
)
from detail_parser import parse_listing_detail
from notifier import send_notification
from parser import parse_listings
from security import SecretRedactionFilter, redact_text


load_dotenv()
logger = logging.getLogger("fb_monitor")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

WATCHLIST: list[dict] = CONFIG.get("watchlist", [])
CHROME_USER_DATA_DIR: str = os.environ.get("CHROME_USER_DATA_DIR", "").strip()
GLOBAL_RADIUS_KM: float | None = None

DEFAULT_AI_MAX_CANDIDATES = 25
RUN_HOUR_START = 0
RUN_HOUR_END = 24

RUN_LOCK = threading.Lock()
RUN_STATE: dict[str, Any] = {
    "is_running": False,
    "started_at": None,
    "last_run_id": None,
    "last_status": "never",
    "last_error": None,
    "phase": "idle",
    "active_watch_id": None,
    "active_product": None,
    "active_keyword": None,
    "ai_progress_current": 0,
    "ai_progress_total": 0,
    "ai_progress_percent": 0.0,
    "last_progress_updated_at": None,
}


def configure_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    redaction_filter = SecretRedactionFilter()
    for handler in logging.getLogger().handlers:
        handler.addFilter(redaction_filter)


def _validate_positive_float(value: object, name: str) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a number.") from exc
    if num <= 0:
        raise ValueError(f"{name} must be > 0.")
    return num


def _validate_positive_int(value: object, name: str) -> int:
    try:
        num = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if num <= 0:
        raise ValueError(f"{name} must be > 0.")
    return num


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float))


def _get_seed_keywords(entry: dict) -> list[str]:
    seeds = entry.get("seed_keywords")
    if seeds is None:
        seeds = entry.get("keywords")
    return [kw.strip() for kw in seeds if isinstance(kw, str) and kw.strip()]


def _extract_constraints(entry: dict) -> dict[str, float | int | None]:
    """
    Extract simple deterministic bedroom/bathroom constraints from query_prompt.
    Conservative parse: only apply constraints when explicit numeric bed/bath is present.
    """
    prompt = str(entry.get("query_prompt") or "").lower()
    min_bedrooms: int | None = None
    required_bathrooms: float | None = None

    bed_match = re.search(r"\b(\d+)\s*(?:bed|beds|bedroom|bedrooms)\b", prompt)
    if bed_match:
        min_bedrooms = int(bed_match.group(1))

    bath_match = re.search(r"\b(\d+(?:\.\d+)?)\s*(?:bath|baths|bathroom|bathrooms)\b", prompt)
    if bath_match:
        required_bathrooms = float(bath_match.group(1))

    return {
        "min_bedrooms": min_bedrooms,
        "required_bathrooms": required_bathrooms,
    }


def _extract_bed_bath(text: str) -> tuple[int | None, float | None]:
    source = str(text or "").lower()
    bed_match = re.search(r"\b(\d+(?:\.\d+)?)\s*(?:bed|beds|bedroom|bedrooms)\b", source)
    bath_match = re.search(r"\b(\d+(?:\.\d+)?)\s*(?:bath|baths|bathroom|bathrooms)\b", source)

    beds: int | None = None
    baths: float | None = None
    if bed_match:
        beds = int(float(bed_match.group(1)))
    if bath_match:
        baths = float(bath_match.group(1))
    return beds, baths


def _deterministic_prefilter_title(entry: dict, listing: dict) -> tuple[bool, str | None]:
    constraints = _extract_constraints(entry)
    title = str(listing.get("title") or "")
    beds, baths = _extract_bed_bath(title)

    min_bedrooms = constraints.get("min_bedrooms")
    if isinstance(min_bedrooms, int) and beds is not None and beds < min_bedrooms:
        return False, f"Deterministic prefilter: title shows {beds} bedrooms, requires >= {min_bedrooms}."

    required_bathrooms = constraints.get("required_bathrooms")
    if isinstance(required_bathrooms, (int, float)) and baths is not None:
        if abs(float(baths) - float(required_bathrooms)) > 0.01:
            return False, (
                f"Deterministic prefilter: title shows {baths:g} baths, "
                f"requires {float(required_bathrooms):g}."
            )
    return True, None


def _deterministic_prefilter_detail(entry: dict, detail: dict) -> tuple[bool, str | None]:
    constraints = _extract_constraints(entry)
    description = str(detail.get("description") or "")
    text = str(detail.get("text") or "")
    beds, baths = _extract_bed_bath(f"{description} {text}")

    min_bedrooms = constraints.get("min_bedrooms")
    if isinstance(min_bedrooms, int) and beds is not None and beds < min_bedrooms:
        return False, f"Deterministic prefilter: detail shows {beds} bedrooms, requires >= {min_bedrooms}."

    required_bathrooms = constraints.get("required_bathrooms")
    if isinstance(required_bathrooms, (int, float)) and baths is not None:
        if abs(float(baths) - float(required_bathrooms)) > 0.01:
            return False, (
                f"Deterministic prefilter: detail shows {baths:g} baths, "
                f"requires {float(required_bathrooms):g}."
            )
    return True, None


def _price_prefilter(listing: dict, entry: dict) -> bool:
    price = listing.get("price")
    if not _is_number(price):
        return False

    min_price = entry.get("min_price")
    max_price = entry.get("max_price")
    has_min = _is_number(min_price)
    has_max = _is_number(max_price)
    if not has_min and not has_max:
        return True

    if has_min and has_max and float(min_price) == 0 and float(max_price) == 0:
        return float(price) == 0.0
    if has_min and float(price) < float(min_price):
        return False
    if has_max and float(price) > float(max_price):
        return False
    return True


def validate_startup(require_dashboard_token: bool = False) -> None:
    global GLOBAL_RADIUS_KM

    errors: list[str] = []

    if not CHROME_USER_DATA_DIR:
        errors.append("CHROME_USER_DATA_DIR is required in .env.")

    if not os.environ.get("OLLAMA_API_BASE_URL", "").strip():
        errors.append("OLLAMA_API_BASE_URL is required in .env.")
    if not os.environ.get("OLLAMA_API_KEY", "").strip():
        errors.append("OLLAMA_API_KEY is required in .env.")

    if require_dashboard_token and not os.environ.get("DASHBOARD_ACCESS_TOKEN", "").strip():
        errors.append("DASHBOARD_ACCESS_TOKEN is required for service mode.")

    raw_timeout = os.environ.get("OLLAMA_TIMEOUT_SEC", "").strip()
    if raw_timeout:
        try:
            _validate_positive_float(raw_timeout, "OLLAMA_TIMEOUT_SEC")
        except ValueError as exc:
            errors.append(str(exc))

    raw_global_radius = os.environ.get("RADIUS_KM", "").strip()
    if raw_global_radius:
        try:
            GLOBAL_RADIUS_KM = _validate_positive_float(raw_global_radius, "RADIUS_KM")
        except ValueError as exc:
            errors.append(str(exc))

    if not isinstance(WATCHLIST, list) or not WATCHLIST:
        errors.append("config.json must include a non-empty 'watchlist' array.")
    else:
        for idx, entry in enumerate(WATCHLIST, start=1):
            if not isinstance(entry, dict):
                errors.append(f"watchlist[{idx}] must be an object.")
                continue

            if not isinstance(entry.get("product"), str) or not str(entry.get("product")).strip():
                errors.append(f"watchlist[{idx}].product must be a non-empty string.")
            if not isinstance(entry.get("query_prompt"), str) or not str(entry.get("query_prompt")).strip():
                errors.append(f"watchlist[{idx}].query_prompt must be a non-empty string.")

            seed_keywords = entry.get("seed_keywords", entry.get("keywords"))
            if not isinstance(seed_keywords, list) or not seed_keywords:
                errors.append(
                    f"watchlist[{idx}].seed_keywords must be a non-empty list "
                    "(or provide legacy keywords)."
                )
            else:
                invalid = [kw for kw in seed_keywords if not isinstance(kw, str) or not kw.strip()]
                if invalid:
                    errors.append(f"watchlist[{idx}].seed_keywords must contain non-empty strings.")

            min_price = entry.get("min_price")
            max_price = entry.get("max_price")
            if min_price is not None and not _is_number(min_price):
                errors.append(f"watchlist[{idx}].min_price must be a number when provided.")
            if max_price is not None and not _is_number(max_price):
                errors.append(f"watchlist[{idx}].max_price must be a number when provided.")
            if _is_number(min_price) and float(min_price) < 0:
                errors.append(f"watchlist[{idx}].min_price must be >= 0.")
            if _is_number(max_price) and float(max_price) < 0:
                errors.append(f"watchlist[{idx}].max_price must be >= 0.")
            if _is_number(min_price) and _is_number(max_price) and float(min_price) > float(max_price):
                errors.append(f"watchlist[{idx}].min_price cannot exceed max_price.")

            if "radius_km" in entry and entry["radius_km"] is not None:
                try:
                    _validate_positive_float(entry["radius_km"], f"watchlist[{idx}].radius_km")
                except ValueError as exc:
                    errors.append(str(exc))
            if "ai_max_candidates" in entry and entry["ai_max_candidates"] is not None:
                try:
                    _validate_positive_int(entry["ai_max_candidates"], f"watchlist[{idx}].ai_max_candidates")
                except ValueError as exc:
                    errors.append(str(exc))

    if errors:
        raise ValueError("Startup validation failed:\n  - " + "\n  - ".join(errors))


def bootstrap_runtime_state() -> None:
    conn = get_connection()
    try:
        ensure_watch_state(conn, WATCHLIST)
    finally:
        conn.close()


def _record_error(
    conn,
    *,
    run_id: int | None,
    counters: dict[str, int],
    code: str,
    message: str,
    context: dict[str, Any] | None = None,
) -> None:
    counters["error_count"] += 1
    redacted = redact_text(message)
    logger.warning("%s: %s", code, redacted)
    try:
        add_run_error(
            conn,
            run_id=run_id,
            code=code,
            message_redacted=redacted,
            context=context,
        )
    except Exception as exc:
        logger.error("Failed to persist run error (%s): %s", code, redact_text(exc))


def _is_within_active_hours(now: datetime) -> bool:
    return RUN_HOUR_START <= now.hour <= RUN_HOUR_END


def _now_iso() -> str:
    return datetime.now().isoformat()


def _set_phase(
    phase: str,
    *,
    active_watch_id: int | None = None,
    active_product: str | None = None,
    active_keyword: str | None = None,
) -> None:
    RUN_STATE["phase"] = phase
    RUN_STATE["active_watch_id"] = active_watch_id
    RUN_STATE["active_product"] = active_product
    RUN_STATE["active_keyword"] = active_keyword
    RUN_STATE["last_progress_updated_at"] = _now_iso()


def _set_ai_progress(
    *,
    current: int,
    total: int,
    watch_id: int | None,
    product: str | None,
) -> None:
    safe_total = max(int(total), 0)
    safe_current = max(int(current), 0)
    if safe_total > 0:
        percent = round(min(100.0, (safe_current / safe_total) * 100.0), 1)
    else:
        percent = 0.0

    RUN_STATE["phase"] = "evaluating_ai"
    RUN_STATE["active_watch_id"] = watch_id
    RUN_STATE["active_product"] = product
    RUN_STATE["ai_progress_current"] = safe_current
    RUN_STATE["ai_progress_total"] = safe_total
    RUN_STATE["ai_progress_percent"] = percent
    RUN_STATE["last_progress_updated_at"] = _now_iso()


def _reset_ai_progress() -> None:
    RUN_STATE["ai_progress_current"] = 0
    RUN_STATE["ai_progress_total"] = 0
    RUN_STATE["ai_progress_percent"] = 0.0
    RUN_STATE["last_progress_updated_at"] = _now_iso()


def run_monitor(trigger: str = "scheduler") -> dict[str, Any]:
    acquired = RUN_LOCK.acquire(blocking=False)
    if not acquired:
        return {"status": "skipped", "reason": "already_running"}

    started_at = datetime.now()
    started_monotonic = monotonic()
    RUN_STATE["is_running"] = True
    RUN_STATE["started_at"] = started_at.isoformat()
    RUN_STATE["last_error"] = None
    _set_phase("starting")
    _reset_ai_progress()

    counters = {
        "searched_count": 0,
        "prefiltered_count": 0,
        "ai_evaluated_count": 0,
        "ai_passed_count": 0,
        "notified_count": 0,
        "skipped_seen_count": 0,
        "error_count": 0,
    }
    status = "completed"
    page = None
    run_id: int | None = None
    conn = get_connection()

    try:
        ensure_watch_state(conn, WATCHLIST)
        run_id = start_run(conn, trigger=trigger)
        RUN_STATE["last_run_id"] = run_id

        now = datetime.now()
        if not _is_within_active_hours(now):
            status = "skipped_outside_hours"
            logger.info(
                "Outside active hours (%s:00-%s:00). Skipping.",
                RUN_HOUR_START,
                RUN_HOUR_END,
            )
            return {"status": status, "run_id": run_id, "counters": counters}

        logger.info("[%s] -- Starting monitor run (%s) --", now.strftime("%Y-%m-%d %H:%M:%S"), trigger)
        page = launch_browser(CHROME_USER_DATA_DIR)
        _set_phase("searching")

        for watch_id, entry in enumerate(WATCHLIST, start=1):
            if is_watch_paused(conn, watch_id):
                logger.info("Watchlist %d paused, skipping.", watch_id)
                continue

            product = str(entry["product"]).strip()
            _set_phase("searching", active_watch_id=watch_id, active_product=product)
            seed_keywords = _get_seed_keywords(entry)
            radius_km = entry.get("radius_km", GLOBAL_RADIUS_KM)
            ai_limit = int(entry.get("ai_max_candidates", DEFAULT_AI_MAX_CANDIDATES))
            logger.info("Checking watchlist %d: %s", watch_id, product)

            candidate_map: dict[str, dict] = {}
            for keyword in seed_keywords:
                try:
                    _set_phase(
                        "searching",
                        active_watch_id=watch_id,
                        active_product=product,
                        active_keyword=keyword,
                    )
                    logger.info("Searching seed keyword: '%s'", keyword)
                    open_search(page, keyword, radius_km=radius_km)
                    html = page.content()
                    listings = parse_listings(html)
                    counters["searched_count"] += len(listings)
                    logger.info("Found %d raw listings", len(listings))
                except Exception as exc:
                    _record_error(
                        conn,
                        run_id=run_id,
                        counters=counters,
                        code="BROWSER_NAV_FAIL",
                        message=f"search '{keyword}' failed: {exc}",
                        context={"watch_id": watch_id, "keyword": keyword},
                    )
                    continue

                for listing in listings:
                    key = listing_key(listing)
                    if key in candidate_map:
                        continue
                    if is_seen(conn, key):
                        counters["skipped_seen_count"] += 1
                        continue
                    if not _price_prefilter(listing, entry):
                        continue

                    counters["prefiltered_count"] += 1
                    candidate_map[key] = listing

            candidate_items = list(candidate_map.items())[:ai_limit]
            ai_ready: list[tuple[str, dict, dict]] = []
            for key, listing in candidate_items:
                title_ok, title_reason = _deterministic_prefilter_title(entry, listing)
                if not title_ok:
                    logger.info("Deterministic prefilter rejected by title: %s", title_reason)
                    add_match_history(
                        conn,
                        run_id=run_id,
                        listing_key_value=key,
                        product=product,
                        listing=listing,
                        ai_passed=False,
                        ai_reason=title_reason,
                        ai_score=None,
                        extracted={},
                        notified=False,
                    )
                    continue

                try:
                    detail_html = open_listing_detail(page, listing["url"])
                    detail = parse_listing_detail(detail_html)
                except Exception as exc:
                    _record_error(
                        conn,
                        run_id=run_id,
                        counters=counters,
                        code="DETAIL_FETCH_FAIL",
                        message=f"detail fetch/parse failed for '{listing.get('title', '')}': {exc}",
                        context={"watch_id": watch_id, "url": listing.get("url")},
                    )
                    continue

                detail_ok, detail_reason = _deterministic_prefilter_detail(entry, detail)
                if not detail_ok:
                    logger.info("Deterministic prefilter rejected by detail: %s", detail_reason)
                    add_match_history(
                        conn,
                        run_id=run_id,
                        listing_key_value=key,
                        product=product,
                        listing=listing,
                        ai_passed=False,
                        ai_reason=detail_reason,
                        ai_score=None,
                        extracted={},
                        notified=False,
                    )
                    continue

                ai_ready.append((key, listing, detail))

            logger.info("AI evaluating %d candidate(s) after deterministic prefilter (cap: %d)", len(ai_ready), ai_limit)
            _set_ai_progress(
                current=0,
                total=len(ai_ready),
                watch_id=watch_id,
                product=product,
            )

            for idx, (key, listing, detail) in enumerate(ai_ready, start=1):
                counters["ai_evaluated_count"] += 1
                _set_ai_progress(
                    current=idx,
                    total=len(ai_ready),
                    watch_id=watch_id,
                    product=product,
                )
                logger.info(
                    "AI progress [watch_id=%d product=%s]: %d/%d",
                    watch_id,
                    product,
                    idx,
                    len(ai_ready),
                )
                try:
                    ai_result = evaluate_listing(entry, listing, detail)
                except Exception as exc:
                    _record_error(
                        conn,
                        run_id=run_id,
                        counters=counters,
                        code="AI_EVAL_FAIL",
                        message=f"AI evaluation failed for '{listing.get('title', '')}': {exc}",
                        context={"watch_id": watch_id, "url": listing.get("url")},
                    )
                    continue

                if not ai_result.passed:
                    add_match_history(
                        conn,
                        run_id=run_id,
                        listing_key_value=key,
                        product=product,
                        listing=listing,
                        ai_passed=False,
                        ai_reason=ai_result.reason,
                        ai_score=ai_result.score,
                        extracted=ai_result.extracted,
                        notified=False,
                    )
                    continue

                counters["ai_passed_count"] += 1
                _set_phase(
                    "notifying",
                    active_watch_id=watch_id,
                    active_product=product,
                    active_keyword=None,
                )
                sent = send_notification(listing, entry, ai_result=ai_result)
                if sent:
                    mark_seen(conn, key)
                    counters["notified_count"] += 1
                else:
                    _record_error(
                        conn,
                        run_id=run_id,
                        counters=counters,
                        code="NOTIFY_FAIL",
                        message=f"failed to notify for '{listing.get('title', '')}'",
                        context={"watch_id": watch_id, "url": listing.get("url")},
                    )

                add_match_history(
                    conn,
                    run_id=run_id,
                    listing_key_value=key,
                    product=product,
                    listing=listing,
                    ai_passed=True,
                    ai_reason=ai_result.reason,
                    ai_score=ai_result.score,
                    extracted=ai_result.extracted,
                    notified=sent,
                )
                _set_phase(
                    "evaluating_ai",
                    active_watch_id=watch_id,
                    active_product=product,
                    active_keyword=None,
                )

            logger.info("AI evaluation done for watchlist %d: %s", watch_id, product)
            _set_phase("searching", active_watch_id=watch_id, active_product=product, active_keyword=None)

        if counters["error_count"] > 0:
            status = "partial_success"
        return {"status": status, "run_id": run_id, "counters": counters}

    except Exception as exc:
        status = "failed"
        RUN_STATE["last_error"] = redact_text(exc)
        logger.exception("run_monitor failed: %s", exc)
        _record_error(
            conn,
            run_id=run_id,
            counters=counters,
            code="RUN_FAIL",
            message=f"run failed: {exc}",
        )
        return {"status": status, "run_id": run_id, "counters": counters}
    finally:
        duration_ms = int((monotonic() - started_monotonic) * 1000)
        if page is not None:
            close_browser()
        if run_id is not None:
            try:
                finish_run(
                    conn,
                    run_id,
                    status=status,
                    duration_ms=duration_ms,
                    counters=counters,
                )
            except Exception as exc:
                logger.error("Failed to finalize run %s: %s", run_id, redact_text(exc))
        conn.close()

        RUN_STATE["is_running"] = False
        RUN_STATE["started_at"] = None
        RUN_STATE["last_status"] = status
        _set_phase("idle", active_watch_id=None, active_product=None, active_keyword=None)
        _reset_ai_progress()
        logger.info("Run complete -- status=%s notified=%d errors=%d", status, counters["notified_count"], counters["error_count"])
        RUN_LOCK.release()


def trigger_manual_run_async() -> dict[str, Any]:
    if RUN_LOCK.locked():
        return {"accepted": False, "reason": "already_running"}

    thread = threading.Thread(target=run_monitor, kwargs={"trigger": "manual"}, daemon=True)
    thread.start()
    return {"accepted": True}


def get_runtime_status() -> dict[str, Any]:
    return dict(RUN_STATE)


def set_watch_pause_state(watch_id: int, paused: bool) -> bool:
    conn = get_connection()
    try:
        ensure_watch_state(conn, WATCHLIST)
        return set_watch_paused(conn, watch_id, paused)
    finally:
        conn.close()


def schedule_next() -> None:
    schedule.clear("monitor")
    schedule.every(45).to(75).minutes.do(_run_and_reschedule).tag("monitor")


def _run_and_reschedule() -> schedule.CancelJob:
    try:
        run_monitor(trigger="scheduler")
    except Exception as exc:
        logger.exception("Unhandled exception in monitor: %s", exc)
    schedule_next()
    return schedule.CancelJob


def run_scheduler_loop(stop_event: threading.Event | None = None) -> None:
    bootstrap_runtime_state()
    run_monitor(trigger="startup")
    schedule_next()

    while True:
        if stop_event is not None and stop_event.is_set():
            break
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    configure_logging()
    try:
        validate_startup(require_dashboard_token=False)
    except ValueError as exc:
        logger.error("[startup] %s", exc)
        raise SystemExit(1)

    logger.info("FB Marketplace Monitor starting...")
    logger.info("Watching %d product(s).", len(WATCHLIST))
    run_scheduler_loop()
