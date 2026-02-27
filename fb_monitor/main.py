import json
import logging
import os
import time
from datetime import datetime

import schedule
from dotenv import load_dotenv

from ai_filter import evaluate_listing
from browser import close_browser, launch_browser, open_listing_detail, open_search
from db import get_connection, is_seen, listing_key, mark_seen
from detail_parser import parse_listing_detail
from notifier import send_notification
from parser import parse_listings


# Startup
load_dotenv()
logger = logging.getLogger("fb_monitor")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

WATCHLIST: list[dict] = CONFIG.get("watchlist", [])
CHROME_USER_DATA_DIR: str = os.environ.get("CHROME_USER_DATA_DIR", "").strip()
GLOBAL_RADIUS_KM: float | None = None

# AI defaults
DEFAULT_AI_MAX_CANDIDATES = 25

# Time gate: only run between 08:00 and 23:00 local time
RUN_HOUR_START = 8
RUN_HOUR_END = 23


def configure_logging() -> None:
    """Configure process logging from LOG_LEVEL env var."""
    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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


def validate_startup() -> None:
    global GLOBAL_RADIUS_KM

    errors: list[str] = []

    if not CHROME_USER_DATA_DIR:
        errors.append("CHROME_USER_DATA_DIR is required in .env.")

    if not os.environ.get("OLLAMA_API_BASE_URL", "").strip():
        errors.append("OLLAMA_API_BASE_URL is required in .env.")

    if not os.environ.get("OLLAMA_API_KEY", "").strip():
        errors.append("OLLAMA_API_KEY is required in .env.")

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

            product = entry.get("product")
            if not isinstance(product, str) or not product.strip():
                errors.append(f"watchlist[{idx}].product must be a non-empty string.")

            query_prompt = entry.get("query_prompt")
            if not isinstance(query_prompt, str) or not query_prompt.strip():
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


def run_monitor() -> None:
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    if not (RUN_HOUR_START <= now.hour <= RUN_HOUR_END):
        logger.info(
            "[%s] Outside active hours (%s:00-%s:00). Skipping.",
            timestamp,
            RUN_HOUR_START,
            RUN_HOUR_END,
        )
        return

    logger.info("[%s] -- Starting monitor run --", timestamp)

    conn = get_connection()
    page = None
    total_new_matches = 0

    try:
        page = launch_browser(CHROME_USER_DATA_DIR)

        for entry in WATCHLIST:
            product = entry["product"].strip()
            seed_keywords = _get_seed_keywords(entry)
            radius_km = entry.get("radius_km", GLOBAL_RADIUS_KM)
            ai_limit = int(entry.get("ai_max_candidates", DEFAULT_AI_MAX_CANDIDATES))

            logger.info("Checking watchlist entry: %s", product)

            candidate_map: dict[str, dict] = {}
            for keyword in seed_keywords:
                logger.info("Searching seed keyword: '%s'", keyword)
                open_search(page, keyword, radius_km=radius_km)

                html = page.content()
                listings = parse_listings(html)
                logger.info("Found %d raw listings", len(listings))

                for listing in listings:
                    if not _price_prefilter(listing, entry):
                        continue

                    key = listing_key(listing)
                    if key in candidate_map:
                        continue
                    if is_seen(conn, key):
                        continue

                    candidate_map[key] = listing

            candidate_items = list(candidate_map.items())[:ai_limit]
            logger.info("AI evaluating %d candidate(s) (cap: %d)", len(candidate_items), ai_limit)

            for key, listing in candidate_items:
                try:
                    detail_html = open_listing_detail(page, listing["url"])
                    detail = parse_listing_detail(detail_html)
                    ai_result = evaluate_listing(entry, listing, detail)
                except Exception as exc:
                    logger.warning("[ai] Skipping candidate due to error: %s", exc)
                    continue

                if not ai_result.passed:
                    logger.info("[ai] Rejected: %s | %s", listing["title"][:60], ai_result.reason[:100])
                    continue

                logger.info("NEW AI match: %s - $%s", listing["title"], listing["price"])
                send_notification(listing, entry, ai_result=ai_result)
                mark_seen(conn, key)
                total_new_matches += 1

    except Exception as exc:
        logger.exception("run_monitor failed: %s", exc)
    finally:
        if page is not None:
            close_browser()
        conn.close()

    logger.info("[%s] Run complete -- %d new match(es).", datetime.now().strftime("%H:%M:%S"), total_new_matches)


def schedule_next() -> None:
    """Schedule the next run at a random interval between 45 and 75 minutes."""
    schedule.every(45).to(75).minutes.do(_run_and_reschedule)


def _run_and_reschedule() -> schedule.CancelJob:
    try:
        run_monitor()
    except Exception as exc:
        logger.exception("Unhandled exception in monitor: %s", exc)
    schedule_next()
    return schedule.CancelJob


if __name__ == "__main__":
    configure_logging()
    try:
        validate_startup()
    except ValueError as exc:
        logger.error("[startup] %s", exc)
        raise SystemExit(1)

    logger.info("FB Marketplace Monitor starting...")
    logger.info("Watching %d product(s).", len(WATCHLIST))

    run_monitor()
    schedule_next()

    while True:
        schedule.run_pending()
        time.sleep(30)
