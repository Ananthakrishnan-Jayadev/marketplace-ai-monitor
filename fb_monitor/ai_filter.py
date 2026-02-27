import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_MODEL = "minimax/m2.5"
DEFAULT_TIMEOUT_SEC = 30
MAX_PROMPT_TEXT_CHARS = 5000

logger = logging.getLogger("fb_monitor.ai")


SYSTEM_PROMPT = (
    "You evaluate Facebook Marketplace listings against user requirements.\n"
    "Decide conservatively: if critical info is missing, fail the listing.\n"
    "Return JSON if possible with keys: passed (bool), reason (str), "
    "score (number 0..1),url and extracted details."
)


@dataclass
class AIEvalResult:
    passed: bool
    reason: str
    score: float | None = None
    extracted: dict[str, Any] | None = None
    raw_output: str | None = None


def evaluate_listing(entry: dict, listing: dict, detail: dict) -> AIEvalResult:
    """Evaluate a listing against entry['query_prompt'] using Ollama Cloud."""
    criteria = str(entry.get("query_prompt", "")).strip()
    if not criteria:
        raise ValueError("Missing query_prompt in watchlist entry.")

    payload_text = _build_user_prompt(criteria, listing, detail)
    raw_output = _call_ollama(payload_text)

    parsed = _parse_json_result(raw_output)
    if parsed is not None:
        parsed.raw_output = raw_output
        return parsed

    fallback = _parse_text_result(raw_output)
    fallback.raw_output = raw_output
    return fallback


def _build_user_prompt(criteria: str, listing: dict, detail: dict) -> str:
    listing_payload = {
        "title": listing.get("title"),
        "price": listing.get("price"),
        "location": listing.get("location"),
        "url": listing.get("url"),
    }

    detail_payload = {
        "year": detail.get("year"),
        "mileage_km": detail.get("mileage_km"),
        "raw_mileage": detail.get("raw_mileage"),
        "attributes": detail.get("attributes", {}),
        "description": str(detail.get("description") or "")[:1200],
        "text_snippet": str(detail.get("text") or "")[:MAX_PROMPT_TEXT_CHARS],
    }

    return (
        "User criteria:\n"
        f"{criteria}\n\n"
        "Listing data (JSON):\n"
        f"{json.dumps(listing_payload, ensure_ascii=True)}\n\n"
        "Listing detail data (JSON):\n"
        f"{json.dumps(detail_payload, ensure_ascii=True)}\n\n"
        "Respond in JSON with keys:\n"
        '{"passed": bool, "reason": str, "score": number|null, '
        '"extracted": {"year": int|null, "mileage_km": int|null, "make_model": str|null}}'
    )


def _call_ollama(user_prompt: str) -> str:
    base_url = os.environ.get("OLLAMA_API_BASE_URL", "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("OLLAMA_API_BASE_URL is required.")

    api_key = os.environ.get("OLLAMA_API_KEY", "").strip()
    model = os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL

    timeout_raw = os.environ.get("OLLAMA_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC)).strip()
    try:
        timeout_sec = float(timeout_raw)
    except ValueError as exc:
        raise RuntimeError("OLLAMA_TIMEOUT_SEC must be numeric.") from exc
    if timeout_sec <= 0:
        raise RuntimeError("OLLAMA_TIMEOUT_SEC must be > 0.")

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "stream": False,
        "options": {"temperature": 0},
    }

    url = f"{base_url}/api/chat"
    last_error: Exception | None = None

    for attempt in range(1, 3):
        try:
            logger.debug("Calling Ollama (attempt %d) model=%s", attempt, model)
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
            if resp.status_code != 200:
                raise RuntimeError(f"Ollama returned {resp.status_code}: {resp.text[:300]}")

            body = resp.json()
            content = _extract_model_content(body)
            if not content:
                raise RuntimeError("Empty model response content.")
            logger.debug("Received Ollama response (%d chars)", len(content))
            return content
        except Exception as exc:
            last_error = exc
            logger.warning("Ollama call attempt %d failed: %s", attempt, exc)
            if attempt == 2:
                break

    raise RuntimeError(f"Ollama request failed after retries: {last_error}")


def _extract_model_content(body: dict) -> str:
    message = body.get("message")
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()

    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message")
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                return msg["content"].strip()

    response_text = body.get("response")
    if isinstance(response_text, str):
        return response_text.strip()

    return ""


def _parse_json_result(raw_text: str) -> AIEvalResult | None:
    text = raw_text.strip()
    candidates = [text]

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidates.append(text[start : end + 1])

    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        passed = _coerce_bool(obj.get("passed"))
        if passed is None:
            passed = _coerce_bool(obj.get("match"))
        if passed is None:
            passed = _coerce_bool(obj.get("approved"))
        if passed is None:
            continue

        reason = str(obj.get("reason") or obj.get("explanation") or "No reason provided").strip()
        score = _coerce_float(obj.get("score"))
        extracted_raw = obj.get("extracted")
        extracted = extracted_raw if isinstance(extracted_raw, dict) else {}
        logger.debug("Parsed AI response as JSON result")
        return AIEvalResult(passed=passed, reason=reason, score=score, extracted=extracted)

    return None


def _parse_text_result(raw_text: str) -> AIEvalResult:
    upper = raw_text.upper()
    pass_match = re.search(r"\bPASS(?:ED)?\b|\bAPPROVE(?:D)?\b|\bMATCH(?:ED)?\b", upper)
    fail_match = re.search(r"\bFAIL(?:ED)?\b|\bREJECT(?:ED)?\b|\bNO MATCH\b", upper)

    if pass_match and fail_match:
        passed = pass_match.start() < fail_match.start()
    elif pass_match:
        passed = True
    elif fail_match:
        passed = False
    else:
        # Conservative default
        passed = False

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    reason = lines[0] if lines else "Could not parse model response."
    logger.debug("Falling back to text-based AI response parsing")
    return AIEvalResult(passed=passed, reason=reason[:240], score=None, extracted={})


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in {"true", "yes", "pass", "passed", "approved", "match", "matched"}:
            return True
        if low in {"false", "no", "fail", "failed", "rejected", "reject", "no match"}:
            return False
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None
