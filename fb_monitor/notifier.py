import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger("fb_monitor.notifier")


def send_notification(listing: dict, entry: dict, ai_result: Any | None = None) -> None:
    """
    Send a Telegram message for a matched listing.

    Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment variables.
    On a non-200 response, waits 5 seconds and retries once.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        logger.warning("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID; skipping notification.")
        return

    product_name = str(entry.get("product", "Match"))
    title = str(listing.get("title", "Unknown title"))
    price = listing.get("price")
    location = str(listing.get("location") or "Unknown")
    url = str(listing.get("url", ""))
    min_price = entry.get("min_price")
    max_price = entry.get("max_price")

    price_display = _format_price(price)
    range_display = _format_range(min_price, max_price)

    ai_reason = _extract_ai_reason(ai_result)
    extracted = _extract_ai_fields(ai_result)

    message = (
        f"Match Found: {product_name}\n\n"
        f"Title: {title}\n"
        f"Price: {price_display} (Range: {range_display})\n"
        f"Location: {location}\n"
        f"URL: {url}"
    )

    if ai_reason:
        message += f"\nAI: {ai_reason}"

    if extracted:
        details = " | ".join(part for part in extracted)
        message += f"\nExtracted: {details}"

    url_api = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": False,
    }

    _post_with_retry(url_api, payload, product_name, title)


def _post_with_retry(api_url: str, payload: dict, product_name: str, title: str) -> None:
    for attempt in range(1, 3):
        try:
            resp = requests.post(api_url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("Sent notification for '%s' -- %s", product_name, title[:50])
                return
            logger.warning("Attempt %d: Telegram %s -- %s", attempt, resp.status_code, resp.text[:200])
        except requests.RequestException as exc:
            logger.warning("Attempt %d: Request error -- %s", attempt, exc)

        if attempt == 1:
            time.sleep(5)

    logger.error("Failed to send notification for '%s' after 2 attempts.", product_name)


def _format_price(price: object) -> str:
    if isinstance(price, (int, float)):
        if float(price) == 0.0:
            return "Free"
        return f"${float(price):,.2f}"
    return "Unknown"


def _format_range(min_price: object, max_price: object) -> str:
    has_min = isinstance(min_price, (int, float))
    has_max = isinstance(max_price, (int, float))

    if has_min and has_max and float(min_price) == 0 and float(max_price) == 0:
        return "Free only"
    if has_min and has_max:
        return f"${min_price} - ${max_price}"
    if has_min:
        return f">= ${min_price}"
    if has_max:
        return f"<= ${max_price}"
    return "No deterministic price filter"


def _extract_ai_reason(ai_result: Any | None) -> str:
    if ai_result is None:
        return ""
    reason = getattr(ai_result, "reason", None)
    if reason is None and isinstance(ai_result, dict):
        reason = ai_result.get("reason")
    return str(reason or "").strip()[:180]


def _extract_ai_fields(ai_result: Any | None) -> list[str]:
    if ai_result is None:
        return []

    extracted = getattr(ai_result, "extracted", None)
    if extracted is None and isinstance(ai_result, dict):
        extracted = ai_result.get("extracted")
    if not isinstance(extracted, dict):
        return []

    pieces: list[str] = []
    year = extracted.get("year")
    mileage = extracted.get("mileage_km")
    make_model = extracted.get("make_model")

    if isinstance(year, (int, float, str)) and str(year).strip():
        pieces.append(f"Year: {year}")
    if isinstance(mileage, (int, float, str)) and str(mileage).strip():
        pieces.append(f"Mileage: {mileage} km")
    if isinstance(make_model, str) and make_model.strip():
        pieces.append(f"Model: {make_model.strip()}")
    return pieces
