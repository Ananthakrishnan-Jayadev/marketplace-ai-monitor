import re
from datetime import datetime

from bs4 import BeautifulSoup


def parse_listing_detail(html: str) -> dict:
    """Parse listing detail page text and extract helpful attributes."""
    soup = BeautifulSoup(html, "html.parser")
    _strip_irrelevant_nodes(soup)

    text = _collapse_whitespace(soup.get_text(" ", strip=True))
    description = _extract_description(soup, text)

    attributes = _extract_attributes_from_text(text)
    year = _extract_year(text, attributes)
    mileage_km, raw_mileage = _extract_mileage(text, attributes)

    return {
        "description": description,
        "text": text[:7000],
        "year": year,
        "mileage_km": mileage_km,
        "raw_mileage": raw_mileage,
        "attributes": attributes,
    }


def _strip_irrelevant_nodes(soup: BeautifulSoup) -> None:
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_description(soup: BeautifulSoup, fallback_text: str) -> str:
    meta = soup.find("meta", attrs={"property": "og:description"})
    if meta and meta.get("content"):
        return _collapse_whitespace(str(meta["content"]))[:1500]

    # Fallback to first meaningful text window
    return fallback_text[:1500]


def _extract_attributes_from_text(text: str) -> dict:
    attrs: dict[str, str] = {}

    patterns = {
        "year": r"\b(?:year)\s*[:\-]?\s*((?:19|20)\d{2})\b",
        "mileage": r"\b(?:mileage|odometer|kilometers|kms?)\s*[:\-]?\s*([0-9][0-9,.\s]*\s*(?:k|km|kms|mi|miles)?)\b",
        "transmission": r"\b(?:transmission)\s*[:\-]?\s*(automatic|manual|cvt)\b",
        "fuel_type": r"\b(?:fuel|fuel type)\s*[:\-]?\s*([a-z ]{3,20})\b",
    }

    lower_text = text.lower()
    for key, pattern in patterns.items():
        match = re.search(pattern, lower_text, flags=re.IGNORECASE)
        if match:
            attrs[key] = match.group(1).strip()

    return attrs


def _extract_year(text: str, attrs: dict) -> int | None:
    current_year = datetime.now().year

    if "year" in attrs:
        try:
            value = int(re.sub(r"\D", "", attrs["year"]))
            if 1900 <= value <= current_year + 1:
                return value
        except ValueError:
            pass

    candidates = re.findall(r"\b(19\d{2}|20\d{2})\b", text)
    years = [int(y) for y in candidates if 1900 <= int(y) <= current_year + 1]
    if not years:
        return None
    return max(years)


def _extract_mileage(text: str, attrs: dict) -> tuple[int | None, str | None]:
    if "mileage" in attrs:
        parsed = _parse_mileage_to_km(attrs["mileage"])
        if parsed[0] is not None:
            return parsed

    patterns = [
        r"\b(\d[\d,\.]*\s*(?:k|km|kms))\b",
        r"\b(\d[\d,\.]*\s*(?:mi|miles))\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_mileage_to_km(match.group(1))
        if parsed[0] is not None:
            return parsed

    return None, None


def _parse_mileage_to_km(raw: str) -> tuple[int | None, str | None]:
    text = raw.strip().lower().replace(" ", "")
    is_miles = "mi" in text and "km" not in text

    text = text.replace("kms", "km")
    text = text.replace("miles", "mi")
    text = text.replace(",", "")

    multiplier = 1
    if text.endswith("k"):
        multiplier = 1000
        text = text[:-1]
    text = text.replace("km", "").replace("mi", "")

    try:
        base = float(text)
    except ValueError:
        return None, None

    value = int(base * multiplier)
    if is_miles:
        value = int(value * 1.60934)

    return value, raw
