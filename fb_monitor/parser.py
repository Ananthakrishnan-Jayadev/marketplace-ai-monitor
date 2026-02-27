import re

from bs4 import BeautifulSoup


def _normalize_url(href: str) -> str:
    """Convert relative FB URLs to absolute URLs."""
    return "https://www.facebook.com" + href if href.startswith("/") else href


def _parse_price(raw_text: str) -> float | None:
    """Parse price text into float; supports 'Free' and common currency formats."""
    text = raw_text.replace("\xa0", " ").strip()
    if not text:
        return None
    if "free" in text.lower():
        return 0.0

    match = re.search(r"\d[\d,]*(?:\.\d{1,2})?", text)
    if not match:
        return None
    return float(match.group(0).replace(",", ""))


def _looks_like_price_text(text: str) -> bool:
    """Guard against non-price numbers like '2 hours ago'."""
    cleaned = text.strip().lower()
    if not cleaned:
        return False
    if "free" in cleaned or "$" in cleaned:
        return True
    if any(unit in cleaned for unit in ("hour", "minute", "day", "week", "month", "year")):
        return False
    return bool(re.fullmatch(r"\d[\d,]*(?:\.\d{1,2})?", cleaned))


def _find_card_root(anchor):
    """Find a stable parent container that likely holds all listing metadata."""
    return (
        anchor.find_parent("div", attrs={"aria-label": "Marketplace listing"})
        or anchor.find_parent("div", attrs={"role": "article"})
        or anchor.parent
    )


def _extract_title(anchor, card_root) -> str | None:
    """Extract title from anchor first, then card fallback."""
    title_el = anchor.find("span", style=lambda s: s and "-webkit-line-clamp" in s)
    if not title_el and card_root is not None:
        title_el = card_root.find("span", style=lambda s: s and "-webkit-line-clamp" in s)
    title = title_el.get_text(strip=True) if title_el else ""
    return title or None


def _extract_price(anchor, card_root) -> tuple[float | None, str]:
    """Extract price with selector and text-pattern fallbacks."""
    price_el = anchor.find("span", attrs={"dir": "auto"})
    if not price_el and card_root is not None:
        price_el = card_root.find("span", attrs={"dir": "auto"})

    if price_el:
        raw_price = price_el.get_text(" ", strip=True)
        parsed = _parse_price(raw_price) if _looks_like_price_text(raw_price) else None
        if parsed is not None:
            return parsed, raw_price

    for scope in (anchor, card_root):
        if scope is None:
            continue
        for span in scope.find_all("span"):
            raw = span.get_text(" ", strip=True)
            if not _looks_like_price_text(raw):
                continue
            parsed = _parse_price(raw)
            if parsed is not None:
                return parsed, raw

    return None, ""


def _extract_location(anchor, card_root, title: str, raw_price: str) -> str | None:
    """
    Extract location with class-based selector and fallback heuristics.
    """
    location_el = anchor.find("span", class_=lambda c: c and "xlyipyv" in c)
    if not location_el and card_root is not None:
        location_el = card_root.find("span", class_=lambda c: c and "xlyipyv" in c)
    if location_el:
        text = location_el.get_text(strip=True)
        if text:
            return text

    title_lower = title.lower()
    raw_price_lower = raw_price.lower()

    for scope in (anchor, card_root):
        if scope is None:
            continue
        for span in scope.find_all("span"):
            text = span.get_text(" ", strip=True)
            if not text:
                continue
            text_lower = text.lower()
            if text_lower in {title_lower, raw_price_lower}:
                continue
            if _parse_price(text) is not None:
                continue
            if len(text) > 64:
                continue
            if "," in text or "mile" in text_lower or "km" in text_lower:
                return text

    return None


def parse_listings(html: str) -> list[dict]:
    """
    Parse FB Marketplace search result HTML and return a list of listing dicts.
    Each dict has: title (str), price (float), location (str | None), url (str).

    Card anchor strategy: every listing card contains an <a href="/marketplace/item/...">
    which is more stable than aria-label attributes that Facebook changes frequently.
    """
    soup = BeautifulSoup(html, "html.parser")
    listings = []
    seen_urls: set[str] = set()

    # Locate cards via marketplace item links — stable URL pattern
    anchors = soup.find_all("a", href=lambda h: h and "/marketplace/item/" in h)

    for anchor in anchors:
        href = anchor.get("href")
        if not href:
            continue

        url = _normalize_url(href)
        base_url = url.split("?")[0]

        # Deduplicate: same item can have multiple anchor tags (image + title link)
        if base_url in seen_urls:
            continue
        seen_urls.add(base_url)

        card_root = _find_card_root(anchor)
        title = _extract_title(anchor, card_root)
        price, raw_price = _extract_price(anchor, card_root)
        location = _extract_location(anchor, card_root, title or "", raw_price)

        if title and price is not None:
            listings.append(
                {
                    "title": title,
                    "price": price,
                    "location": location,
                    "url": base_url,
                }
            )

    return listings
