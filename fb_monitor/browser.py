import os
import random
import time
from urllib.parse import quote
from playwright.sync_api import sync_playwright, BrowserContext, Page

_playwright = None
_context: BrowserContext = None


def random_delay(min_sec: float, max_sec: float) -> None:
    """Sleep for a random duration between min_sec and max_sec."""
    time.sleep(random.uniform(min_sec, max_sec))


def human_scroll(page: Page, scrolls: int =15) -> None:
    """Scroll down the page in randomized increments with human-like pauses."""
    for _ in range(scrolls):
        distance = random.randint(300, 700)
        page.evaluate(f"window.scrollBy(0, {distance})")
        random_delay(1.5, 4.0)


def construct_search_url(keyword: str, radius_km: float | None = None) -> str:
    """Build a FB Marketplace search URL sorted by newest listings.

    Appends latitude, longitude, and radius when all three location values are
    available (from .env globals or the per-product radius_km override).
    """
    encoded = quote(keyword)
    base = f"https://www.facebook.com/marketplace/search?query={encoded}&sortBy=creation_time_descend"

    lat = os.environ.get("LATITUDE", "").strip()
    lng = os.environ.get("LONGITUDE", "").strip()
    # Per-product radius takes priority; fall back to global .env value
    radius = str(radius_km) if radius_km is not None else os.environ.get("RADIUS_KM", "").strip()

    if lat and lng and radius:
        base += f"&latitude={lat}&longitude={lng}&radius={radius}"

    return base


def open_search(page: Page, keyword: str, radius_km: float | None = None) -> None:
    """Navigate to the search URL, wait for load, then human-scroll."""
    url = construct_search_url(keyword, radius_km=radius_km)
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    random_delay(2.0, 4.0)
    human_scroll(page, scrolls=15)


def open_listing_detail(page: Page, url: str) -> str:
    """Open a listing detail page and return rendered HTML."""
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    random_delay(1.5, 3.0)
    return page.content()


def launch_browser(chrome_user_data_dir: str) -> Page:
    """
    Launch Chromium in non-headless mode using the real Chrome user data
    directory so the existing login session is reused.
    Returns a new page in a persistent context.
    """
    global _playwright, _context

    _playwright = sync_playwright().start()
    _context = _playwright.chromium.launch_persistent_context(
        user_data_dir=chrome_user_data_dir,
        channel="chrome",
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
    )
    page = _context.new_page()
    return page


def close_browser() -> None:
    """Close the browser and stop the Playwright instance."""
    global _playwright, _context
    if _context:
        _context.close()
        _context = None
    if _playwright:
        _playwright.stop()
        _playwright = None
