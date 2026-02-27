import hashlib
import re
import sqlite3
from datetime import datetime, timezone

DB_PATH = "seen_listings.db"


def get_connection() -> sqlite3.Connection:
    """Return a reusable SQLite connection and ensure the schema exists."""
    conn = sqlite3.connect(DB_PATH)
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            hash    TEXT PRIMARY KEY,
            seen_at TEXT
        )
        """
    )
    conn.commit()


def listing_key(listing: dict) -> str:
    """
    Build a stable dedupe key for a listing.

    Priority:
    1) Marketplace item ID from URL (most stable)
    2) Canonical URL without query params
    3) Legacy title+price hash fallback
    """
    url = str(listing.get("url", "")).strip()

    match = re.search(r"/marketplace/item/([^/?#]+)", url)
    if match:
        return f"item:{match.group(1)}"

    if url:
        base_url = url.split("?")[0].strip().lower()
        if base_url:
            return f"url:{base_url}"

    raw = str(listing.get("title", "")) + str(listing.get("price", ""))
    legacy_hash = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"legacy:{legacy_hash}"


def listing_hash(listing: dict) -> str:
    """Backward-compatible alias for older imports."""
    return listing_key(listing)


def is_seen(conn: sqlite3.Connection, key: str) -> bool:
    """Return True if the listing key already exists in the seen table."""
    row = conn.execute("SELECT 1 FROM seen WHERE hash = ?", (key,)).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, key: str) -> None:
    """Insert the listing key with the current UTC timestamp into the seen table."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT OR IGNORE INTO seen (hash, seen_at) VALUES (?, ?)", (key, now))
    conn.commit()
