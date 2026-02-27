import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any

DB_PATH = "seen_listings.db"


def get_connection() -> sqlite3.Connection:
    """Return a reusable SQLite connection and ensure the schema exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger              TEXT NOT NULL,
            started_at           TEXT NOT NULL,
            ended_at             TEXT,
            status               TEXT NOT NULL,
            searched_count       INTEGER NOT NULL DEFAULT 0,
            prefiltered_count    INTEGER NOT NULL DEFAULT 0,
            ai_evaluated_count   INTEGER NOT NULL DEFAULT 0,
            ai_passed_count      INTEGER NOT NULL DEFAULT 0,
            notified_count       INTEGER NOT NULL DEFAULT 0,
            skipped_seen_count   INTEGER NOT NULL DEFAULT 0,
            error_count          INTEGER NOT NULL DEFAULT 0,
            duration_ms          INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_errors (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           INTEGER,
            code             TEXT NOT NULL,
            message_redacted TEXT NOT NULL,
            context_json     TEXT,
            created_at       TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER,
            listing_key     TEXT NOT NULL,
            product         TEXT NOT NULL,
            title           TEXT,
            price           REAL,
            location        TEXT,
            url             TEXT,
            ai_passed       INTEGER NOT NULL,
            ai_reason       TEXT,
            ai_score        REAL,
            extracted_json  TEXT,
            notified        INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watch_state (
            watch_id     INTEGER PRIMARY KEY,
            product      TEXT NOT NULL,
            paused       INTEGER NOT NULL DEFAULT 0,
            updated_at   TEXT NOT NULL
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


def start_run(conn: sqlite3.Connection, trigger: str = "scheduler") -> int:
    now = _utc_now()
    cur = conn.execute(
        "INSERT INTO runs (trigger, started_at, status) VALUES (?, ?, ?)",
        (trigger, now, "running"),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    duration_ms: int,
    counters: dict[str, int],
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET ended_at = ?,
            status = ?,
            searched_count = ?,
            prefiltered_count = ?,
            ai_evaluated_count = ?,
            ai_passed_count = ?,
            notified_count = ?,
            skipped_seen_count = ?,
            error_count = ?,
            duration_ms = ?
        WHERE id = ?
        """,
        (
            _utc_now(),
            status,
            int(counters.get("searched_count", 0)),
            int(counters.get("prefiltered_count", 0)),
            int(counters.get("ai_evaluated_count", 0)),
            int(counters.get("ai_passed_count", 0)),
            int(counters.get("notified_count", 0)),
            int(counters.get("skipped_seen_count", 0)),
            int(counters.get("error_count", 0)),
            int(duration_ms),
            run_id,
        ),
    )
    conn.commit()


def add_run_error(
    conn: sqlite3.Connection,
    *,
    run_id: int | None,
    code: str,
    message_redacted: str,
    context: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO run_errors (run_id, code, message_redacted, context_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            code,
            message_redacted,
            _to_json(context),
            _utc_now(),
        ),
    )
    conn.commit()


def add_match_history(
    conn: sqlite3.Connection,
    *,
    run_id: int | None,
    listing_key_value: str,
    product: str,
    listing: dict,
    ai_passed: bool,
    ai_reason: str | None,
    ai_score: float | None,
    extracted: dict[str, Any] | None,
    notified: bool,
) -> None:
    conn.execute(
        """
        INSERT INTO match_history (
            run_id, listing_key, product, title, price, location, url,
            ai_passed, ai_reason, ai_score, extracted_json, notified, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            listing_key_value,
            product,
            str(listing.get("title") or ""),
            _coerce_float(listing.get("price")),
            str(listing.get("location") or ""),
            str(listing.get("url") or ""),
            1 if ai_passed else 0,
            str(ai_reason or ""),
            ai_score,
            _to_json(extracted or {}),
            1 if notified else 0,
            _utc_now(),
        ),
    )
    conn.commit()


def ensure_watch_state(conn: sqlite3.Connection, watchlist: list[dict]) -> None:
    now = _utc_now()
    ids: list[int] = []

    for idx, entry in enumerate(watchlist, start=1):
        ids.append(idx)
        product = str(entry.get("product") or f"watch_{idx}")
        conn.execute(
            """
            INSERT INTO watch_state (watch_id, product, paused, updated_at)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(watch_id) DO UPDATE SET
                product = excluded.product
            """,
            (idx, product, now),
        )

    if ids:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(f"DELETE FROM watch_state WHERE watch_id NOT IN ({placeholders})", ids)
    else:
        conn.execute("DELETE FROM watch_state")
    conn.commit()


def is_watch_paused(conn: sqlite3.Connection, watch_id: int) -> bool:
    row = conn.execute("SELECT paused FROM watch_state WHERE watch_id = ?", (watch_id,)).fetchone()
    if row is None:
        return False
    return bool(row["paused"])


def set_watch_paused(conn: sqlite3.Connection, watch_id: int, paused: bool) -> bool:
    row = conn.execute("SELECT 1 FROM watch_state WHERE watch_id = ?", (watch_id,)).fetchone()
    if row is None:
        return False
    conn.execute(
        "UPDATE watch_state SET paused = ?, updated_at = ? WHERE watch_id = ?",
        (1 if paused else 0, _utc_now(), watch_id),
    )
    conn.commit()
    return True


def list_watch_state(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT watch_id, product, paused, updated_at FROM watch_state ORDER BY watch_id"
    ).fetchall()
    return [
        {
            "watch_id": int(row["watch_id"]),
            "product": row["product"],
            "paused": bool(row["paused"]),
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


def get_recent_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, trigger, started_at, ended_at, status,
               searched_count, prefiltered_count, ai_evaluated_count, ai_passed_count,
               notified_count, skipped_seen_count, error_count, duration_ms
        FROM runs
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(row) for row in rows]


def get_run_errors(conn: sqlite3.Connection, run_id: int | None = None, limit: int = 100) -> list[dict]:
    if run_id is None:
        rows = conn.execute(
            """
            SELECT id, run_id, code, message_redacted, context_json, created_at
            FROM run_errors
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT id, run_id, code, message_redacted, context_json, created_at
            FROM run_errors
            WHERE run_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(run_id), int(limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def get_recent_matches(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, run_id, listing_key, product, title, price, location, url,
               ai_passed, ai_reason, ai_score, extracted_json, notified, created_at
        FROM match_history
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(row) for row in rows]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=True)


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None
