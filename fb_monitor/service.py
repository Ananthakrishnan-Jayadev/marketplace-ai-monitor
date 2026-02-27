import logging
import os
import threading
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse

from db import (
    ensure_watch_state,
    get_connection,
    get_recent_matches,
    get_recent_runs,
    get_run_errors,
    list_watch_state,
)
from main import (
    WATCHLIST,
    bootstrap_runtime_state,
    configure_logging,
    get_runtime_status,
    run_scheduler_loop,
    set_watch_pause_state,
    trigger_manual_run_async,
    validate_startup,
)


logger = logging.getLogger("fb_monitor.service")
_scheduler_stop_event = threading.Event()
_scheduler_thread: threading.Thread | None = None


def _extract_access_token(authorization: str | None, x_access_token: str | None) -> str:
    if x_access_token and x_access_token.strip():
        return x_access_token.strip()
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return ""


def _require_access_token(
    authorization: str | None = Header(default=None),
    x_access_token: str | None = Header(default=None),
) -> None:
    expected = os.environ.get("DASHBOARD_ACCESS_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="DASHBOARD_ACCESS_TOKEN is not configured.")

    provided = _extract_access_token(authorization, x_access_token)
    if not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _scheduler_thread
    configure_logging()
    validate_startup(require_dashboard_token=True)
    bootstrap_runtime_state()

    _scheduler_stop_event.clear()
    _scheduler_thread = threading.Thread(
        target=run_scheduler_loop,
        kwargs={"stop_event": _scheduler_stop_event},
        daemon=True,
        name="monitor-scheduler",
    )
    _scheduler_thread.start()
    logger.info("Scheduler thread started")

    try:
        yield
    finally:
        _scheduler_stop_event.set()
        logger.info("Stopping scheduler thread")


app = FastAPI(title="FB Monitor Service", lifespan=lifespan)


@app.get("/")
def dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard", "index.html")
    return FileResponse(dashboard_path)


@app.get("/api/health")
def api_health(_: None = Depends(_require_access_token)):
    runtime = get_runtime_status()
    conn = get_connection()
    try:
        recent_runs = get_recent_runs(conn, limit=1)
    finally:
        conn.close()
    return {
        "ok": True,
        "runtime": runtime,
        "last_run": recent_runs[0] if recent_runs else None,
    }


@app.get("/api/runs")
def api_runs(
    limit: int = Query(default=20, ge=1, le=200),
    _: None = Depends(_require_access_token),
):
    conn = get_connection()
    try:
        return {"items": get_recent_runs(conn, limit=limit)}
    finally:
        conn.close()


@app.get("/api/errors")
def api_errors(
    run_id: int | None = Query(default=None, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
    _: None = Depends(_require_access_token),
):
    conn = get_connection()
    try:
        return {"items": get_run_errors(conn, run_id=run_id, limit=limit)}
    finally:
        conn.close()


@app.get("/api/matches")
def api_matches(
    limit: int = Query(default=50, ge=1, le=500),
    _: None = Depends(_require_access_token),
):
    conn = get_connection()
    try:
        return {"items": get_recent_matches(conn, limit=limit)}
    finally:
        conn.close()


@app.get("/api/watchlist")
def api_watchlist(_: None = Depends(_require_access_token)):
    conn = get_connection()
    try:
        ensure_watch_state(conn, WATCHLIST)
        state_rows = list_watch_state(conn)
    finally:
        conn.close()

    details = []
    for row in state_rows:
        idx = int(row["watch_id"]) - 1
        entry = WATCHLIST[idx] if 0 <= idx < len(WATCHLIST) else {}
        details.append(
            {
                "watch_id": row["watch_id"],
                "product": row["product"],
                "paused": row["paused"],
                "updated_at": row["updated_at"],
                "query_prompt": entry.get("query_prompt"),
                "seed_keywords": entry.get("seed_keywords", entry.get("keywords", [])),
            }
        )
    return {"items": details}


@app.post("/api/run/trigger")
def api_run_trigger(_: None = Depends(_require_access_token)):
    result = trigger_manual_run_async()
    if not result.get("accepted"):
        raise HTTPException(status_code=409, detail=result.get("reason", "Run already active"))
    return result


@app.post("/api/watchlist/{watch_id}/pause")
def api_watch_pause(watch_id: int, _: None = Depends(_require_access_token)):
    if watch_id < 1:
        raise HTTPException(status_code=400, detail="watch_id must be >= 1")
    updated = set_watch_pause_state(watch_id, paused=True)
    if not updated:
        raise HTTPException(status_code=404, detail="watch_id not found")
    return {"ok": True, "watch_id": watch_id, "paused": True}


@app.post("/api/watchlist/{watch_id}/resume")
def api_watch_resume(watch_id: int, _: None = Depends(_require_access_token)):
    if watch_id < 1:
        raise HTTPException(status_code=400, detail="watch_id must be >= 1")
    updated = set_watch_pause_state(watch_id, paused=False)
    if not updated:
        raise HTTPException(status_code=404, detail="watch_id not found")
    return {"ok": True, "watch_id": watch_id, "paused": False}


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("SERVICE_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.environ.get("SERVICE_PORT", "8080"))
    uvicorn.run("service:app", host=host, port=port, reload=False)
