from __future__ import annotations

import json
import os
import sys
from functools import lru_cache
from typing import Any

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from sqlalchemy import text

from src.config import load_settings, project_root
from src.db.engine import get_engine
from src.db.writers import insert_dead_letter
from src.api.handlers import ingest_events, normalize_events
from src.api.security import require_api_key
from src.utils.time import utc_now

logger.remove()
logger.add(sys.stderr, level="INFO")

app = FastAPI(title="Kada Mandiya Analytics Collector", version="1.0.0")


def _cors_origins() -> list[str]:
    raw = os.getenv("ANALYTICS_CORS_ALLOW_ORIGINS")
    if raw and raw.strip():
        parts = [p.strip() for p in raw.split(",")]
        return [p for p in parts if p]
    return ["http://localhost:3000", "http://127.0.0.1:3000"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
    max_age=600,
)


@lru_cache(maxsize=1)
def _settings():
    return load_settings()


@lru_cache(maxsize=1)
def _engine():
    return get_engine(_settings())


@app.get("/")
def root() -> JSONResponse:
    return JSONResponse(
        {
            "service": app.title,
            "status": "ok",
            "endpoints": {
                "health": "/health",
                "events": "/events",
                "tracker": "/tracker.js",
                "docs": "/docs",
            },
        }
    )


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        with _engine().connect() as conn:
            conn.execute(text("SELECT 1;"))
        return {"status": "ok"}
    except Exception as exc:
        logger.error("healthcheck failed: {}", exc)
        return {"status": "degraded", "error": str(exc)}


@app.get("/tracker.js", include_in_schema=False)
def tracker_js(request: Request) -> Response:
    logger.debug(
        "tracker.js requested from {}",
        request.client.host if request.client else "unknown",
    )

    settings = _settings()
    key = settings.analytics_api_key
    key_value = key.get_secret_value() if key is not None else ""

    tracker_path = project_root() / "src" / "web_tracker" / "tracker.js"
    js = tracker_path.read_text(encoding="utf-8")

    # Safe string injection (quoted + escaped JSON string)
    injected = json.dumps(key_value)
    js = js.replace('"__KM_ANALYTICS_WEB_KEY__"', injected)

    return Response(
        content=js,
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.post("/events", status_code=202)
async def post_events(request: Request, _: None = Depends(require_api_key)) -> Response:
    received_at = utc_now()

    body_bytes = await request.body()
    if not body_bytes:
        return Response(
            status_code=202,
            content=json.dumps({"accepted": 0, "dead_lettered": 0}),
            media_type="application/json",
        )

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except Exception as exc:
        logger.warning("invalid json: {}", exc)
        try:
            with _engine().begin() as conn:
                insert_dead_letter(
                    conn,
                    source="collector",
                    reason="invalid_json",
                    payload=body_bytes.decode("utf-8", errors="replace"),
                )
        except Exception as db_exc:
            logger.error("dead-letter insert failed: {}", db_exc)
            return Response(
                status_code=503,
                content=json.dumps({"status": "unavailable"}),
                media_type="application/json",
            )
        return Response(
            status_code=202,
            content=json.dumps({"accepted": 0, "dead_lettered": 1}),
            media_type="application/json",
        )

    events = normalize_events(payload)

    try:
        with _engine().begin() as conn:
            result = ingest_events(conn, events, received_at)
    except Exception as exc:
        logger.error("ingestion failed: {}", exc)
        return Response(
            status_code=503,
            content=json.dumps({"status": "unavailable"}),
            media_type="application/json",
        )

    return Response(
        status_code=202, content=json.dumps(result), media_type="application/json"
    )
