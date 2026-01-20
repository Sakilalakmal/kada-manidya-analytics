from __future__ import annotations

import json
import sys
from functools import lru_cache
from typing import Any

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.db.writers import insert_dead_letter
from src.api.handlers import ingest_events, normalize_events
from src.api.security import require_api_key
from src.utils.time import utc_now

logger.remove()
logger.add(sys.stderr, level="INFO")

app = FastAPI(title="Kada Mandiya Analytics Collector", version="1.0.0")


@lru_cache(maxsize=1)
def _engine():
    settings = load_settings()
    return get_engine(settings)


@app.get("/")
def root() -> JSONResponse:
    return JSONResponse(
        {
            "service": app.title,
            "status": "ok",
            "endpoints": {"health": "/health", "events": "/events", "docs": "/docs"},
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
