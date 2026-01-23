from __future__ import annotations

import sys
from typing import Any

import aio_pika
from fastapi import FastAPI
from loguru import logger
from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine

logger.remove()
logger.add(sys.stderr, level="INFO")

app = FastAPI(title="Kada Mandiya Analytics Consumer Health", version="1.0.0")


@app.get("/health")
async def health() -> dict[str, Any]:
    settings = load_settings()
    engine = get_engine(settings)

    out: dict[str, Any] = {"status": "ok"}

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        out["sql"] = "ok"
    except Exception as exc:
        out["sql"] = "degraded"
        out["sql_error"] = str(exc)
        out["status"] = "degraded"

    try:
        conn = await aio_pika.connect_robust(settings.rabbitmq_url)
        await conn.close()
        out["rabbitmq"] = "ok"
    except Exception as exc:
        out["rabbitmq"] = "degraded"
        out["rabbitmq_error"] = str(exc)
        out["status"] = "degraded"

    return out

