from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine

logger.remove()
logger.add(sys.stderr, level="INFO")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _clamp_str(value: str | None, *, max_len: int) -> str | None:
    if value is None:
        return None
    v = str(value)
    return v if len(v) <= max_len else v[:max_len]


class PageViewEventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    user_id: str | None = Field(default=None, max_length=64)
    page_url: str = Field(..., min_length=1, max_length=2000)
    referrer_url: str | None = Field(default=None, max_length=2000)
    utm_source: str | None = Field(default=None, max_length=100)
    utm_medium: str | None = Field(default=None, max_length=100)
    utm_campaign: str | None = Field(default=None, max_length=100)
    time_on_prev_page: int | None = Field(default=None, ge=0)
    properties: dict[str, Any] = Field(default_factory=dict)


class ClickEventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    user_id: str | None = Field(default=None, max_length=64)
    page_url: str = Field(..., min_length=1, max_length=2000)
    element_id: str = Field(..., min_length=1, max_length=255)
    properties: dict[str, Any] = Field(default_factory=dict)


settings = load_settings()
engine = get_engine(settings)

app = FastAPI(title="Kada Mandiya Tracking API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/track/page_view")
def track_page_view(event: PageViewEventIn) -> dict[str, str]:
    event_id = str(uuid.uuid4())
    props_json = _json_dumps(event.properties)

    params = {
        "event_id": event_id,
        "session_id": _clamp_str(event.session_id, max_len=64),
        "user_id": _clamp_str(event.user_id, max_len=64),
        "page_url": _clamp_str(event.page_url, max_len=2000),
        "referrer_url": _clamp_str(event.referrer_url, max_len=2000),
        "utm_source": _clamp_str(event.utm_source, max_len=100),
        "utm_medium": _clamp_str(event.utm_medium, max_len=100),
        "utm_campaign": _clamp_str(event.utm_campaign, max_len=100),
        "time_on_prev_page_seconds": event.time_on_prev_page,
        "properties": props_json,
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO bronze.page_view_events (
                    event_id,
                    event_timestamp,
                    session_id,
                    user_id,
                    page_url,
                    referrer_url,
                    utm_source,
                    utm_medium,
                    utm_campaign,
                    time_on_prev_page_seconds,
                    properties
                )
                VALUES (
                    :event_id,
                    SYSUTCDATETIME(),
                    :session_id,
                    :user_id,
                    :page_url,
                    :referrer_url,
                    :utm_source,
                    :utm_medium,
                    :utm_campaign,
                    :time_on_prev_page_seconds,
                    :properties
                );
                """
            ),
            params,
        )

    logger.info(
        "page_view inserted event_id={} session_id={} page_url={} ts={}",
        event_id,
        event.session_id,
        event.page_url,
        _utc_now_iso(),
    )
    return {"ok": "true", "event_id": event_id}


@app.post("/track/click")
def track_click(event: ClickEventIn, request: Request) -> dict[str, str]:
    event_id = str(uuid.uuid4())
    props_json = _json_dumps(event.properties)

    user_agent = _clamp_str(request.headers.get("user-agent"), max_len=500)
    ip_address = None
    if request.client is not None and request.client.host:
        ip_address = _clamp_str(request.client.host, max_len=64)

    params = {
        "event_id": event_id,
        "session_id": _clamp_str(event.session_id, max_len=64),
        "user_id": _clamp_str(event.user_id, max_len=64),
        "page_url": _clamp_str(event.page_url, max_len=2000),
        "element_id": _clamp_str(event.element_id, max_len=255),
        "x": None,
        "y": None,
        "viewport_w": None,
        "viewport_h": None,
        "user_agent": user_agent,
        "ip_address": ip_address,
        "properties": props_json,
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO bronze.click_events (
                    event_id,
                    event_timestamp,
                    session_id,
                    user_id,
                    page_url,
                    element_id,
                    x,
                    y,
                    viewport_w,
                    viewport_h,
                    user_agent,
                    ip_address,
                    properties
                )
                VALUES (
                    :event_id,
                    SYSUTCDATETIME(),
                    :session_id,
                    :user_id,
                    :page_url,
                    :element_id,
                    :x,
                    :y,
                    :viewport_w,
                    :viewport_h,
                    :user_agent,
                    :ip_address,
                    :properties
                );
                """
            ),
            params,
        )

    logger.info(
        "click inserted event_id={} session_id={} element_id={} page_url={} ts={}",
        event_id,
        event.session_id,
        event.element_id,
        event.page_url,
        _utc_now_iso(),
    )
    return {"ok": "true", "event_id": event_id}

