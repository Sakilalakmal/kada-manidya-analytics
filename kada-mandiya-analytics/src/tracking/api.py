from __future__ import annotations

import json
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Annotated, Literal, Union

import aio_pika
from aio_pika import ExchangeType
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
    # Use an ISO 8601 string with timezone.
    return datetime.now(timezone.utc).isoformat()


def _clamp_str(value: str | None, *, max_len: int) -> str | None:
    if value is None:
        return None
    v = str(value)
    return v if len(v) <= max_len else v[:max_len]

UI_EVENT_TYPE = Literal["page_view", "click", "add_to_cart", "begin_checkout"]

UI_EVENT_ROUTING_KEY: dict[UI_EVENT_TYPE, str] = {
    "page_view": "ui.page_view",
    "click": "ui.click",
    "add_to_cart": "ui.add_to_cart",
    "begin_checkout": "ui.begin_checkout",
}


class _RabbitPublisher:
    def __init__(
        self,
        *,
        url: str,
        exchange: str,
        exchange_type: str,
        queue: str,
        dlq: str | None,
        routing_keys: list[str],
    ) -> None:
        self._url = url
        self._exchange_name = exchange
        self._exchange_type = exchange_type
        self._queue_name = queue
        self._dlq = dlq
        self._routing_keys = routing_keys
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._exchange: aio_pika.abc.AbstractRobustExchange | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None

    def _exchange_type_value(self) -> ExchangeType:
        v = (self._exchange_type or "topic").strip().lower()
        if v == "direct":
            return ExchangeType.DIRECT
        if v == "fanout":
            return ExchangeType.FANOUT
        if v == "headers":
            return ExchangeType.HEADERS
        return ExchangeType.TOPIC

    async def _ensure_connected(self) -> None:
        if self._connection and not self._connection.is_closed:
            return
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name,
            type=self._exchange_type_value(),
            durable=True,
        )
        await self._declare_queue_and_bindings()

    async def _declare_queue_and_bindings(self) -> None:
        assert self._channel is not None
        assert self._exchange is not None

        queue_args: dict[str, object] = {}
        if self._dlq:
            dlx_name = "analytics.dlx"
            dlx = await self._channel.declare_exchange(
                dlx_name, type=ExchangeType.DIRECT, durable=True
            )
            dlq = await self._channel.declare_queue(self._dlq, durable=True)
            await dlq.bind(dlx, routing_key=self._dlq)
            queue_args["x-dead-letter-exchange"] = dlx_name
            queue_args["x-dead-letter-routing-key"] = self._dlq

        self._queue = await self._channel.declare_queue(
            self._queue_name, durable=True, arguments=queue_args or None
        )
        for rk in self._routing_keys:
            await self._queue.bind(self._exchange, routing_key=rk)

    async def publish(
        self,
        *,
        routing_key: str,
        body_obj: dict[str, Any],
        message_id: str,
        correlation_id: str | None,
        event_timestamp: datetime,
    ) -> None:
        body = json.dumps(body_obj, ensure_ascii=False, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
        msg = aio_pika.Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
            correlation_id=correlation_id,
            timestamp=event_timestamp,
        )

        try:
            await self._ensure_connected()
            assert self._exchange is not None
            await self._exchange.publish(msg, routing_key=routing_key)
            return
        except Exception:
            # Best-effort reconnect once.
            self._connection = None
            self._channel = None
            self._exchange = None
            self._queue = None
            await self._ensure_connected()
            assert self._exchange is not None
            await self._exchange.publish(msg, routing_key=routing_key)

    async def ping(self) -> None:
        await self._ensure_connected()

    async def close(self) -> None:
        try:
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
        finally:
            self._connection = None
            self._channel = None
            self._exchange = None
            self._queue = None


class PageViewEventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    user_id: str | None = Field(default=None, max_length=64)
    correlation_id: str | None = Field(default=None, max_length=64)
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
    correlation_id: str | None = Field(default=None, max_length=64)
    page_url: str = Field(..., min_length=1, max_length=2000)
    element_id: str = Field(..., min_length=1, max_length=255)
    properties: dict[str, Any] = Field(default_factory=dict)

class AddToCartEventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    user_id: str | None = Field(default=None, max_length=64)
    correlation_id: str | None = Field(default=None, max_length=64)
    page_url: str = Field(..., min_length=1, max_length=2000)
    product_id: str = Field(..., min_length=1, max_length=128)
    quantity: int = Field(default=1, ge=1, le=1000)
    element_id: str | None = Field(default="btn_add_to_cart", max_length=255)
    properties: dict[str, Any] = Field(default_factory=dict)


class BeginCheckoutEventIn(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=64)
    user_id: str | None = Field(default=None, max_length=64)
    correlation_id: str | None = Field(default=None, max_length=64)
    page_url: str = Field(..., min_length=1, max_length=2000)
    order_id: str | None = Field(default=None, max_length=128)
    element_id: str | None = Field(default="btn_checkout", max_length=255)
    properties: dict[str, Any] = Field(default_factory=dict)


class TrackPageViewEventIn(PageViewEventIn):
    event_type: Literal["page_view"] = "page_view"
    entity_id: str | None = Field(default=None, max_length=128)


class TrackClickEventIn(ClickEventIn):
    event_type: Literal["click"] = "click"
    entity_id: str | None = Field(default=None, max_length=128)


class TrackAddToCartEventIn(AddToCartEventIn):
    event_type: Literal["add_to_cart"] = "add_to_cart"
    entity_id: str | None = Field(default=None, max_length=128)


class TrackBeginCheckoutEventIn(BeginCheckoutEventIn):
    event_type: Literal["begin_checkout"] = "begin_checkout"
    entity_id: str | None = Field(default=None, max_length=128)


TrackEventIn = Annotated[
    Union[
        TrackPageViewEventIn,
        TrackClickEventIn,
        TrackAddToCartEventIn,
        TrackBeginCheckoutEventIn,
    ],
    Field(discriminator="event_type"),
]

def _cors_origins() -> list[str]:
    allow = (
        os.getenv("TRACKING_CORS_ALLOW_ORIGINS")
        or os.getenv("ANALYTICS_CORS_ALLOW_ORIGINS")
        or ""
    ).strip()
    if allow:
        parts = [p.strip() for p in allow.split(",")]
        return [p for p in parts if p]
    return ["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:8501"]


@asynccontextmanager
async def _lifespan(app: FastAPI):
    settings = load_settings()
    app.state.settings = settings
    app.state.engine = get_engine(settings)
    app.state.publisher = _RabbitPublisher(
        url=settings.rabbitmq_url,
        exchange=settings.rabbitmq_exchange,
        exchange_type=settings.rabbitmq_exchange_type,
        queue=settings.rabbitmq_queue,
        dlq=settings.rabbitmq_dlq,
        routing_keys=list(UI_EVENT_ROUTING_KEY.values()),
    )
    try:
        # Don't fail startup if RabbitMQ isn't ready yet.
        await app.state.publisher.ping()
    except Exception as exc:
        logger.warning("rabbitmq not ready at startup: {}", exc)
    yield
    try:
        await app.state.publisher.close()
    except Exception:
        pass

app = FastAPI(title="Kada Mandiya Tracking API", version="1.0.0", lifespan=_lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _derive_entity_id(event_type: UI_EVENT_TYPE, payload: dict[str, Any]) -> str | None:
    if payload.get("entity_id"):
        return str(payload["entity_id"])[:128]
    if event_type == "add_to_cart":
        pid = payload.get("product_id")
        if pid:
            return str(pid)[:128]
    if event_type == "begin_checkout":
        oid = payload.get("order_id")
        if oid:
            return str(oid)[:128]
    return None


async def _publish_ui_event(
    *,
    event_type: UI_EVENT_TYPE,
    request: Request,
    payload: dict[str, Any],
) -> dict[str, str]:
    event_id = str(uuid.uuid4())
    event_ts = datetime.now(timezone.utc)
    if event_type == "page_view" and payload.get("time_on_prev_page") is not None:
        payload.setdefault("time_on_prev_page_seconds", payload.get("time_on_prev_page"))
    correlation_id = (
        str(payload.get("correlation_id") or payload.get("session_id") or "").strip()[:64]
        or str(uuid.uuid4())
    )

    user_agent = _clamp_str(request.headers.get("user-agent"), max_len=500)
    ip_address = None
    if request.client is not None and request.client.host:
        ip_address = _clamp_str(request.client.host, max_len=64)

    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    meta = {
        "user_agent": user_agent,
        "ip_address": ip_address,
    }

    entity_id = _derive_entity_id(event_type, payload)

    envelope: dict[str, Any] = {
        "event_id": event_id,
        "event_timestamp": event_ts.isoformat(),
        "correlation_id": correlation_id,
        "service": "web",
        "event_type": event_type,
        "user_id": payload.get("user_id"),
        "entity_id": entity_id,
        # Include the event data at top-level for easy consumer extraction.
        **{k: v for k, v in payload.items() if k != "properties"},
        "properties": props,
        # Also include a nested payload for other consumers/tools.
        "payload": {**{k: v for k, v in payload.items() if k != "properties"}, "properties": props},
        "meta": meta,
    }

    rk = UI_EVENT_ROUTING_KEY[event_type]
    publisher: _RabbitPublisher = app.state.publisher
    await publisher.publish(
        routing_key=rk,
        body_obj=envelope,
        message_id=event_id,
        correlation_id=correlation_id,
        event_timestamp=event_ts,
    )

    logger.info(
        "published rk={} event_id={} correlation_id={}",
        rk,
        event_id,
        correlation_id,
    )
    return {"ok": "true", "event_id": event_id, "routing_key": rk}


@app.get("/health")
async def health() -> dict[str, Any]:
    status: dict[str, Any] = {"status": "ok"}

    # SQL Server check (analytics warehouse)
    try:
        engine = app.state.engine
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        status["sql"] = "ok"
    except Exception as exc:
        status["sql"] = "degraded"
        status["sql_error"] = str(exc)
        status["status"] = "degraded"

    # RabbitMQ check
    try:
        publisher: _RabbitPublisher = app.state.publisher
        await publisher.ping()
        status["rabbitmq"] = "ok"
    except Exception as exc:
        status["rabbitmq"] = "degraded"
        status["rabbitmq_error"] = str(exc)
        status["status"] = "degraded"

    return status


@app.post("/track")
async def track(event: TrackEventIn, request: Request) -> dict[str, str]:
    payload = event.model_dump(exclude_none=True)
    event_type = payload.pop("event_type")
    return await _publish_ui_event(event_type=event_type, request=request, payload=payload)


@app.post("/track/page_view")
async def track_page_view(event: PageViewEventIn, request: Request) -> dict[str, str]:
    payload = event.model_dump(exclude_none=True)
    return await _publish_ui_event(event_type="page_view", request=request, payload=payload)


@app.post("/track/click")
async def track_click(event: ClickEventIn, request: Request) -> dict[str, str]:
    payload = event.model_dump(exclude_none=True)
    return await _publish_ui_event(event_type="click", request=request, payload=payload)


@app.post("/track/add_to_cart")
async def track_add_to_cart(event: AddToCartEventIn, request: Request) -> dict[str, str]:
    payload = event.model_dump(exclude_none=True)
    return await _publish_ui_event(event_type="add_to_cart", request=request, payload=payload)


@app.post("/track/begin_checkout")
async def track_begin_checkout(event: BeginCheckoutEventIn, request: Request) -> dict[str, str]:
    payload = event.model_dump(exclude_none=True)
    return await _publish_ui_event(event_type="begin_checkout", request=request, payload=payload)
