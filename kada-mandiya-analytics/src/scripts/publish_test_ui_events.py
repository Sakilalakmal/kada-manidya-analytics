from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import aio_pika
from aio_pika import ExchangeType
from loguru import logger

from src.config import load_settings


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _exchange_type(value: str) -> ExchangeType:
    v = (value or "topic").strip().lower()
    if v == "direct":
        return ExchangeType.DIRECT
    if v == "fanout":
        return ExchangeType.FANOUT
    if v == "headers":
        return ExchangeType.HEADERS
    return ExchangeType.TOPIC


def _build_envelope(
    *,
    event_type: str,
    routing_key: str,
    session_id: str,
    user_id: str | None,
    page_url: str,
    entity_id: str | None,
    data: dict[str, Any],
) -> tuple[str, dict[str, Any], datetime]:
    event_id = str(uuid.uuid4())
    ts = _utc_now()

    base: dict[str, Any] = {
        "event_id": event_id,
        "event_timestamp": ts.isoformat(),
        "correlation_id": session_id,
        "service": "web",
        "event_type": event_type,
        "user_id": user_id,
        "entity_id": entity_id,
        "session_id": session_id,
        "page_url": page_url,
        **data,
        "payload": {"session_id": session_id, "page_url": page_url, **data},
        "meta": {"routing_key": routing_key, "source": "publish_test_ui_events"},
    }
    return event_id, base, ts


async def _publish(
    *,
    url: str,
    exchange_name: str,
    exchange_type: str,
    routing_key: str,
    envelope: dict[str, Any],
    message_id: str,
    correlation_id: str,
    timestamp: datetime,
) -> None:
    conn = await aio_pika.connect_robust(url)
    async with conn:
        ch = await conn.channel()
        exchange = await ch.declare_exchange(
            exchange_name, type=_exchange_type(exchange_type), durable=True
        )

        msg = aio_pika.Message(
            body=json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), default=str).encode(
                "utf-8"
            ),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
            correlation_id=correlation_id,
            timestamp=timestamp,
        )
        await exchange.publish(msg, routing_key=routing_key)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Publish test UI events to RabbitMQ exchange domain.events (ui.click + ui.add_to_cart)."
    )
    p.add_argument("--session-id", default="test-session", help="Session/correlation id.")
    p.add_argument("--user-id", default="", help="Optional user id.")
    p.add_argument("--page-url", default="/test", help="Page URL (path+query).")
    p.add_argument("--product-id", default="sku_test_001", help="Product id for add_to_cart.")
    p.add_argument("--quantity", type=int, default=1, help="Quantity for add_to_cart.")
    return p.parse_args()


async def main_async() -> int:
    args = _parse_args()
    settings = load_settings()

    session_id = str(args.session_id).strip()[:64] or "test-session"
    user_id = str(args.user_id).strip()[:64] or None
    page_url = str(args.page_url).strip()[:2000] or "/test"

    # ui.page_view
    pv_rk = "ui.page_view"
    pv_id, pv_env, pv_ts = _build_envelope(
        event_type="page_view",
        routing_key=pv_rk,
        session_id=session_id,
        user_id=user_id,
        page_url=page_url,
        entity_id=None,
        data={
            "referrer_url": None,
            "utm_source": None,
            "utm_medium": None,
            "utm_campaign": None,
            "time_on_prev_page_seconds": 1,
            "properties": {"kind": "test"},
        },
    )
    await _publish(
        url=settings.rabbitmq_url,
        exchange_name=settings.rabbitmq_exchange,
        exchange_type=settings.rabbitmq_exchange_type,
        routing_key=pv_rk,
        envelope=pv_env,
        message_id=pv_id,
        correlation_id=session_id,
        timestamp=pv_ts,
    )
    logger.info("published ui.page_view event_id={}", pv_id)

    # ui.click
    click_rk = "ui.click"
    click_id, click_env, click_ts = _build_envelope(
        event_type="click",
        routing_key=click_rk,
        session_id=session_id,
        user_id=user_id,
        page_url=page_url,
        entity_id=None,
        data={"element_id": "btn_add_to_cart", "properties": {"kind": "test"}},
    )
    await _publish(
        url=settings.rabbitmq_url,
        exchange_name=settings.rabbitmq_exchange,
        exchange_type=settings.rabbitmq_exchange_type,
        routing_key=click_rk,
        envelope=click_env,
        message_id=click_id,
        correlation_id=session_id,
        timestamp=click_ts,
    )
    logger.info("published ui.click event_id={}", click_id)

    # ui.add_to_cart
    cart_rk = "ui.add_to_cart"
    qty = max(1, int(args.quantity))
    cart_id, cart_env, cart_ts = _build_envelope(
        event_type="add_to_cart",
        routing_key=cart_rk,
        session_id=session_id,
        user_id=user_id,
        page_url=page_url,
        entity_id=str(args.product_id).strip()[:128] or None,
        data={
            "product_id": str(args.product_id).strip()[:128] or None,
            "quantity": qty,
            "element_id": "btn_add_to_cart",
            "properties": {"kind": "test"},
        },
    )
    await _publish(
        url=settings.rabbitmq_url,
        exchange_name=settings.rabbitmq_exchange,
        exchange_type=settings.rabbitmq_exchange_type,
        routing_key=cart_rk,
        envelope=cart_env,
        message_id=cart_id,
        correlation_id=session_id,
        timestamp=cart_ts,
    )
    logger.info("published ui.add_to_cart event_id={}", cart_id)

    return 0


def main() -> int:
    import asyncio

    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
