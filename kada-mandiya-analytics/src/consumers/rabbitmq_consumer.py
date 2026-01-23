from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from decimal import Decimal

import aio_pika
from aio_pika import ExchangeType, IncomingMessage
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy import text

from src.config import load_settings
from src.consumers.idempotency import compute_fingerprint
from src.consumers.mapper import decode_json_message, normalize_to_business_event
from src.consumers.ops import ensure_ops_tables, write_dead_letter, claim_fingerprint_or_skip
from src.db.engine import get_engine
from src.utils.time import to_sqlserver_utc_naive, utc_now


@dataclass(frozen=True)
class RabbitMQConsumerConfig:
    url: str
    exchange: str
    exchange_type: str
    queue: str
    routing_keys: list[str]
    prefetch: int
    dlq: str | None


def _parse_csv(value: str) -> list[str]:
    parts = [p.strip() for p in (value or "").split(",")]
    return [p for p in parts if p]


def _exchange_type(value: str) -> ExchangeType:
    v = (value or "topic").strip().lower()
    if v == "direct":
        return ExchangeType.DIRECT
    if v == "fanout":
        return ExchangeType.FANOUT
    if v == "headers":
        return ExchangeType.HEADERS
    return ExchangeType.TOPIC


class RabbitMQConsumer:
    def __init__(self, cfg: RabbitMQConsumerConfig) -> None:
        self._cfg = cfg
        self._engine = get_engine(load_settings())

    @staticmethod
    def _candidate_dicts(payload_obj) -> list[dict]:
        if not isinstance(payload_obj, dict):
            return []
        out: list[dict] = [payload_obj]
        for k in ["meta", "data", "payload", "event", "properties"]:
            v = payload_obj.get(k)
            if isinstance(v, dict):
                out.append(v)
        return out

    @classmethod
    def _first_nonempty(cls, payload_obj, keys: list[str]) -> str | None:
        for d in cls._candidate_dicts(payload_obj):
            for k in keys:
                v = d.get(k)
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
        return None

    @classmethod
    def _first_int(cls, payload_obj, keys: list[str]) -> int | None:
        s = cls._first_nonempty(payload_obj, keys)
        if s is None:
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    @classmethod
    def _first_decimal(cls, payload_obj, keys: list[str]) -> Decimal | None:
        s = cls._first_nonempty(payload_obj, keys)
        if s is None:
            return None
        try:
            return Decimal(str(s))
        except Exception:
            return None

    @classmethod
    def from_env(cls) -> "RabbitMQConsumer":
        s = load_settings()
        cfg = RabbitMQConsumerConfig(
            url=s.rabbitmq_url,
            exchange=s.rabbitmq_exchange,
            exchange_type=s.rabbitmq_exchange_type,
            queue=s.rabbitmq_queue,
            routing_keys=_parse_csv(s.rabbitmq_routing_keys),
            prefetch=s.rabbitmq_prefetch,
            dlq=s.rabbitmq_dlq,
        )
        if not cfg.routing_keys:
            cfg = RabbitMQConsumerConfig(
                url=cfg.url,
                exchange=cfg.exchange,
                exchange_type=cfg.exchange_type,
                queue=cfg.queue,
                routing_keys=[
                    "ui.page_view",
                    "ui.click",
                    "ui.add_to_cart",
                    "ui.begin_checkout",
                    "order.paid",
                    "payment.succeeded",
                    "review.*",
                ],
                prefetch=cfg.prefetch,
                dlq=cfg.dlq,
            )
        return cls(cfg)

    def ensure_ops(self) -> None:
        with self._engine.begin() as conn:
            ensure_ops_tables(conn)

    async def run(self, stop_event: asyncio.Event) -> None:
        self.ensure_ops()

        backoff = 1.0
        while not stop_event.is_set():
            try:
                await self._run_once(stop_event)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("rabbitmq consumer crashed: {}", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        logger.info(
            "connecting rabbitmq url='{}' exchange='{}' queue='{}' keys='{}'",
            self._cfg.url,
            self._cfg.exchange,
            self._cfg.queue,
            ",".join(self._cfg.routing_keys),
        )

        connection = await aio_pika.connect_robust(self._cfg.url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=int(self._cfg.prefetch or 50))

            exchange = await channel.declare_exchange(
                self._cfg.exchange,
                type=_exchange_type(self._cfg.exchange_type),
                durable=True,
            )

            queue_args: dict[str, object] = {}
            if self._cfg.dlq:
                dlx_name = "analytics.dlx"
                dlx = await channel.declare_exchange(dlx_name, type=ExchangeType.DIRECT, durable=True)
                dlq = await channel.declare_queue(self._cfg.dlq, durable=True)
                await dlq.bind(dlx, routing_key=self._cfg.dlq)
                queue_args["x-dead-letter-exchange"] = dlx_name
                queue_args["x-dead-letter-routing-key"] = self._cfg.dlq

            queue = await channel.declare_queue(
                self._cfg.queue, durable=True, arguments=queue_args or None
            )
            for rk in self._cfg.routing_keys:
                await queue.bind(exchange, routing_key=rk)

            logger.info("consumer ready (prefetch={})", int(self._cfg.prefetch or 50))

            async with queue.iterator() as it:
                async for msg in it:
                    if stop_event.is_set():
                        break
                    await self._handle_message(msg)

    async def _handle_message(self, msg: IncomingMessage) -> None:
        action: str = "ack"
        try:
            action = await self._process_message(msg)
        except Exception as exc:
            logger.error("unexpected processing failure: {}", exc)
        finally:
            try:
                if action == "reject" and self._cfg.dlq:
                    await msg.reject(requeue=False)
                else:
                    await msg.ack()
            except Exception:
                pass

    async def _process_message(self, msg: IncomingMessage) -> str:
        routing_key = (msg.routing_key or "").strip()
        message_id = msg.message_id or None
        correlation_id = msg.correlation_id or None

        try:
            raw_json, payload_obj = decode_json_message(msg.body)
        except Exception as exc:
            logger.warning("invalid json routing_key='{}': {}", routing_key, exc)
            await asyncio.to_thread(
                self._write_dead_letter_sync,
                reason="invalid_json",
                routing_key=routing_key,
                message_id=message_id,
                body=msg.body,
            )
            return "reject" if self._cfg.dlq else "ack"

        fp = compute_fingerprint(routing_key, message_id, payload_obj)

        try:
            event = normalize_to_business_event(
                routing_key=routing_key,
                message_id=message_id,
                message_correlation_id=correlation_id,
                message_timestamp=getattr(msg, "timestamp", None),
                payload_obj=payload_obj,
                raw_json=raw_json,
                session_fallback=fp[:64],
            )
        except Exception as exc:
            logger.warning("normalize failed rk='{}': {}", routing_key, exc)
            await asyncio.to_thread(
                self._write_dead_letter_sync,
                reason="normalize_failed",
                routing_key=routing_key,
                message_id=message_id,
                body=msg.body,
            )
            return "reject" if self._cfg.dlq else "ack"

        try:
            inserted, rowcount = await asyncio.to_thread(
                self._insert_event_sync,
                fp,
                routing_key,
                message_id,
                correlation_id,
                raw_json,
                payload_obj,
                event,
            )
        except Exception as exc:
            logger.error("db insert failed rk='{}': {}", routing_key, exc)
            await asyncio.to_thread(
                self._write_dead_letter_sync,
                reason="db_insert_failed",
                routing_key=routing_key,
                message_id=message_id,
                body=msg.body,
            )
            return "reject" if self._cfg.dlq else "ack"

        if inserted:
            logger.info(
                "ingested rk='{}' event_type='{}' rowcount={}",
                routing_key,
                event.event_type,
                rowcount,
            )
        else:
            logger.debug("duplicate rk='{}' fp='{}' (skipped)", routing_key, fp[:12])
        return "ack"

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, min=0.5, max=10), reraise=True)
    def _insert_event_sync(
        self,
        fingerprint: str,
        routing_key: str,
        message_id: str | None,
        correlation_id: str | None,
        raw_json: str,
        payload_obj,
        event,
    ) -> tuple[bool, int]:
        first_seen_at = to_sqlserver_utc_naive(utc_now())
        with self._engine.begin() as conn:
            claimed = claim_fingerprint_or_skip(
                conn,
                fingerprint=fingerprint,
                first_seen_at=first_seen_at,
                source="rabbitmq",
            )
            if not claimed:
                return False, 0

            rk = (routing_key or "").strip().lower()
            ts = to_sqlserver_utc_naive(event.event_timestamp)

            meta = {
                "routing_key": routing_key,
                "message_id": message_id,
                "correlation_id": correlation_id,
            }
            meta_json = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))

            if rk == "ui.page_view":
                session_id = self._first_nonempty(payload_obj, ["session_id", "sessionId"]) or None
                page_url = self._first_nonempty(payload_obj, ["page_url", "pageUrl", "url", "path"]) or "/"
                props = raw_json
                result = conn.execute(
                    text(
                        """
                        INSERT INTO bronze.page_view_events
                            (event_id, event_timestamp, session_id, user_id, page_url, referrer_url, utm_source, utm_medium, utm_campaign, time_on_prev_page_seconds, properties, payload, meta)
                        VALUES
                            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :referrer_url, :utm_source, :utm_medium, :utm_campaign, :time_on_prev_page_seconds, :properties, :payload, :meta);
                        """
                    ),
                    {
                        "event_id": str(event.event_id),
                        "event_timestamp": ts,
                        "session_id": session_id,
                        "user_id": event.user_id,
                        "page_url": page_url,
                        "referrer_url": self._first_nonempty(payload_obj, ["referrer_url", "referrerUrl", "referrer"]) or None,
                        "utm_source": self._first_nonempty(payload_obj, ["utm_source", "utmSource"]) or None,
                        "utm_medium": self._first_nonempty(payload_obj, ["utm_medium", "utmMedium"]) or None,
                        "utm_campaign": self._first_nonempty(payload_obj, ["utm_campaign", "utmCampaign"]) or None,
                        "time_on_prev_page_seconds": self._first_int(payload_obj, ["time_on_prev_page_seconds", "timeOnPrevPageSeconds"]) or None,
                        "properties": props,
                        "payload": raw_json,
                        "meta": meta_json,
                    },
                )
                return True, int(getattr(result, "rowcount", 0) or 0)

            if rk == "ui.click":
                session_id = self._first_nonempty(payload_obj, ["session_id", "sessionId"]) or None
                page_url = self._first_nonempty(payload_obj, ["page_url", "pageUrl", "url", "path"]) or "/"
                element_id = self._first_nonempty(payload_obj, ["element_id", "elementId", "target_id", "targetId"]) or None
                result = conn.execute(
                    text(
                        """
                        INSERT INTO bronze.click_events
                            (event_id, event_timestamp, session_id, user_id, page_url, element_id, x, y, viewport_w, viewport_h, user_agent, ip_address, properties, payload, meta)
                        VALUES
                            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :element_id, :x, :y, :viewport_w, :viewport_h, :user_agent, :ip_address, :properties, :payload, :meta);
                        """
                    ),
                    {
                        "event_id": str(event.event_id),
                        "event_timestamp": ts,
                        "session_id": session_id,
                        "user_id": event.user_id,
                        "page_url": page_url,
                        "element_id": element_id,
                        "x": self._first_int(payload_obj, ["x"]) or None,
                        "y": self._first_int(payload_obj, ["y"]) or None,
                        "viewport_w": self._first_int(payload_obj, ["viewport_w", "viewportW"]) or None,
                        "viewport_h": self._first_int(payload_obj, ["viewport_h", "viewportH"]) or None,
                        "user_agent": self._first_nonempty(payload_obj, ["user_agent", "userAgent"]) or None,
                        "ip_address": self._first_nonempty(payload_obj, ["ip_address", "ipAddress", "ip"]) or None,
                        "properties": raw_json,
                        "payload": raw_json,
                        "meta": meta_json,
                    },
                )
                return True, int(getattr(result, "rowcount", 0) or 0)

            if rk == "ui.add_to_cart":
                session_id = self._first_nonempty(payload_obj, ["session_id", "sessionId"]) or None
                page_url = self._first_nonempty(payload_obj, ["page_url", "pageUrl", "url", "path"]) or None
                product_id = self._first_nonempty(payload_obj, ["product_id", "productId", "sku"]) or None
                qty = self._first_int(payload_obj, ["quantity", "qty", "count"])
                result = conn.execute(
                    text(
                        """
                        INSERT INTO bronze.cart_events
                            (event_id, event_timestamp, session_id, user_id, page_url, product_id, quantity, payload, meta)
                        VALUES
                            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :product_id, :quantity, :payload, :meta);
                        """
                    ),
                    {
                        "event_id": str(event.event_id),
                        "event_timestamp": ts,
                        "session_id": session_id,
                        "user_id": event.user_id,
                        "page_url": page_url,
                        "product_id": product_id,
                        "quantity": qty,
                        "payload": raw_json,
                        "meta": meta_json,
                    },
                )
                return True, int(getattr(result, "rowcount", 0) or 0)

            if rk == "ui.begin_checkout":
                session_id = self._first_nonempty(payload_obj, ["session_id", "sessionId"]) or None
                page_url = self._first_nonempty(payload_obj, ["page_url", "pageUrl", "url", "path"]) or None
                order_id = self._first_nonempty(payload_obj, ["order_id", "orderId"]) or None
                result = conn.execute(
                    text(
                        """
                        INSERT INTO bronze.checkout_events
                            (event_id, event_timestamp, session_id, user_id, page_url, order_id, payload, meta)
                        VALUES
                            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :order_id, :payload, :meta);
                        """
                    ),
                    {
                        "event_id": str(event.event_id),
                        "event_timestamp": ts,
                        "session_id": session_id,
                        "user_id": event.user_id,
                        "page_url": page_url,
                        "order_id": order_id,
                        "payload": raw_json,
                        "meta": meta_json,
                    },
                )
                return True, int(getattr(result, "rowcount", 0) or 0)

            inserted_rows = 0

            # Keep raw business events for downstream orders/payments/reviews ETL.
            result = conn.execute(
                text(
                    """
                    INSERT INTO bronze.business_events
                        (event_id, event_timestamp, correlation_id, service, event_type, user_id, entity_id, payload)
                    VALUES
                        (:event_id, :event_timestamp, :correlation_id, :service, :event_type, :user_id, :entity_id, :payload);
                    """
                ),
                {
                    "event_id": str(event.event_id),
                    "event_timestamp": ts,
                    "correlation_id": event.correlation_id,
                    "service": event.service,
                    "event_type": event.event_type,
                    "user_id": event.user_id,
                    "entity_id": event.entity_id,
                    "payload": event.payload,
                },
            )
            inserted_rows += int(getattr(result, "rowcount", 0) or 0)

            if rk in {"order.paid", "payment.succeeded"}:
                result2 = conn.execute(
                    text(
                        """
                        INSERT INTO bronze.order_events
                            (event_id, event_timestamp, session_id, user_id, order_id, payment_id, event_type, total_amount, currency, payload, meta)
                        VALUES
                            (:event_id, :event_timestamp, :session_id, :user_id, :order_id, :payment_id, :event_type, :total_amount, :currency, :payload, :meta);
                        """
                    ),
                    {
                        "event_id": str(event.event_id),
                        "event_timestamp": ts,
                        "session_id": None,
                        "user_id": event.user_id,
                        "order_id": self._first_nonempty(payload_obj, ["order_id", "orderId"]) or event.entity_id,
                        "payment_id": self._first_nonempty(payload_obj, ["payment_id", "paymentId"]) or None,
                        "event_type": rk,
                        "total_amount": self._first_decimal(payload_obj, ["total_amount", "totalAmount", "amount"]),
                        "currency": self._first_nonempty(payload_obj, ["currency", "currency_code", "currencyCode"]) or None,
                        "payload": raw_json,
                        "meta": meta_json,
                    },
                )
                inserted_rows += int(getattr(result2, "rowcount", 0) or 0)

            return True, inserted_rows

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=5), reraise=True)
    def _write_dead_letter_sync(
        self,
        *,
        reason: str,
        routing_key: str,
        message_id: str | None,
        body: bytes,
    ) -> None:
        with self._engine.begin() as conn:
            write_dead_letter(
                conn,
                source="rabbitmq",
                reason=reason,
                payload={
                    "routing_key": routing_key,
                    "message_id": message_id,
                    "body": body.decode("utf-8", errors="replace"),
                },
            )
