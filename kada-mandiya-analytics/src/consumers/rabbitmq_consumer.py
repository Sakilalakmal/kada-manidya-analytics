from __future__ import annotations

import asyncio
from dataclasses import dataclass

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
                routing_keys=["order.*", "payment.*", "review.*"],
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
            inserted, rowcount = await asyncio.to_thread(self._insert_event_sync, fp, event)
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
    def _insert_event_sync(self, fingerprint: str, event) -> tuple[bool, int]:
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
                    "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
                    "correlation_id": event.correlation_id,
                    "service": event.service,
                    "event_type": event.event_type,
                    "user_id": event.user_id,
                    "entity_id": event.entity_id,
                    "payload": event.payload,
                },
            )
            return True, int(getattr(result, "rowcount", 0) or 0)

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
