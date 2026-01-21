from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.models.events import BusinessEvent
from src.utils.time import utc_now


def _iso_to_utc(value: str) -> datetime | None:
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _epoch_to_utc(value: float) -> datetime | None:
    try:
        v = float(value)
    except Exception:
        return None
    if v > 1e12:  # ms
        v = v / 1000.0
    try:
        return datetime.fromtimestamp(v, tz=UTC)
    except Exception:
        return None


def _candidate_dicts(payload: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        out.append(payload)
        for k in ["meta", "data", "payload", "event"]:
            v = payload.get(k)
            if isinstance(v, dict):
                out.append(v)
    return out


def _first_nonempty_str(dcts: list[dict[str, Any]], keys: list[str]) -> str | None:
    for d in dcts:
        for k in keys:
            v = d.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
    return None


def _best_effort_user_id(payload: Any) -> str | None:
    dcts = _candidate_dicts(payload)
    uid = _first_nonempty_str(dcts, ["user_id", "userId", "uid", "customer_id", "customerId"])
    if uid and "@" in uid:
        return None
    if uid and len(uid) > 64:
        return None
    return uid


def _best_effort_entity_id(payload: Any) -> str | None:
    dcts = _candidate_dicts(payload)
    return _first_nonempty_str(
        dcts,
        [
            "entity_id",
            "entityId",
            "order_id",
            "orderId",
            "payment_id",
            "paymentId",
            "review_id",
            "reviewId",
            "product_id",
            "productId",
            "cart_id",
            "cartId",
            "id",
        ],
    )


def _best_effort_service(payload: Any) -> str:
    dcts = _candidate_dicts(payload)
    svc = _first_nonempty_str(dcts, ["service", "service_name", "serviceName", "producer", "source"])
    return (svc or "unknown")[:64]


def _best_effort_event_type(payload: Any, routing_key: str) -> str:
    dcts = _candidate_dicts(payload)
    et = _first_nonempty_str(
        dcts, ["event_type", "eventType", "type", "name", "event", "action"]
    )
    return (et or routing_key or "unknown")[:100]


def _best_effort_correlation_id(payload: Any, message_correlation_id: str | None) -> str | None:
    if message_correlation_id:
        s = str(message_correlation_id).strip()
        if s:
            return s[:64]

    dcts = _candidate_dicts(payload)
    cid = _first_nonempty_str(
        dcts,
        [
            "correlation_id",
            "correlationId",
            "correlationID",
            "request_id",
            "requestId",
            "trace_id",
            "traceId",
        ],
    )
    return cid[:64] if cid else None


def _best_effort_event_timestamp(payload: Any, message_timestamp: Any | None) -> datetime:
    dcts = _candidate_dicts(payload)
    for d in dcts:
        for k in [
            "event_timestamp",
            "eventTimestamp",
            "timestamp",
            "ts",
            "occurred_at",
            "occurredAt",
            "created_at",
            "createdAt",
            "time",
        ]:
            v = d.get(k)
            if v is None:
                continue
            if isinstance(v, str):
                dt = _iso_to_utc(v)
                if dt is not None:
                    return dt
            if isinstance(v, (int, float)):
                dt = _epoch_to_utc(v)
                if dt is not None:
                    return dt

    if isinstance(message_timestamp, datetime):
        if message_timestamp.tzinfo is None or message_timestamp.tzinfo.utcoffset(message_timestamp) is None:
            return message_timestamp.replace(tzinfo=UTC)
        return message_timestamp.astimezone(UTC)
    if isinstance(message_timestamp, (int, float)):
        dt = _epoch_to_utc(message_timestamp)
        if dt is not None:
            return dt

    return utc_now()


def decode_json_message(body: bytes) -> tuple[str, Any]:
    raw = body.decode("utf-8", errors="replace")
    obj = json.loads(raw)
    return raw, obj


def normalize_to_business_event(
    *,
    routing_key: str,
    message_id: str | None,
    message_correlation_id: str | None,
    message_timestamp: Any | None,
    payload_obj: Any,
    raw_json: str,
    session_fallback: str,
) -> BusinessEvent:
    event_type = _best_effort_event_type(payload_obj, routing_key)
    service = _best_effort_service(payload_obj)
    correlation_id = _best_effort_correlation_id(payload_obj, message_correlation_id)
    user_id = _best_effort_user_id(payload_obj)
    entity_id = _best_effort_entity_id(payload_obj)
    event_timestamp = _best_effort_event_timestamp(payload_obj, message_timestamp)

    session_id = correlation_id or session_fallback

    return BusinessEvent.model_validate(
        {
            "event_type": event_type,
            "event_timestamp": event_timestamp,
            "session_id": session_id,
            "source": "service",
            "service": service,
            "correlation_id": correlation_id,
            "user_id": user_id,
            "entity_id": entity_id,
            "payload": raw_json,
            "_meta": {
                "routing_key": routing_key,
                "message_id": message_id,
            },
        }
    )

