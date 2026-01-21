from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


CANONICAL_EVENT_MAP: dict[str, str] = {
    # orders
    "order.created": "order_created",
    "order_created": "order_created",
    "order.cancelled": "order_cancelled",
    "order_cancelled": "order_cancelled",
    "order.paid": "order_paid",
    "order_paid": "order_paid",
    # payments
    "payment.succeeded": "order_paid",
    "payment_success": "order_paid",
    "payment.failed": "payment_failed",
    "payment_failed": "payment_failed",
    # refunds
    "refund.created": "refund_created",
    "refund_created": "refund_created",
    "payment.refunded": "refund_created",
    # reviews
    "review.created": "review_created",
    "review_created": "review_created",
    "review_submitted": "review_created",
}


def canonical_event_type(raw: str | None) -> str:
    if not raw:
        return "unknown"
    key = str(raw).strip().lower()
    return CANONICAL_EVENT_MAP.get(key, key)


def parse_json_payload(payload: str) -> Any | None:
    try:
        return json.loads(payload)
    except Exception:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _find_first(obj: Any, keys: list[str]) -> Any | None:
    if not isinstance(obj, dict):
        return None
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return None


def _candidate_dicts(payload_obj: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(payload_obj, dict):
        out.append(payload_obj)
        for k in ["meta", "data", "payload", "event", "order", "payment", "review"]:
            v = payload_obj.get(k)
            if isinstance(v, dict):
                out.append(v)
    return out


def best_effort_order_id(entity_id: str | None, payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["order_id", "orderId", "id"])
        s = _as_str(v)
        if s:
            return s[:64]
    if entity_id:
        return entity_id[:64]
    return None


def best_effort_payment_id(payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["payment_id", "paymentId", "id"])
        s = _as_str(v)
        if s:
            return s[:64]
    return None


def best_effort_review_id(entity_id: str | None, payload_obj: Any) -> str | None:
    if entity_id:
        return entity_id[:64]
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["review_id", "reviewId", "id"])
        s = _as_str(v)
        if s:
            return s[:64]
    return None


def best_effort_product_id(payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["product_id", "productId", "sku"])
        s = _as_str(v)
        if s:
            return s[:64]
    return None


def best_effort_currency(payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["currency", "currency_code", "currencyCode"])
        s = _as_str(v)
        if s:
            return s[:10]
    return None


def best_effort_amount(payload_obj: Any) -> Decimal | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["total_amount", "totalAmount", "total", "amount", "revenue"])
        dec = _as_decimal(v)
        if dec is not None:
            return dec
    return None


def best_effort_provider(payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["provider", "gateway", "payment_provider", "paymentProvider"])
        s = _as_str(v)
        if s:
            return s[:50]
    return None


def best_effort_rating(payload_obj: Any) -> int | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["rating", "stars", "score"])
        i = _as_int(v)
        if i is not None:
            return i
    return None


def best_effort_comment(payload_obj: Any) -> str | None:
    for d in _candidate_dicts(payload_obj):
        v = _find_first(d, ["comment", "message", "text", "review"])
        s = _as_str(v)
        if s:
            return s[:1000]
    return None


def best_effort_items(payload_obj: Any) -> list[dict[str, Any]]:
    if not isinstance(payload_obj, dict):
        return []

    for k in ["items", "order_items", "orderItems", "products", "line_items", "lineItems"]:
        v = payload_obj.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    data = payload_obj.get("data")
    if isinstance(data, dict):
        for k in ["items", "order_items", "orderItems", "products", "line_items", "lineItems"]:
            v = data.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

    return []


@dataclass(frozen=True)
class ItemRow:
    product_id: str
    quantity: int
    unit_price: Decimal | None
    line_total: Decimal | None


def normalize_items(payload_obj: Any) -> list[ItemRow]:
    items = best_effort_items(payload_obj)
    out: list[ItemRow] = []
    for it in items:
        pid = _as_str(_find_first(it, ["product_id", "productId", "sku", "id"]))
        qty = _as_int(_find_first(it, ["quantity", "qty", "count"])) or 1
        if not pid:
            continue
        unit_price = _as_decimal(_find_first(it, ["unit_price", "unitPrice", "price"]))
        line_total = _as_decimal(_find_first(it, ["line_total", "lineTotal", "total"]))
        out.append(ItemRow(product_id=pid[:64], quantity=max(qty, 1), unit_price=unit_price, line_total=line_total))

    if out:
        return out

    if isinstance(payload_obj, dict):
        pid = _as_str(_find_first(payload_obj, ["product_id", "productId", "sku"]))
        qty = _as_int(_find_first(payload_obj, ["quantity", "qty"])) or 1
        if pid:
            unit_price = _as_decimal(_find_first(payload_obj, ["unit_price", "unitPrice", "price"]))
            line_total = _as_decimal(_find_first(payload_obj, ["line_total", "lineTotal", "total"]))
            return [
                ItemRow(
                    product_id=pid[:64],
                    quantity=max(qty, 1),
                    unit_price=unit_price,
                    line_total=line_total,
                )
            ]

    return []


def deterministic_id(seed: str, max_len: int = 64) -> str:
    h = hashlib.sha1(seed.encode("utf-8", errors="replace")).hexdigest()
    return h[:max_len]


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
