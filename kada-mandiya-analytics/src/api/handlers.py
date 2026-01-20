from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from loguru import logger
from pydantic import ValidationError
from sqlalchemy.engine import Connection

from src.db.writers import (
    insert_api_request_log,
    insert_business,
    insert_click,
    insert_db_query_perf,
    insert_dead_letter,
    insert_form,
    insert_page_view,
    insert_scroll,
    insert_search,
)
from src.models.events import (
    ApiRequestLogEvent,
    BaseEvent,
    BusinessEvent,
    CartActionEvent,
    CheckoutEvent,
    ClickEvent,
    DbQueryPerfEvent,
    FormInteractionEvent,
    FrontendErrorEvent,
    PageViewEvent,
    PerformanceEvent,
    PurchaseViewEvent,
    ScrollEvent,
    SearchEvent,
)

EVENT_TYPE_TO_MODEL: dict[str, type[BaseEvent]] = {
    "page_view": PageViewEvent,
    "click": ClickEvent,
    "scroll": ScrollEvent,
    "form_interaction": FormInteractionEvent,
    "search": SearchEvent,
    "performance": PerformanceEvent,
    "cart_action": CartActionEvent,
    "checkout": CheckoutEvent,
    "purchase_view": PurchaseViewEvent,
    "frontend_error": FrontendErrorEvent,
    "api_request_log": ApiRequestLogEvent,
    "db_query_perf": DbQueryPerfEvent,
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def normalize_events(body: Any) -> list[Any]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and isinstance(body.get("events"), list):
        return body["events"]
    return [body]


def _as_business_event(event: BaseEvent, raw: dict[str, Any]) -> BusinessEvent:
    if isinstance(event, BusinessEvent):
        return event
    data = event.model_dump(mode="json")
    data.setdefault("payload", {"event": data, "raw": raw})
    return BusinessEvent.model_validate(data)


def ingest_events(
    conn: Connection, events: list[Any], received_at: datetime
) -> dict[str, int]:
    accepted = 0
    dead_lettered = 0

    for idx, raw in enumerate(events):
        if not isinstance(raw, dict):
            dead_lettered += 1
            try:
                insert_dead_letter(
                    conn,
                    source="collector",
                    reason="invalid_event_shape",
                    payload={"index": idx, "event": raw},
                )
            except Exception as exc:
                logger.error("dead-letter insert failed: {}", exc)
            continue

        event_type = raw.get("event_type")
        if not isinstance(event_type, str) or not event_type.strip():
            dead_lettered += 1
            try:
                insert_dead_letter(
                    conn,
                    source=str(raw.get("source") or "unknown"),
                    reason="missing_event_type",
                    payload=raw,
                )
            except Exception as exc:
                logger.error("dead-letter insert failed: {}", exc)
            continue

        try:
            model = EVENT_TYPE_TO_MODEL.get(event_type)
            if model is not None:
                event = model.model_validate(raw)
            else:
                event = BusinessEvent.model_validate(raw)
        except ValidationError as exc:
            dead_lettered += 1
            logger.warning("validation failed for event_type='{}': {}", event_type, exc)
            try:
                insert_dead_letter(
                    conn,
                    source=str(raw.get("source") or "unknown"),
                    reason=f"validation_error:{event_type}",
                    payload={"event": raw, "error": exc.errors()},
                )
            except Exception as dlq_exc:
                logger.error("dead-letter insert failed: {}", dlq_exc)
            continue

        try:
            if isinstance(event, ClickEvent):
                insert_click(conn, event, received_at, raw)
            elif isinstance(event, PageViewEvent):
                insert_page_view(conn, event, received_at, raw)
            elif isinstance(event, ScrollEvent):
                insert_scroll(conn, event, received_at, raw)
            elif isinstance(event, FormInteractionEvent):
                insert_form(conn, event, received_at, raw)
            elif isinstance(event, SearchEvent):
                insert_search(conn, event, received_at, raw)
            elif isinstance(event, ApiRequestLogEvent):
                insert_api_request_log(conn, event, received_at, raw)
            elif isinstance(event, DbQueryPerfEvent):
                insert_db_query_perf(conn, event, received_at, raw)
            else:
                be = _as_business_event(event, raw)
                insert_business(conn, be, received_at, raw)

            accepted += 1
            logger.info(
                "ingested event_type='{}' event_id='{}'",
                event.event_type,
                event.event_id,
            )

        except Exception as exc:
            dead_lettered += 1
            logger.error("insert failed for event_type='{}': {}", event.event_type, exc)
            try:
                insert_dead_letter(
                    conn,
                    source=str(
                        getattr(event, "source", None) or raw.get("source") or "unknown"
                    ),
                    reason=f"db_insert_error:{event.event_type}",
                    payload={"event": raw, "error": str(exc)},
                )
            except Exception as dlq_exc:
                logger.error("dead-letter insert failed: {}", dlq_exc)

    return {"accepted": accepted, "dead_lettered": dead_lettered}
