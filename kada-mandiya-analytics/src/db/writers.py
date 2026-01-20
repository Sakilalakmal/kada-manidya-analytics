from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.models.events import (
    ApiRequestLogEvent,
    BaseEvent,
    BusinessEvent,
    ClickEvent,
    FormInteractionEvent,
    PageViewEvent,
    ScrollEvent,
    SearchEvent,
    DbQueryPerfEvent,
)
from src.utils.time import to_sqlserver_utc_naive


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _merge_properties(
    event: BaseEvent, received_at: datetime, raw: dict[str, Any]
) -> str | None:
    merged: dict[str, Any] = {}
    if isinstance(event.properties, dict):
        merged.update(event.properties)
    if event.model_extra:
        merged.update(event.model_extra)

    merged["_meta"] = {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "source": event.source,
        "received_at": received_at.isoformat(),
    }

    if "_raw" not in merged:
        merged["_raw"] = raw

    return _json_dumps(merged) if merged else None


def insert_dead_letter(
    conn: Connection, source: str, reason: str, payload: Any
) -> None:
    src = (source or "unknown")[:50]
    rsn = (reason or "unknown")[:500]
    body = payload if isinstance(payload, str) else _json_dumps(payload)
    conn.execute(
        text("""
            INSERT INTO ops.dead_letter_events (source, reason, payload)
            VALUES (:source, :reason, :payload);
            """),
        {"source": src, "reason": rsn, "payload": body},
    )


def _insert_ignore_duplicates(
    conn: Connection, insert_sql: str, params: dict[str, Any]
) -> None:
    conn.execute(
        text(f"""
            BEGIN TRY
                {insert_sql}
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() IN (2601, 2627) RETURN;
                THROW;
            END CATCH
            """),
        params,
    )


def insert_click(
    conn: Connection, event: ClickEvent, received_at: datetime, raw: dict[str, Any]
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.click_events
            (event_id, event_timestamp, session_id, user_id, page_url, element_id,
             x, y, viewport_w, viewport_h, user_agent, ip_address, properties)
        VALUES
            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :element_id,
             :x, :y, :viewport_w, :viewport_h, :user_agent, :ip_address, :properties);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "session_id": event.session_id,
            "user_id": event.user_id,
            "page_url": event.page_url,
            "element_id": event.element_id,
            "x": event.x,
            "y": event.y,
            "viewport_w": event.viewport_w,
            "viewport_h": event.viewport_h,
            "user_agent": event.user_agent,
            "ip_address": event.ip_address,
            "properties": _merge_properties(event, received_at, raw),
        },
    )


def insert_page_view(
    conn: Connection, event: PageViewEvent, received_at: datetime, raw: dict[str, Any]
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.page_view_events
            (event_id, event_timestamp, session_id, user_id, page_url, referrer_url,
             utm_source, utm_medium, utm_campaign, time_on_prev_page_seconds, properties)
        VALUES
            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :referrer_url,
             :utm_source, :utm_medium, :utm_campaign, :time_on_prev_page_seconds, :properties);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "session_id": event.session_id,
            "user_id": event.user_id,
            "page_url": event.page_url,
            "referrer_url": event.referrer_url,
            "utm_source": event.utm_source,
            "utm_medium": event.utm_medium,
            "utm_campaign": event.utm_campaign,
            "time_on_prev_page_seconds": event.time_on_prev_page_seconds,
            "properties": _merge_properties(event, received_at, raw),
        },
    )


def insert_scroll(
    conn: Connection, event: ScrollEvent, received_at: datetime, raw: dict[str, Any]
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.scroll_events
            (event_id, event_timestamp, session_id, user_id, page_url, scroll_depth_pct, properties)
        VALUES
            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :scroll_depth_pct, :properties);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "session_id": event.session_id,
            "user_id": event.user_id,
            "page_url": event.page_url,
            "scroll_depth_pct": event.scroll_depth_pct,
            "properties": _merge_properties(event, received_at, raw),
        },
    )


def insert_form(
    conn: Connection,
    event: FormInteractionEvent,
    received_at: datetime,
    raw: dict[str, Any],
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.form_events
            (event_id, event_timestamp, session_id, user_id, page_url, form_id, field_id, action,
             error_message, time_spent_ms, properties)
        VALUES
            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :form_id, :field_id, :action,
             :error_message, :time_spent_ms, :properties);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "session_id": event.session_id,
            "user_id": event.user_id,
            "page_url": event.page_url,
            "form_id": event.form_id,
            "field_id": event.field_id,
            "action": event.action,
            "error_message": event.error_message,
            "time_spent_ms": event.time_spent_ms,
            "properties": _merge_properties(event, received_at, raw),
        },
    )


def insert_search(
    conn: Connection, event: SearchEvent, received_at: datetime, raw: dict[str, Any]
) -> None:
    filters = event.filters
    filters_str = None
    if filters is not None:
        filters_str = filters if isinstance(filters, str) else _json_dumps(filters)
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.search_events
            (event_id, event_timestamp, session_id, user_id, page_url, query, results_count, filters, properties)
        VALUES
            (:event_id, :event_timestamp, :session_id, :user_id, :page_url, :query, :results_count, :filters, :properties);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "session_id": event.session_id,
            "user_id": event.user_id,
            "page_url": event.page_url,
            "query": event.query,
            "results_count": event.results_count,
            "filters": filters_str,
            "properties": _merge_properties(event, received_at, raw),
        },
    )


def insert_business(
    conn: Connection, event: BusinessEvent, received_at: datetime, raw: dict[str, Any]
) -> None:
    payload = event.payload
    payload_obj: Any = payload if payload is not None else raw
    if not isinstance(payload_obj, str):
        payload_obj = {
            "event": event.model_dump(mode="json"),
            "received_at": received_at.isoformat(),
            "raw": raw,
        }
        payload_str = _json_dumps(payload_obj)
    else:
        payload_str = payload_obj

    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.business_events
            (event_id, event_timestamp, correlation_id, service, event_type, user_id, entity_id, payload)
        VALUES
            (:event_id, :event_timestamp, :correlation_id, :service, :event_type, :user_id, :entity_id, :payload);
        """,
        {
            "event_id": str(event.event_id),
            "event_timestamp": to_sqlserver_utc_naive(event.event_timestamp),
            "correlation_id": event.correlation_id,
            "service": event.service,
            "event_type": event.event_type,
            "user_id": event.user_id,
            "entity_id": event.entity_id,
            "payload": payload_str,
        },
    )


def insert_api_request_log(
    conn: Connection,
    event: ApiRequestLogEvent,
    received_at: datetime,
    raw: dict[str, Any],
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.api_request_logs
            (event_id, [timestamp], service, endpoint, method, status_code, response_time_ms, user_id,
             correlation_id, request_size_bytes, response_size_bytes, ip_address, user_agent)
        VALUES
            (:event_id, :ts, :service, :endpoint, :method, :status_code, :response_time_ms, :user_id,
             :correlation_id, :request_size_bytes, :response_size_bytes, :ip_address, :user_agent);
        """,
        {
            "event_id": str(event.event_id),
            "ts": to_sqlserver_utc_naive(event.event_timestamp),
            "service": event.service,
            "endpoint": event.endpoint,
            "method": event.method,
            "status_code": event.status_code,
            "response_time_ms": event.response_time_ms,
            "user_id": event.user_id,
            "correlation_id": event.correlation_id,
            "request_size_bytes": event.request_size_bytes,
            "response_size_bytes": event.response_size_bytes,
            "ip_address": event.ip_address,
            "user_agent": event.user_agent,
        },
    )


def insert_db_query_perf(
    conn: Connection,
    event: DbQueryPerfEvent,
    received_at: datetime,
    raw: dict[str, Any],
) -> None:
    _insert_ignore_duplicates(
        conn,
        """
        INSERT INTO bronze.db_query_perf
            (event_id, [timestamp], service, database_name, query_type, table_name, execution_time_ms,
             rows_affected, query_hash)
        VALUES
            (:event_id, :ts, :service, :database_name, :query_type, :table_name, :execution_time_ms,
             :rows_affected, :query_hash);
        """,
        {
            "event_id": str(event.event_id),
            "ts": to_sqlserver_utc_naive(event.event_timestamp),
            "service": event.service,
            "database_name": event.database_name,
            "query_type": event.query_type,
            "table_name": event.table_name,
            "execution_time_ms": event.execution_time_ms,
            "rows_affected": event.rows_affected,
            "query_hash": event.query_hash,
        },
    )
