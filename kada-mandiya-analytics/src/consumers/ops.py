from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.engine import Connection

from src.consumers.idempotency import ensure_fingerprints_table, ensure_processed_events_table
from src.db.writers import insert_dead_letter


def ensure_ops_tables(conn: Connection) -> None:
    ensure_fingerprints_table(conn)
    ensure_processed_events_table(conn)


def write_dead_letter(conn: Connection, source: str, reason: str, payload: Any) -> None:
    insert_dead_letter(conn, source=source, reason=reason, payload=payload)


def claim_fingerprint_or_skip(
    conn: Connection, fingerprint: str, first_seen_at: datetime, source: str
) -> bool:
    from src.consumers.idempotency import try_claim_fingerprint

    return try_claim_fingerprint(
        conn, fingerprint=fingerprint, first_seen_at=first_seen_at, source=source
    )


def claim_event_id_or_skip(
    conn: Connection,
    *,
    event_id: str,
    first_seen_at: datetime,
    routing_key: str,
    message_id: str | None,
    source: str,
) -> bool:
    from src.consumers.idempotency import try_claim_event_id

    return try_claim_event_id(
        conn,
        event_id=event_id,
        first_seen_at=first_seen_at,
        routing_key=routing_key,
        message_id=message_id,
        source=source,
    )

