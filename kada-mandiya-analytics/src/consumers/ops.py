from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.engine import Connection

from src.consumers.idempotency import ensure_fingerprints_table
from src.db.writers import insert_dead_letter


def ensure_ops_tables(conn: Connection) -> None:
    ensure_fingerprints_table(conn)


def write_dead_letter(conn: Connection, source: str, reason: str, payload: Any) -> None:
    insert_dead_letter(conn, source=source, reason=reason, payload=payload)


def claim_fingerprint_or_skip(
    conn: Connection, fingerprint: str, first_seen_at: datetime, source: str
) -> bool:
    from src.consumers.idempotency import try_claim_fingerprint

    return try_claim_fingerprint(
        conn, fingerprint=fingerprint, first_seen_at=first_seen_at, source=source
    )

