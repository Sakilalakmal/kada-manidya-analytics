from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_fingerprint(routing_key: str, message_id: str | None, payload_obj: Any) -> str:
    mid = message_id or ""
    blob = routing_key + "|" + mid + "|" + stable_json(payload_obj)
    return hashlib.sha256(blob.encode("utf-8", errors="replace")).hexdigest()


def ensure_fingerprints_table(conn: Connection) -> None:
    conn.execute(
        text("""
        IF OBJECT_ID('ops.event_fingerprints', 'U') IS NULL
        BEGIN
            CREATE TABLE ops.event_fingerprints(
                fingerprint varchar(64) NOT NULL
                    CONSTRAINT PK_ops_event_fingerprints PRIMARY KEY,
                first_seen_at datetime2 NOT NULL,
                source varchar(50) NOT NULL
            );
        END
        """)
    )


def ensure_processed_events_table(conn: Connection) -> None:
    conn.execute(
        text(
            """
        IF OBJECT_ID('ops.processed_events', 'U') IS NULL
        BEGIN
            CREATE TABLE ops.processed_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT PK_ops_processed_events PRIMARY KEY,
                first_seen_at datetime2 NOT NULL,
                routing_key varchar(200) NULL,
                message_id varchar(128) NULL,
                source varchar(50) NOT NULL
            );
        END
        """
        )
    )


def try_claim_event_id(
    conn: Connection,
    *,
    event_id: str,
    first_seen_at: datetime,
    routing_key: str,
    message_id: str | None,
    source: str,
) -> bool:
    params = {
        "event_id": event_id,
        "first_seen_at": first_seen_at,
        "routing_key": (routing_key or "")[:200] or None,
        "message_id": (message_id or "")[:128] or None,
        "source": (source or "rabbitmq")[:50],
    }

    try:
        conn.execute(
            text(
                """
                INSERT INTO ops.processed_events (event_id, first_seen_at, routing_key, message_id, source)
                VALUES (:event_id, :first_seen_at, :routing_key, :message_id, :source);
                """
            ),
            params,
        )
        return True
    except DBAPIError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "2601" in msg or "2627" in msg:
            return False
        raise


def try_claim_fingerprint(
    conn: Connection, fingerprint: str, first_seen_at: datetime, source: str
) -> bool:
    params = {
        "fingerprint": fingerprint,
        "first_seen_at": first_seen_at,
        "source": (source or "rabbitmq")[:50],
    }

    try:
        conn.execute(
            text(
                """
                INSERT INTO ops.event_fingerprints (fingerprint, first_seen_at, source)
                VALUES (:fingerprint, :first_seen_at, :source);
                """
            ),
            params,
        )
        return True
    except DBAPIError as exc:
        # SQL Server duplicates: 2601 (unique index) / 2627 (PK)
        msg = str(getattr(exc, "orig", exc))
        if "2601" in msg or "2627" in msg:
            return False
        raise
