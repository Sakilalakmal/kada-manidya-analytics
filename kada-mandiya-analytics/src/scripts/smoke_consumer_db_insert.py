from __future__ import annotations

import json
from uuid import uuid4

from sqlalchemy import text

from src.config import load_settings
from src.consumers.idempotency import (
    compute_fingerprint,
    ensure_fingerprints_table,
    try_claim_fingerprint,
)
from src.db.engine import get_engine
from src.utils.time import to_sqlserver_utc_naive, utc_now


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    routing_key = "order.smoke_test"
    payload_obj = {
        "type": "order.smoke_test",
        "data": {"order_id": "smoke-1", "user_id": "u_smoke"},
        "meta": {"service": "smoke"},
    }
    raw_json = json.dumps(payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    fingerprint = compute_fingerprint(routing_key, "smoke-message-id", payload_obj)

    with engine.begin() as conn:
        ensure_fingerprints_table(conn)

        claimed = try_claim_fingerprint(
            conn,
            fingerprint=fingerprint,
            first_seen_at=to_sqlserver_utc_naive(utc_now()),
            source="smoke",
        )
        if not claimed:
            print("OK (duplicate fingerprint; skipped insert)")
            return 0

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
                "event_id": str(uuid4()),
                "event_timestamp": to_sqlserver_utc_naive(utc_now()),
                "correlation_id": "smoke-corr-1",
                "service": "smoke",
                "event_type": routing_key,
                "user_id": "u_smoke",
                "entity_id": "smoke-1",
                "payload": raw_json,
            },
        )

    print(f"OK (rowcount={getattr(result, 'rowcount', None)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
