from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from uuid import uuid4

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run


@dataclass(frozen=True)
class SeedConfig:
    users: int
    orders: int


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _seed_run_id(now: datetime) -> str:
    return now.strftime("%Y-%m-%d-seed-business")


def _already_seeded(conn, seed_run_id: str) -> bool:
    pattern = f'%\"seed_run_id\":\"{seed_run_id}\"%'
    row = conn.execute(
        text("""
            SELECT TOP 1 1
            FROM bronze.business_events
            WHERE event_timestamp >= DATEADD(day, -7, SYSDATETIME())
              AND payload LIKE :pattern;
            """),
        {"pattern": pattern},
    ).first()
    return row is not None


def _generate_orders(now: datetime, cfg: SeedConfig, seed_run_id: str) -> list[dict]:
    rng = random.Random(seed_run_id)
    users = [f"U{idx:03d}" for idx in range(1, cfg.users + 1)]
    products = [f"P{idx:03d}" for idx in range(1, 21)]

    rows: list[dict] = []
    for i in range(cfg.orders):
        order_id = f"ORD-{now.strftime('%Y%m%d')}-{i+1:04d}"
        user_id = rng.choice(users)
        correlation_id = uuid4().hex

        created_at = now - timedelta(
            days=rng.randint(0, 6),
            minutes=rng.randint(0, 24 * 60 - 1),
            seconds=rng.randint(0, 59),
        )

        n_items = rng.randint(1, 4)
        item_product_ids = rng.sample(products, k=min(n_items, len(products)))

        items: list[dict] = []
        total_amount = Decimal("0.00")
        for pid in item_product_ids:
            qty = rng.randint(1, 4)
            unit_price = _money(Decimal(str(rng.uniform(120.0, 3200.0))))
            line_total = _money(unit_price * Decimal(qty))
            total_amount += line_total
            items.append(
                {
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price": str(unit_price),
                    "line_total": str(line_total),
                }
            )

        payload_base = {
            "meta": {"seed_run_id": seed_run_id},
            "order_id": order_id,
            "user_id": user_id,
            "currency": "LKR",
            "total_amount": str(_money(total_amount)),
            "items": items,
        }

        rows.append(
            {
                "event_timestamp": created_at,
                "correlation_id": correlation_id,
                "service": "order-service",
                "event_type": "order.created",
                "user_id": user_id,
                "entity_id": order_id,
                "payload": json.dumps(payload_base, separators=(",", ":")),
            }
        )

        followup_at = created_at + timedelta(
            minutes=rng.randint(1, 30), seconds=rng.randint(0, 59)
        )
        p = rng.random()
        if p < 0.80:
            event_type = "order_paid"
        elif p < 0.95:
            event_type = "order.cancelled"
        else:
            event_type = "payment.failed"

        payload_followup = dict(payload_base)
        payload_followup["meta"] = dict(payload_base.get("meta") or {})
        payload_followup["meta"]["outcome"] = event_type

        rows.append(
            {
                "event_timestamp": followup_at,
                "correlation_id": correlation_id,
                "service": "order-service",
                "event_type": event_type,
                "user_id": user_id,
                "entity_id": order_id,
                "payload": json.dumps(payload_followup, separators=(",", ":")),
            }
        )

    rows.sort(key=lambda r: r["event_timestamp"])
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed realistic order/payment business events into bronze.business_events."
    )
    parser.add_argument("--users", type=int, default=50, help="Number of users.")
    parser.add_argument("--orders", type=int, default=120, help="Number of orders.")
    args = parser.parse_args()

    cfg = SeedConfig(users=max(1, int(args.users)), orders=max(1, int(args.orders)))

    settings = load_settings()
    engine = get_engine(settings)

    now = datetime.now()
    seed_run_id = _seed_run_id(now)
    rows: list[dict] = []

    with engine.begin() as conn:
        run = start_run(conn, "seed_business_events")
        try:
            if _already_seeded(conn, seed_run_id):
                print(
                    f"Seed already present for seed_run_id='{seed_run_id}' in the last 7 days; exiting."
                )
                finish_run(conn, run, rows_inserted=0)
                return 0

            rows = _generate_orders(now=now, cfg=cfg, seed_run_id=seed_run_id)

            conn.execute(
                text("""
                    INSERT INTO bronze.business_events
                        (event_timestamp, correlation_id, service, event_type, user_id, entity_id, payload)
                    VALUES
                        (:event_timestamp, :correlation_id, :service, :event_type, :user_id, :entity_id, :payload);
                    """),
                rows,
            )

            finish_run(conn, run, rows_inserted=len(rows))
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print(f"Seed complete: inserted {len(rows)} rows into bronze.business_events.")
    print(f"- seed_run_id: {seed_run_id}")
    print(f"- users: {cfg.users}")
    print(f"- orders: {cfg.orders}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
