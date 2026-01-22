from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run


@dataclass(frozen=True)
class SeedSession:
    session_id: str
    user_id: str | None
    product_id: str | None
    order_created_at: datetime


def _seed_run_id(now: datetime) -> str:
    return now.strftime("%Y-%m-%d-seed-business")


def _already_seeded(conn, seed_run_id: str, *, lookback_days: int) -> bool:
    pattern = f'%\"seed_run_id\":\"{seed_run_id}\"%'
    row = conn.execute(
        text(
            """
            SELECT TOP 1 1
            FROM bronze.page_view_events
            WHERE event_timestamp >= DATEADD(day, -:days, SYSDATETIME())
              AND COALESCE(properties, '') LIKE :pattern

            UNION ALL

            SELECT TOP 1 1
            FROM bronze.click_events
            WHERE event_timestamp >= DATEADD(day, -:days, SYSDATETIME())
              AND COALESCE(properties, '') LIKE :pattern;
            """
        ),
        {"pattern": pattern, "days": int(lookback_days)},
    ).first()
    return row is not None


def _pick_product_id(payload_raw: str) -> str | None:
    try:
        obj: Any = json.loads(payload_raw)
    except Exception:
        return None

    if isinstance(obj, dict):
        items = obj.get("items")
        if isinstance(items, list) and items:
            first = items[0]
            if isinstance(first, dict):
                pid = first.get("product_id")
                if pid is not None:
                    s = str(pid).strip()
                    return s[:64] if s else None

        for key in ["product_id", "productId"]:
            pid = obj.get(key)
            if pid is not None:
                s = str(pid).strip()
                return s[:64] if s else None

        meta = obj.get("meta")
        if isinstance(meta, dict):
            pid = meta.get("product_id")
            if pid is not None:
                s = str(pid).strip()
                return s[:64] if s else None

    return None


def _load_seed_sessions(conn, seed_run_id: str, *, lookback_days: int) -> list[SeedSession]:
    pattern = f'%\"seed_run_id\":\"{seed_run_id}\"%'
    rows = conn.execute(
        text(
            """
            SELECT
                be.event_timestamp,
                be.correlation_id,
                be.user_id,
                be.payload
            FROM bronze.business_events be
            WHERE be.event_timestamp >= DATEADD(day, -:days, SYSDATETIME())
              AND be.payload LIKE :pattern
              AND LOWER(LTRIM(RTRIM(be.event_type))) IN ('order.created', 'order_created')
              AND be.correlation_id IS NOT NULL
            ORDER BY be.event_timestamp ASC;
            """
        ),
        {"pattern": pattern, "days": int(lookback_days)},
    ).mappings()

    sessions: list[SeedSession] = []
    for r in rows:
        cid = str(r.get("correlation_id") or "").strip()
        if not cid:
            continue
        sessions.append(
            SeedSession(
                session_id=cid[:64],
                user_id=(str(r.get("user_id")).strip()[:64] if r.get("user_id") else None),
                product_id=_pick_product_id(str(r.get("payload") or "")),
                order_created_at=r["event_timestamp"],
            )
        )
    return sessions


def _pv_props(seed_run_id: str, product_id: str | None = None) -> str:
    payload: dict[str, Any] = {"seed_run_id": seed_run_id, "source_type": "seed"}
    if product_id:
        payload["product_id"] = product_id
    payload["load_time_ms"] = 200 + (hash(seed_run_id) % 400)
    return json.dumps(payload, separators=(",", ":"))


def _click_props(seed_run_id: str, product_id: str | None, interaction_type: str | None) -> str:
    payload: dict[str, Any] = {"seed_run_id": seed_run_id, "source_type": "seed"}
    if product_id:
        payload["product_id"] = product_id
    if interaction_type:
        payload["interaction_type"] = interaction_type
    return json.dumps(payload, separators=(",", ":"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Seed realistic web behavior events into bronze.page_view_events and bronze.click_events."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window for seed data (default: 7).",
    )
    args = parser.parse_args()
    lookback_days = max(1, int(args.days))

    settings = load_settings()
    engine = get_engine(settings)

    now = datetime.now()
    seed_run_id = _seed_run_id(now)
    sessions: list[SeedSession] = []
    rows_inserted = 0

    with engine.begin() as conn:
        run = start_run(conn, "seed_behavior_events")
        try:
            if _already_seeded(conn, seed_run_id, lookback_days=lookback_days):
                print(
                    f"Seed already present for seed_run_id='{seed_run_id}' in the last {lookback_days} days; exiting."
                )
                finish_run(conn, run, rows_inserted=0)
                return 0

            sessions = _load_seed_sessions(conn, seed_run_id, lookback_days=lookback_days)
            if not sessions:
                print(
                    f"No matching seed business events found for seed_run_id='{seed_run_id}' in the last {lookback_days} days; skipping."
                )
                finish_run(conn, run, rows_inserted=0)
                return 0

            page_views: list[dict[str, Any]] = []
            clicks: list[dict[str, Any]] = []

            for s in sessions:
                rng = random.Random(f"{seed_run_id}:{s.session_id}")
                product_id = s.product_id or f"P{rng.randint(1, 20):03d}"
                product_url = f"/products/{product_id}"

                end_ts = s.order_created_at
                start_ts = end_ts - timedelta(minutes=rng.randint(2, 20), seconds=rng.randint(0, 59))

                t1 = start_ts
                t2 = t1 + timedelta(seconds=rng.randint(10, 40))
                t3 = t2 + timedelta(seconds=rng.randint(10, 40))
                t4 = t3 + timedelta(seconds=rng.randint(5, 25))
                t5 = t4 + timedelta(seconds=rng.randint(10, 45))
                t6 = t5 + timedelta(seconds=rng.randint(5, 30))

                page_views.append(
                    {
                        "event_timestamp": t1,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": "/",
                        "referrer_url": None,
                        "utm_source": "seed",
                        "utm_medium": "demo",
                        "utm_campaign": seed_run_id,
                        "time_on_prev_page_seconds": None,
                        "properties": _pv_props(seed_run_id),
                    }
                )
                page_views.append(
                    {
                        "event_timestamp": t2,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": "/products",
                        "referrer_url": "/",
                        "utm_source": "seed",
                        "utm_medium": "demo",
                        "utm_campaign": seed_run_id,
                        "time_on_prev_page_seconds": int((t2 - t1).total_seconds()),
                        "properties": _pv_props(seed_run_id),
                    }
                )
                page_views.append(
                    {
                        "event_timestamp": t3,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": product_url,
                        "referrer_url": "/products",
                        "utm_source": "seed",
                        "utm_medium": "demo",
                        "utm_campaign": seed_run_id,
                        "time_on_prev_page_seconds": int((t3 - t2).total_seconds()),
                        "properties": _pv_props(seed_run_id, product_id),
                    }
                )

                clicks.append(
                    {
                        "event_timestamp": t4,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": product_url,
                        "element_id": "btn_add_to_cart",
                        "properties": _click_props(seed_run_id, product_id, "add_to_cart"),
                    }
                )
                clicks.append(
                    {
                        "event_timestamp": t5,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": "/products",
                        "element_id": "btn_checkout",
                        "properties": _click_props(seed_run_id, product_id, "begin_checkout"),
                    }
                )
                page_views.append(
                    {
                        "event_timestamp": t6,
                        "session_id": s.session_id,
                        "user_id": s.user_id,
                        "page_url": "/checkout",
                        "referrer_url": product_url,
                        "utm_source": "seed",
                        "utm_medium": "demo",
                        "utm_campaign": seed_run_id,
                        "time_on_prev_page_seconds": int((t6 - t5).total_seconds()),
                        "properties": _pv_props(seed_run_id),
                    }
                )

            conn.execute(
                text(
                    """
                    INSERT INTO bronze.page_view_events
                        (event_timestamp, session_id, user_id, page_url, referrer_url,
                         utm_source, utm_medium, utm_campaign, time_on_prev_page_seconds, properties)
                    VALUES
                        (:event_timestamp, :session_id, :user_id, :page_url, :referrer_url,
                         :utm_source, :utm_medium, :utm_campaign, :time_on_prev_page_seconds, :properties);
                    """
                ),
                page_views,
            )
            conn.execute(
                text(
                    """
                    INSERT INTO bronze.click_events
                        (event_timestamp, session_id, user_id, page_url, element_id, properties)
                    VALUES
                        (:event_timestamp, :session_id, :user_id, :page_url, :element_id, :properties);
                    """
                ),
                clicks,
            )

            rows_inserted = len(page_views) + len(clicks)
            finish_run(conn, run, rows_inserted=rows_inserted)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print(
        f"Seed complete: inserted {rows_inserted} rows into bronze.page_view_events/bronze.click_events."
    )
    print(f"- seed_run_id: {seed_run_id}")
    print(f"- sessions: {len(sessions)}")
    print(f"- days: {lookback_days}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
