from __future__ import annotations

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine


def _print_rows(title: str, rows) -> None:
    print(title)
    for r in rows:
        print("- " + " ".join([f"{k}={r.get(k)}" for k in r.keys()]))


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.connect() as conn:
        behavior = conn.execute(
            text("""
                SELECT TOP 5 *
                FROM gold.behavior_daily
                ORDER BY metric_date DESC;
            """)
        ).mappings()
        funnel = conn.execute(
            text("""
                SELECT TOP 5 *
                FROM gold.funnel_daily
                ORDER BY metric_date DESC;
            """)
        ).mappings()
        web = conn.execute(
            text("""
                SELECT TOP 5
                    event_timestamp, event_date, session_id, user_id, event_type, page_url, product_id, element_id
                FROM silver.web_events
                ORDER BY event_timestamp DESC;
            """)
        ).mappings()
        purchases = conn.execute(
            text("""
                SELECT TOP 5
                    event_timestamp, event_date, order_id, user_id, total_amount, currency, correlation_id, source_service
                FROM silver.purchases
                ORDER BY event_timestamp DESC;
            """)
        ).mappings()

        _print_rows("gold.behavior_daily last_5:", behavior)
        _print_rows("gold.funnel_daily last_5:", funnel)
        _print_rows("silver.web_events last_5:", web)
        _print_rows("silver.purchases last_5:", purchases)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

