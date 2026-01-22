from __future__ import annotations

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine


def _counts_sql(table: str) -> str:
    return f"""
    SELECT
        COUNT(1) AS total_events,
        SUM(CASE WHEN COALESCE(properties, '') LIKE '%seed_run_id%' THEN 1 ELSE 0 END) AS seed_events,
        SUM(CASE WHEN COALESCE(properties, '') NOT LIKE '%seed_run_id%' THEN 1 ELSE 0 END) AS real_events
    FROM {table}
    WHERE CAST(event_timestamp AS date) = CAST(SYSUTCDATETIME() AS date);
    """


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.connect() as conn:
        pv = conn.execute(text(_counts_sql("bronze.page_view_events"))).mappings().one()
        ck = conn.execute(text(_counts_sql("bronze.click_events"))).mappings().one()

    print("Tracking verification (UTC today)")
    print(
        f"page_view_events: total={int(pv['total_events'] or 0)} real={int(pv['real_events'] or 0)} seed={int(pv['seed_events'] or 0)}"
    )
    print(
        f"click_events:     total={int(ck['total_events'] or 0)} real={int(ck['real_events'] or 0)} seed={int(ck['seed_events'] or 0)}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

