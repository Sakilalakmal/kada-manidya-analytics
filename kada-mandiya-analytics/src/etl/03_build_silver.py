from __future__ import annotations

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.begin() as conn:
        run = start_run(conn, "build_silver")
        try:
            conn.execute(text("""
                    ;WITH pv_ordered AS (
                        SELECT
                            session_id,
                            user_id,
                            event_timestamp,
                            page_url,
                            utm_source,
                            utm_medium,
                            utm_campaign,
                            ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS rn_asc,
                            ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp DESC) AS rn_desc
                        FROM bronze.page_view_events
                    ),
                    pv AS (
                        SELECT
                            session_id,
                            MAX(user_id) AS user_id,
                            MIN(event_timestamp) AS pv_start_time,
                            MAX(event_timestamp) AS pv_end_time,
                            COUNT(*) AS page_views,
                            MAX(CASE WHEN rn_asc = 1 THEN page_url END) AS entry_page,
                            MAX(CASE WHEN rn_desc = 1 THEN page_url END) AS exit_page,
                            MAX(CASE WHEN rn_asc = 1 THEN utm_source END) AS utm_source,
                            MAX(CASE WHEN rn_asc = 1 THEN utm_medium END) AS utm_medium,
                            MAX(CASE WHEN rn_asc = 1 THEN utm_campaign END) AS utm_campaign
                        FROM pv_ordered
                        GROUP BY session_id
                    ),
                    ce AS (
                        SELECT
                            session_id,
                            MIN(event_timestamp) AS click_start_time,
                            MAX(event_timestamp) AS click_end_time,
                            COUNT(*) AS clicks
                        FROM bronze.click_events
                        GROUP BY session_id
                    ),
                    bounds AS (
                        SELECT
                            pv.session_id,
                            pv.user_id,
                            CASE
                                WHEN ce.click_start_time IS NULL THEN pv.pv_start_time
                                WHEN pv.pv_start_time <= ce.click_start_time THEN pv.pv_start_time
                                ELSE ce.click_start_time
                            END AS start_time,
                            CASE
                                WHEN ce.click_end_time IS NULL THEN pv.pv_end_time
                                WHEN pv.pv_end_time >= ce.click_end_time THEN pv.pv_end_time
                                ELSE ce.click_end_time
                            END AS end_time,
                            pv.page_views,
                            ISNULL(ce.clicks, 0) AS clicks,
                            pv.entry_page,
                            pv.exit_page,
                            pv.utm_source,
                            pv.utm_medium,
                            pv.utm_campaign
                        FROM pv
                        LEFT JOIN ce ON pv.session_id = ce.session_id
                    )
                    MERGE silver.user_sessions AS tgt
                    USING (
                        SELECT
                            session_id,
                            user_id,
                            start_time,
                            end_time,
                            CASE
                                WHEN DATEDIFF(SECOND, start_time, end_time) < 0 THEN 0
                                ELSE DATEDIFF(SECOND, start_time, end_time)
                            END AS duration_seconds,
                            page_views,
                            clicks,
                            entry_page,
                            exit_page,
                            utm_source,
                            utm_medium,
                            utm_campaign
                        FROM bounds
                    ) AS src
                    ON tgt.session_id = src.session_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            tgt.user_id = src.user_id,
                            tgt.start_time = src.start_time,
                            tgt.end_time = src.end_time,
                            tgt.duration_seconds = src.duration_seconds,
                            tgt.page_views = src.page_views,
                            tgt.clicks = src.clicks,
                            tgt.entry_page = src.entry_page,
                            tgt.exit_page = src.exit_page,
                            tgt.utm_source = src.utm_source,
                            tgt.utm_medium = src.utm_medium,
                            tgt.utm_campaign = src.utm_campaign
                    WHEN NOT MATCHED THEN
                        INSERT (
                            session_id, user_id, start_time, end_time, duration_seconds,
                            page_views, clicks, entry_page, exit_page, utm_source, utm_medium, utm_campaign
                        )
                        VALUES (
                            src.session_id, src.user_id, src.start_time, src.end_time, src.duration_seconds,
                            src.page_views, src.clicks, src.entry_page, src.exit_page, src.utm_source, src.utm_medium, src.utm_campaign
                        );
                    """))

            conn.execute(text("DELETE FROM silver.page_sequence;"))
            conn.execute(text("""
                    INSERT INTO silver.page_sequence (session_id, step_number, page_url, event_timestamp)
                    SELECT
                        session_id,
                        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS step_number,
                        page_url,
                        event_timestamp
                    FROM bronze.page_view_events;
                    """))

            conn.execute(text("""
                    ;WITH src AS (
                        SELECT
                            ce.event_timestamp,
                            ce.session_id,
                            ce.user_id,
                            JSON_VALUE(ce.properties, '$.product_id') AS product_id,
                            COALESCE(JSON_VALUE(ce.properties, '$.interaction_type'), 'click') AS interaction_type,
                            ce.properties
                        FROM bronze.click_events ce
                        WHERE ce.properties IS NOT NULL
                          AND JSON_VALUE(ce.properties, '$.product_id') IS NOT NULL
                    )
                    MERGE silver.product_interactions AS tgt
                    USING src
                    ON tgt.session_id = src.session_id
                       AND tgt.event_timestamp = src.event_timestamp
                       AND tgt.product_id = src.product_id
                       AND tgt.interaction_type = src.interaction_type
                    WHEN NOT MATCHED THEN
                        INSERT (event_timestamp, session_id, user_id, product_id, interaction_type, properties)
                        VALUES (src.event_timestamp, src.session_id, src.user_id, src.product_id, src.interaction_type, src.properties);
                    """))

            finish_run(conn, run, rows_inserted=0)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print("Silver layer built: user_sessions, page_sequence, product_interactions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
