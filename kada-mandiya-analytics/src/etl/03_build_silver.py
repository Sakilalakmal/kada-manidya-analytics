from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.db.writers import insert_dead_letter
from src.etl._ops import fail_run, finish_run, start_run
from src.etl.utils_business_events import (
    best_effort_amount,
    best_effort_comment,
    best_effort_currency,
    best_effort_order_id,
    best_effort_payment_id,
    best_effort_product_id,
    best_effort_provider,
    best_effort_rating,
    best_effort_review_id,
    canonical_event_type,
    deterministic_id,
    ensure_utc,
    normalize_items,
    parse_json_payload,
)
from src.utils.time import to_sqlserver_utc_naive, utc_now


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 365) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None and str(raw).strip() else int(default)
    except Exception:
        value = int(default)
    return max(min_value, min(max_value, value))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Silver layer from Bronze events")
    p.add_argument(
        "--days",
        type=int,
        default=_env_int("ETL_RECENT_DAYS", 30),
        help="Recompute window (days) for business events (default: 30)",
    )
    return p.parse_args()


def _sql_in_list(name: str, values: list[str]) -> tuple[str, dict[str, object]]:
    params: dict[str, object] = {}
    placeholders: list[str] = []
    for i, v in enumerate(values):
        key = f"{name}{i}"
        placeholders.append(f":{key}")
        params[key] = v
    return ",".join(placeholders), params


def _existing_ids(conn, table: str, key_col: str, ids: list[str]) -> set[str]:
    if not ids:
        return set()
    ph, params = _sql_in_list("id", ids)
    rows = conn.execute(
        text(f"SELECT {key_col} AS id FROM {table} WHERE {key_col} IN ({ph});"), params
    ).mappings()
    return {str(r["id"]) for r in rows}


def _delete_order_items(conn, order_ids: list[str]) -> int:
    if not order_ids:
        return 0
    ph, params = _sql_in_list("oid", order_ids)
    result = conn.execute(
        text(f"DELETE FROM silver.order_items WHERE order_id IN ({ph});"), params
    )
    return int(getattr(result, "rowcount", 0) or 0)


def _table_exists(conn, qualified_name: str) -> bool:
    oid = conn.execute(text("SELECT OBJECT_ID(:name, 'U');"), {"name": qualified_name}).scalar()
    return oid is not None


def _table_columns(conn, qualified_name: str) -> set[str]:
    rows = conn.execute(
        text("""
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = OBJECT_ID(:name, 'U');
        """),
        {"name": qualified_name},
    ).mappings()
    return {str(r["name"]).lower() for r in rows}


def _ensure_behavior_tables(conn) -> None:
    conn.execute(
        text("""
        IF OBJECT_ID('silver.web_events', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.web_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_silver_web_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_silver_web_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                event_date date NOT NULL,
                session_id nvarchar(64) NOT NULL,
                user_id nvarchar(64) NULL,
                event_type nvarchar(64) NOT NULL,
                page_url nvarchar(2000) NOT NULL,
                product_id nvarchar(64) NULL,
                element_id nvarchar(255) NULL,
                properties_json nvarchar(max) NULL
            );
        END

        IF OBJECT_ID('silver.purchases', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.purchases(
                order_id nvarchar(64) NOT NULL CONSTRAINT PK_silver_purchases PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                event_date date NOT NULL,
                user_id nvarchar(64) NULL,
                total_amount decimal(12,2) NULL,
                currency nvarchar(10) NULL,
                correlation_id nvarchar(64) NULL,
                source_service nvarchar(64) NULL
            );
        END
        """)
    )


def build_silver_web_events(conn) -> int:
    _ensure_behavior_tables(conn)

    max_ts = conn.execute(text("SELECT MAX(event_timestamp) FROM silver.web_events;")).scalar()
    if max_ts is None:
        since_ts = to_sqlserver_utc_naive(utc_now() - timedelta(days=_env_int("ETL_RECENT_DAYS", 30)))
    else:
        since_ts = max_ts - timedelta(minutes=2)

    if _table_exists(conn, "bronze.tracker_events"):
        cols = _table_columns(conn, "bronze.tracker_events")
        if "event_timestamp" not in cols and "timestamp" not in cols:
            cols = set()
        has_event_id = "event_id" in cols
        has_event_type = "event_type" in cols
        has_page_url = "page_url" in cols
        has_element_id = "element_id" in cols
        has_session_id = "session_id" in cols
        has_user_id = "user_id" in cols

        if not cols or not has_session_id:
            sql = ""
        else:
            ts_col = "t.event_timestamp" if "event_timestamp" in cols else "t.[timestamp]"

            src_event_id = "t.event_id" if has_event_id else "CAST(NULL AS uniqueidentifier)"
            src_event_type = "t.event_type" if has_event_type else "JSON_VALUE(t.properties, '$.event_type')"
            src_page_url = (
                "t.page_url" if has_page_url else "JSON_VALUE(t.properties, '$.page_url')"
            )
            src_element_id = (
                "t.element_id" if has_element_id else "JSON_VALUE(t.properties, '$.element_id')"
            )
            src_user_id = "t.user_id" if has_user_id else "JSON_VALUE(t.properties, '$.user_id')"

            sql = f"""
            ;WITH src AS (
                SELECT
                    {src_event_id} AS source_event_id,
                    {ts_col} AS event_timestamp,
                    CAST({ts_col} AS date) AS event_date,
                    t.session_id,
                    {src_user_id} AS user_id,
                    COALESCE({src_event_type}, 'unknown') AS event_type,
                    COALESCE({src_page_url}, '') AS page_url,
                COALESCE(
                    JSON_VALUE(t.properties, '$.product_id'),
                    CASE
                        WHEN COALESCE({src_page_url}, '') LIKE '/products/%'
                            THEN RIGHT(COALESCE({src_page_url}, ''), CHARINDEX('/', REVERSE(COALESCE({src_page_url}, ''))) - 1)
                            ELSE NULL
                        END
                ) AS product_id,
                {src_element_id} AS element_id,
                t.properties AS properties_json
            FROM bronze.tracker_events t
            WHERE {ts_col} >= :since_ts
            ),
            dedup AS (
                SELECT
                    event_timestamp,
                    event_date,
                    session_id,
                    user_id,
                    event_type,
                    page_url,
                    product_id,
                    element_id,
                    properties_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            event_timestamp,
                            session_id,
                            event_type,
                            page_url,
                            ISNULL(element_id, ''),
                            ISNULL(product_id, '')
                        ORDER BY source_event_id
                    ) AS rn
                FROM src
            )
            INSERT INTO silver.web_events
                (event_timestamp, event_date, session_id, user_id, event_type, page_url, product_id, element_id, properties_json)
            SELECT
                d.event_timestamp,
                d.event_date,
                d.session_id,
                d.user_id,
                d.event_type,
                d.page_url,
                d.product_id,
                d.element_id,
                d.properties_json
            FROM dedup d
            WHERE d.rn = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM silver.web_events w
                  WHERE w.event_timestamp = d.event_timestamp
                    AND w.session_id = d.session_id
                    AND w.event_type = d.event_type
                    AND w.page_url = d.page_url
                    AND ISNULL(w.element_id, '') = ISNULL(d.element_id, '')
                    AND ISNULL(w.product_id, '') = ISNULL(d.product_id, '')
              );
            """
    else:
        sql = """
        ;WITH src AS (
            SELECT
                pv.event_id AS source_event_id,
                pv.event_timestamp,
                CAST(pv.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(pv.session_id)), ''),
                    JSON_VALUE(pv.properties, '$.session_id'),
                    JSON_VALUE(pv.properties, '$.sessionId'),
                    CONCAT('derived:', COALESCE(pv.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(pv.event_timestamp AS date), 120))
                ) AS session_id,
                pv.user_id,
                'page_view' AS event_type,
                pv.page_url,
                COALESCE(
                    JSON_VALUE(pv.properties, '$.product_id'),
                    CASE
                        WHEN pv.page_url LIKE '/products/%'
                        THEN RIGHT(pv.page_url, CHARINDEX('/', REVERSE(pv.page_url)) - 1)
                        ELSE NULL
                    END
                ) AS product_id,
                CAST(NULL AS nvarchar(255)) AS element_id,
                pv.properties AS properties_json
            FROM bronze.page_view_events pv
            WHERE pv.event_timestamp >= :since_ts

            UNION ALL

            SELECT
                ce.event_id AS source_event_id,
                ce.event_timestamp,
                CAST(ce.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(ce.session_id)), ''),
                    JSON_VALUE(ce.properties, '$.session_id'),
                    JSON_VALUE(ce.properties, '$.sessionId'),
                    CONCAT('derived:', COALESCE(ce.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(ce.event_timestamp AS date), 120))
                ) AS session_id,
                ce.user_id,
                CASE
                    WHEN ce.element_id = 'btn_add_to_cart' THEN 'add_to_cart'
                    WHEN COALESCE(JSON_VALUE(ce.properties, '$.interaction_type'), '') = 'add_to_cart' THEN 'add_to_cart'
                    WHEN ce.element_id = 'btn_checkout' THEN 'begin_checkout'
                    WHEN COALESCE(JSON_VALUE(ce.properties, '$.interaction_type'), '') IN ('checkout_started', 'begin_checkout') THEN 'begin_checkout'
                    WHEN ce.element_id = 'purchase_completed' THEN 'purchase'
                    WHEN COALESCE(JSON_VALUE(ce.properties, '$.interaction_type'), '') IN ('purchase_completed', 'purchase') THEN 'purchase'
                    ELSE 'click'
                END AS event_type,
                ce.page_url,
                COALESCE(
                    JSON_VALUE(ce.properties, '$.product_id'),
                    CASE
                        WHEN ce.page_url LIKE '/products/%'
                        THEN RIGHT(ce.page_url, CHARINDEX('/', REVERSE(ce.page_url)) - 1)
                        ELSE NULL
                    END
                ) AS product_id,
                ce.element_id,
                ce.properties AS properties_json
            FROM bronze.click_events ce
            WHERE ce.event_timestamp >= :since_ts

            UNION ALL

            SELECT
                c.event_id AS source_event_id,
                c.event_timestamp,
                CAST(c.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(c.session_id)), ''),
                    CONCAT('derived:', COALESCE(c.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(c.event_timestamp AS date), 120))
                ) AS session_id,
                c.user_id,
                'add_to_cart' AS event_type,
                COALESCE(c.page_url, '/') AS page_url,
                c.product_id,
                CAST('btn_add_to_cart' AS nvarchar(255)) AS element_id,
                c.payload AS properties_json
            FROM bronze.cart_events c
            WHERE c.event_timestamp >= :since_ts

            UNION ALL

            SELECT
                co.event_id AS source_event_id,
                co.event_timestamp,
                CAST(co.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(co.session_id)), ''),
                    CONCAT('derived:', COALESCE(co.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(co.event_timestamp AS date), 120))
                ) AS session_id,
                co.user_id,
                'begin_checkout' AS event_type,
                COALESCE(co.page_url, '/') AS page_url,
                CAST(NULL AS nvarchar(64)) AS product_id,
                CAST('btn_checkout' AS nvarchar(255)) AS element_id,
                co.payload AS properties_json
            FROM bronze.checkout_events co
            WHERE co.event_timestamp >= :since_ts

            UNION ALL

            SELECT
                se.event_id AS source_event_id,
                se.event_timestamp,
                CAST(se.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(se.session_id)), ''),
                    JSON_VALUE(se.properties, '$.session_id'),
                    JSON_VALUE(se.properties, '$.sessionId'),
                    CONCAT('derived:', COALESCE(se.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(se.event_timestamp AS date), 120))
                ) AS session_id,
                se.user_id,
                'scroll' AS event_type,
                se.page_url,
                COALESCE(JSON_VALUE(se.properties, '$.product_id'), NULL) AS product_id,
                CAST(NULL AS nvarchar(255)) AS element_id,
                se.properties AS properties_json
            FROM bronze.scroll_events se
            WHERE se.event_timestamp >= :since_ts

            UNION ALL

            SELECT
                sr.event_id AS source_event_id,
                sr.event_timestamp,
                CAST(sr.event_timestamp AS date) AS event_date,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(sr.session_id)), ''),
                    JSON_VALUE(sr.properties, '$.session_id'),
                    JSON_VALUE(sr.properties, '$.sessionId'),
                    CONCAT('derived:', COALESCE(sr.user_id, 'anon'), ':', CONVERT(varchar(10), CAST(sr.event_timestamp AS date), 120))
                ) AS session_id,
                sr.user_id,
                'search' AS event_type,
                sr.page_url,
                COALESCE(JSON_VALUE(sr.properties, '$.product_id'), NULL) AS product_id,
                CAST(NULL AS nvarchar(255)) AS element_id,
                COALESCE(
                    sr.properties,
                    CONCAT(
                        '{\"query\":', QUOTENAME(ISNULL(sr.query, N''), '\"'),
                        ',\"results_count\":', COALESCE(CONVERT(varchar(20), sr.results_count), 'null'),
                        ',\"filters\":', COALESCE(sr.filters, 'null'),
                        '}'
                    )
                ) AS properties_json
            FROM bronze.search_events sr
            WHERE sr.event_timestamp >= :since_ts
        ),
        dedup AS (
            SELECT
                event_timestamp,
                event_date,
                session_id,
                user_id,
                event_type,
                page_url,
                product_id,
                element_id,
                properties_json,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        event_timestamp,
                        session_id,
                        event_type,
                        page_url,
                        ISNULL(element_id, ''),
                        ISNULL(product_id, '')
                    ORDER BY source_event_id
                ) AS rn
            FROM src
        )
        INSERT INTO silver.web_events
            (event_timestamp, event_date, session_id, user_id, event_type, page_url, product_id, element_id, properties_json)
        SELECT
            d.event_timestamp,
            d.event_date,
            d.session_id,
            d.user_id,
            d.event_type,
            d.page_url,
            d.product_id,
            d.element_id,
            d.properties_json
        FROM dedup d
        WHERE d.rn = 1
          AND NOT EXISTS (
              SELECT 1
              FROM silver.web_events w
              WHERE w.event_timestamp = d.event_timestamp
                AND w.session_id = d.session_id
                AND w.event_type = d.event_type
                AND w.page_url = d.page_url
                AND ISNULL(w.element_id, '') = ISNULL(d.element_id, '')
                AND ISNULL(w.product_id, '') = ISNULL(d.product_id, '')
          );
        """

    if not sql:
        return 0

    res = conn.execute(text(sql), {"since_ts": since_ts})
    rc = int(getattr(res, "rowcount", 0) or 0)
    return rc if rc > 0 else 0


def build_silver_purchases(conn) -> int:
    _ensure_behavior_tables(conn)

    since_ts = to_sqlserver_utc_naive(utc_now() - timedelta(days=_env_int("ETL_RECENT_DAYS", 30)))
    res = conn.execute(
        text(
            """
            ;WITH src AS (
                SELECT
                    be.event_timestamp,
                    CAST(be.event_timestamp AS date) AS event_date,
                    COALESCE(
                        be.entity_id,
                        JSON_VALUE(be.payload, '$.order_id'),
                        JSON_VALUE(be.payload, '$.data.order_id'),
                        JSON_VALUE(be.payload, '$.order.order_id')
                    ) AS order_id,
                    be.user_id,
                    TRY_CONVERT(
                        decimal(12,2),
                        COALESCE(
                            JSON_VALUE(be.payload, '$.total_amount'),
                            JSON_VALUE(be.payload, '$.data.total_amount'),
                            JSON_VALUE(be.payload, '$.order.total_amount'),
                            JSON_VALUE(be.payload, '$.amount'),
                            JSON_VALUE(be.payload, '$.data.amount')
                        )
                    ) AS total_amount,
                    COALESCE(
                        JSON_VALUE(be.payload, '$.currency'),
                        JSON_VALUE(be.payload, '$.data.currency'),
                        JSON_VALUE(be.payload, '$.order.currency'),
                        JSON_VALUE(be.payload, '$.payment.currency')
                    ) AS currency,
                    be.correlation_id,
                    be.service AS source_service,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(
                            be.entity_id,
                            JSON_VALUE(be.payload, '$.order_id'),
                            JSON_VALUE(be.payload, '$.data.order_id'),
                            JSON_VALUE(be.payload, '$.order.order_id')
                        )
                        ORDER BY be.event_timestamp ASC
                    ) AS rn
                FROM bronze.business_events be
                WHERE be.event_timestamp >= :since_ts
                  AND LOWER(LTRIM(RTRIM(be.event_type))) IN (
                      'order_paid',
                      'order.paid',
                      'payment.succeeded',
                      'payment.success',
                      'payment_success'
                  )
            ),
            dedup AS (
                SELECT *
                FROM src
                WHERE rn = 1 AND order_id IS NOT NULL
            )
            MERGE silver.purchases AS tgt
            USING dedup AS src
            ON tgt.order_id = src.order_id
            WHEN NOT MATCHED THEN
                INSERT (order_id, event_timestamp, event_date, user_id, total_amount, currency, correlation_id, source_service)
                VALUES (src.order_id, src.event_timestamp, src.event_date, src.user_id, src.total_amount, src.currency, src.correlation_id, src.source_service)
            WHEN MATCHED AND src.event_timestamp < tgt.event_timestamp THEN
                UPDATE SET
                    tgt.event_timestamp = src.event_timestamp,
                    tgt.event_date = src.event_date,
                    tgt.user_id = src.user_id,
                    tgt.total_amount = src.total_amount,
                    tgt.currency = src.currency,
                    tgt.correlation_id = src.correlation_id,
                    tgt.source_service = src.source_service;
            """
        ),
        {"since_ts": since_ts},
    )
    rc = int(getattr(res, "rowcount", 0) or 0)
    return rc if rc > 0 else 0


def _ensure_session_fact_tables(conn) -> None:
    conn.execute(
        text(
            """
            IF OBJECT_ID('silver.sessions', 'U') IS NULL
            BEGIN
                CREATE TABLE silver.sessions(
                    session_id nvarchar(64) NOT NULL CONSTRAINT PK_silver_sessions PRIMARY KEY,
                    user_id nvarchar(64) NULL,
                    session_start datetime2 NOT NULL,
                    session_end datetime2 NOT NULL,
                    duration_seconds int NOT NULL,
                    events_count int NOT NULL
                );
            END

            IF OBJECT_ID('silver.session_events', 'U') IS NULL
            BEGIN
                CREATE TABLE silver.session_events(
                    event_id uniqueidentifier NOT NULL
                        CONSTRAINT DF_silver_session_events_event_id DEFAULT NEWID()
                        CONSTRAINT PK_silver_session_events PRIMARY KEY,
                    event_timestamp datetime2 NOT NULL,
                    event_date date NOT NULL,
                    session_id nvarchar(64) NOT NULL,
                    user_id nvarchar(64) NULL,
                    event_type nvarchar(64) NOT NULL,
                    page_url nvarchar(2000) NULL,
                    product_id nvarchar(64) NULL,
                    element_id nvarchar(255) NULL,
                    properties_json nvarchar(max) NULL
                );
            END

            IF OBJECT_ID('silver.session_products', 'U') IS NULL
            BEGIN
                CREATE TABLE silver.session_products(
                    session_id nvarchar(64) NOT NULL,
                    product_id nvarchar(64) NOT NULL,
                    first_seen datetime2 NOT NULL,
                    last_seen datetime2 NOT NULL,
                    views int NOT NULL,
                    clicks int NOT NULL,
                    add_to_cart int NOT NULL,
                    begin_checkout int NOT NULL,
                    purchases int NOT NULL,
                    CONSTRAINT PK_silver_session_products PRIMARY KEY (session_id, product_id)
                );
            END
            """
        )
    )


def build_silver_session_facts(conn, *, days: int) -> int:
    _ensure_session_fact_tables(conn)

    since_ts = to_sqlserver_utc_naive(utc_now() - timedelta(days=max(1, int(days or 30))))

    conn.execute(
        text(
            """
            DELETE sp
            FROM silver.session_products sp
            INNER JOIN silver.sessions s ON sp.session_id = s.session_id
            WHERE s.session_end >= :since_ts;
            """
        ),
        {"since_ts": since_ts},
    )
    conn.execute(
        text("DELETE FROM silver.session_events WHERE event_timestamp >= :since_ts;"),
        {"since_ts": since_ts},
    )
    conn.execute(
        text("DELETE FROM silver.sessions WHERE session_end >= :since_ts;"),
        {"since_ts": since_ts},
    )

    # Sessionize web events (prefer explicit session_id; derive by time-window when missing)
    res = conn.execute(
        text(
            """
            ;WITH web_base AS (
                SELECT
                    we.event_timestamp,
                    CAST(we.event_timestamp AS date) AS event_date,
                    NULLIF(LTRIM(RTRIM(we.user_id)), '') AS user_id,
                    NULLIF(LTRIM(RTRIM(we.session_id)), '') AS raw_session_id,
                    we.event_type,
                    we.page_url,
                    NULLIF(LTRIM(RTRIM(we.product_id)), '') AS product_id,
                    we.element_id,
                    we.properties_json
                FROM silver.web_events we
                WHERE we.event_timestamp >= :since_ts
            ),
            web_norm AS (
                SELECT
                    *,
                    CASE
                        WHEN raw_session_id IS NULL THEN NULL
                        WHEN raw_session_id LIKE 'derived:%' THEN NULL
                        ELSE raw_session_id
                    END AS provided_session_id
                FROM web_base
            ),
            web_with_prev AS (
                SELECT
                    *,
                    LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_ts
                FROM web_norm
            ),
            web_flags AS (
                SELECT
                    *,
                    CASE
                        WHEN provided_session_id IS NOT NULL THEN 0
                        WHEN user_id IS NULL THEN 0
                        WHEN prev_ts IS NULL THEN 1
                        WHEN DATEDIFF(minute, prev_ts, event_timestamp) > 30 THEN 1
                        ELSE 0
                    END AS new_session_flag
                FROM web_with_prev
            ),
            web_groups AS (
                SELECT
                    *,
                    CASE
                        WHEN provided_session_id IS NOT NULL THEN 0
                        WHEN user_id IS NULL THEN 0
                        ELSE SUM(new_session_flag) OVER (
                            PARTITION BY user_id
                            ORDER BY event_timestamp
                            ROWS UNBOUNDED PRECEDING
                        )
                    END AS session_group
                FROM web_flags
            ),
            web_start AS (
                SELECT
                    *,
                    CASE
                        WHEN provided_session_id IS NOT NULL THEN MIN(event_timestamp) OVER (PARTITION BY provided_session_id)
                        WHEN user_id IS NULL THEN MIN(event_timestamp) OVER (PARTITION BY page_url, event_date)
                        ELSE MIN(event_timestamp) OVER (PARTITION BY user_id, session_group)
                    END AS session_start_ts
                FROM web_groups
            ),
            web_sessioned AS (
                SELECT
                    event_timestamp,
                    event_date,
                    COALESCE(
                        provided_session_id,
                        CASE
                            WHEN user_id IS NULL THEN CONVERT(
                                varchar(64),
                                HASHBYTES('SHA2_256', CONCAT('anon|', COALESCE(page_url, ''), '|', CONVERT(varchar(10), event_date, 120))),
                                2
                            )
                            ELSE CONVERT(
                                varchar(64),
                                HASHBYTES('SHA2_256', CONCAT('u:', user_id, '|', CONVERT(varchar(19), session_start_ts, 126), '|', CAST(session_group AS varchar(10)))),
                                2
                            )
                        END
                    ) AS session_id,
                    user_id,
                    event_type,
                    page_url,
                    product_id,
                    element_id,
                    properties_json
                FROM web_start
            ),
            web_bounds AS (
                SELECT
                    session_id,
                    user_id,
                    MIN(event_timestamp) AS session_start,
                    MAX(event_timestamp) AS session_end
                FROM web_sessioned
                GROUP BY session_id, user_id
            ),
            purchases_src AS (
                SELECT
                    p.event_timestamp,
                    CAST(p.event_timestamp AS date) AS event_date,
                    NULLIF(LTRIM(RTRIM(p.user_id)), '') AS user_id,
                    p.order_id,
                    p.total_amount,
                    p.currency
                FROM silver.purchases p
                WHERE p.event_timestamp >= :since_ts
            ),
            purchase_sessioned AS (
                SELECT
                    p.event_timestamp,
                    p.event_date,
                    COALESCE(
                        wb.session_id,
                        CONVERT(
                            varchar(64),
                            HASHBYTES(
                                'SHA2_256',
                                CONCAT(
                                    'purchase|u:',
                                    COALESCE(p.user_id, 'anon'),
                                    '|',
                                    CONVERT(
                                        varchar(19),
                                        DATEADD(minute, (DATEDIFF(minute, 0, p.event_timestamp) / 30) * 30, 0),
                                        126
                                    )
                                )
                            ),
                            2
                        )
                    ) AS session_id,
                    p.user_id,
                    'purchase' AS event_type,
                    CAST(NULL AS nvarchar(2000)) AS page_url,
                    CAST(NULL AS nvarchar(64)) AS product_id,
                    CAST('purchase' AS nvarchar(255)) AS element_id,
                    CONCAT(
                        '{',
                            '\"order_id\":', QUOTENAME(ISNULL(p.order_id, N''), '\"'),
                            ',\"total_amount\":', COALESCE(CONVERT(varchar(50), p.total_amount), 'null'),
                            ',\"currency\":', QUOTENAME(ISNULL(p.currency, N''), '\"'),
                        '}'
                    ) AS properties_json
                FROM purchases_src p
                OUTER APPLY (
                    SELECT TOP 1 b.session_id
                    FROM web_bounds b
                    WHERE b.user_id = p.user_id
                      AND p.event_timestamp >= DATEADD(minute, -15, b.session_start)
                      AND p.event_timestamp <= DATEADD(minute, 30, b.session_end)
                    ORDER BY b.session_end DESC
                ) wb
            ),
            src AS (
                SELECT * FROM web_sessioned
                UNION ALL
                SELECT
                    event_timestamp,
                    event_date,
                    session_id,
                    user_id,
                    event_type,
                    page_url,
                    product_id,
                    element_id,
                    properties_json
                FROM purchase_sessioned
            ),
            dedup AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            event_timestamp,
                            session_id,
                            event_type,
                            ISNULL(page_url, ''),
                            ISNULL(element_id, ''),
                            ISNULL(product_id, '')
                        ORDER BY event_timestamp
                    ) AS rn
                FROM src
            )
            INSERT INTO silver.session_events
                (event_timestamp, event_date, session_id, user_id, event_type, page_url, product_id, element_id, properties_json)
            SELECT
                event_timestamp, event_date, session_id, user_id, event_type, page_url, product_id, element_id, properties_json
            FROM dedup
            WHERE rn = 1;
            """
        ),
        {"since_ts": since_ts},
    )
    rows = int(getattr(res, "rowcount", 0) or 0)

    conn.execute(
        text(
            """
            INSERT INTO silver.sessions (session_id, user_id, session_start, session_end, duration_seconds, events_count)
            SELECT
                session_id,
                MAX(user_id) AS user_id,
                MIN(event_timestamp) AS session_start,
                MAX(event_timestamp) AS session_end,
                CASE
                    WHEN DATEDIFF(second, MIN(event_timestamp), MAX(event_timestamp)) < 0 THEN 0
                    ELSE DATEDIFF(second, MIN(event_timestamp), MAX(event_timestamp))
                END AS duration_seconds,
                COUNT(*) AS events_count
            FROM silver.session_events
            WHERE event_timestamp >= :since_ts
            GROUP BY session_id;
            """
        ),
        {"since_ts": since_ts},
    )

    conn.execute(
        text(
            """
            INSERT INTO silver.session_products
                (session_id, product_id, first_seen, last_seen, views, clicks, add_to_cart, begin_checkout, purchases)
            SELECT
                session_id,
                product_id,
                MIN(event_timestamp) AS first_seen,
                MAX(event_timestamp) AS last_seen,
                SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS views,
                SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
                SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
                SUM(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
            FROM silver.session_events
            WHERE event_timestamp >= :since_ts
              AND product_id IS NOT NULL
              AND LTRIM(RTRIM(product_id)) <> ''
            GROUP BY session_id, product_id;
            """
        ),
        {"since_ts": since_ts},
    )

    return rows if rows > 0 else 0


def main() -> int:
    args = _parse_args()
    settings = load_settings()
    engine = get_engine(settings)

    recent_days = _env_int("ETL_RECENT_DAYS", 30)
    since_interactions = to_sqlserver_utc_naive(utc_now() - timedelta(days=min(7, recent_days)))

    with engine.begin() as conn:
        run = start_run(conn, "build_silver")
        try:
            web_rows_inserted = build_silver_web_events(conn)

            # Web/session silver tables (existing)
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
                        WHERE session_id IS NOT NULL AND LTRIM(RTRIM(session_id)) <> ''
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
                        WHERE session_id IS NOT NULL AND LTRIM(RTRIM(session_id)) <> ''
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
                    FROM bronze.page_view_events
                    WHERE session_id IS NOT NULL AND LTRIM(RTRIM(session_id)) <> '';
                    """))

            conn.execute(
                text(
                    "DELETE FROM silver.product_interactions WHERE event_timestamp >= :since;"
                ),
                {"since": since_interactions},
            )
            conn.execute(
                text(
                    """
                    ;WITH views AS (
                        SELECT
                            pv.event_timestamp,
                            pv.session_id,
                            pv.user_id,
                            COALESCE(
                                JSON_VALUE(pv.properties, '$.product_id'),
                                CASE
                                    WHEN pv.page_url LIKE '/products/%'
                                    THEN RIGHT(pv.page_url, CHARINDEX('/', REVERSE(pv.page_url)) - 1)
                                    ELSE NULL
                                END
                            ) AS product_id,
                            'view' AS interaction_type,
                            pv.properties
                    FROM bronze.page_view_events pv
                    WHERE pv.event_timestamp >= :since
                      AND pv.page_url LIKE '/products/%'
                      AND pv.session_id IS NOT NULL AND LTRIM(RTRIM(pv.session_id)) <> ''
                    ),
                    clicks AS (
                        SELECT
                            ce.event_timestamp,
                            ce.session_id,
                            ce.user_id,
                            COALESCE(
                                JSON_VALUE(ce.properties, '$.product_id'),
                                CASE
                                    WHEN ce.page_url LIKE '/products/%'
                                    THEN RIGHT(ce.page_url, CHARINDEX('/', REVERSE(ce.page_url)) - 1)
                                    ELSE NULL
                                END
                            ) AS product_id,
                            CASE
                                WHEN ce.element_id = 'btn_add_to_cart' THEN 'add_to_cart'
                                WHEN COALESCE(JSON_VALUE(ce.properties, '$.interaction_type'), '') = 'add_to_cart' THEN 'add_to_cart'
                                ELSE 'click'
                            END AS interaction_type,
                            ce.properties
                    FROM bronze.click_events ce
                    WHERE ce.event_timestamp >= :since
                      AND ce.page_url LIKE '/products/%'
                      AND ce.session_id IS NOT NULL AND LTRIM(RTRIM(ce.session_id)) <> ''
                    ),
                    src AS (
                        SELECT event_timestamp, session_id, user_id, product_id, interaction_type, properties FROM views
                        UNION ALL
                        SELECT event_timestamp, session_id, user_id, product_id, interaction_type, properties FROM clicks
                    )
                    INSERT INTO silver.product_interactions
                        (event_timestamp, session_id, user_id, product_id, interaction_type, properties)
                    SELECT DISTINCT
                        event_timestamp,
                        session_id,
                        user_id,
                        product_id,
                        interaction_type,
                        properties
                    FROM src
                    WHERE product_id IS NOT NULL;
                    """
                ),
                {"since": since_interactions},
            )

            # Business silver tables from bronze.business_events (recompute window)
            since = to_sqlserver_utc_naive(utc_now() - timedelta(days=int(args.days)))

            src_rows = conn.execute(
                text("""
                    SELECT
                        event_timestamp,
                        correlation_id,
                        service,
                        event_type,
                        user_id,
                        entity_id,
                        payload
                    FROM bronze.business_events
                    WHERE event_timestamp >= :since
                    ORDER BY event_timestamp ASC;
                """),
                {"since": since},
            ).mappings()

            orders: dict[str, dict[str, object]] = {}
            order_item_payloads: dict[str, tuple[datetime, object]] = {}
            payments: list[dict[str, object]] = []
            reviews: list[dict[str, object]] = []

            for r in src_rows:
                payload_raw = str(r.get("payload") or "")
                payload_obj = parse_json_payload(payload_raw)
                if payload_obj is None:
                    insert_dead_letter(
                        conn,
                        source=str(r.get("service") or "silver")[:50],
                        reason="silver_parse_failed",
                        payload={
                            "event_timestamp": str(r.get("event_timestamp")),
                            "correlation_id": r.get("correlation_id"),
                            "service": r.get("service"),
                            "event_type": r.get("event_type"),
                            "user_id": r.get("user_id"),
                            "entity_id": r.get("entity_id"),
                            "payload": payload_raw[:4000],
                        },
                    )
                    continue

                raw_event_type = str(r.get("event_type") or "")
                canon = canonical_event_type(raw_event_type)

                ts_db = r["event_timestamp"]
                ts = ensure_utc(ts_db)
                ts_naive = to_sqlserver_utc_naive(ts)

                correlation_id = r.get("correlation_id")
                service = r.get("service")
                user_id = r.get("user_id")
                entity_id = r.get("entity_id")

                # Orders
                if canon in {"order_created", "order_cancelled", "order_paid", "refund_created"}:
                    order_id = best_effort_order_id(
                        str(entity_id) if entity_id else None, payload_obj
                    )
                    if order_id:
                        state = orders.setdefault(
                            order_id,
                            {
                                "order_id": order_id,
                                "first_seen_at": ts_naive,
                                "created_at": None,
                                "updated_at": ts_naive,
                                "status": "created",
                                "status_ts": None,
                                "user_id": None,
                                "currency": None,
                                "total_amount": None,
                                "correlation_id": None,
                                "source_service": None,
                            },
                        )

                        state["first_seen_at"] = min(
                            state["first_seen_at"], ts_naive  # type: ignore[arg-type]
                        )
                        state["updated_at"] = max(
                            state["updated_at"], ts_naive  # type: ignore[arg-type]
                        )
                        if user_id:
                            state["user_id"] = str(user_id)[:64]
                        if correlation_id:
                            state["correlation_id"] = str(correlation_id)[:64]
                        if service:
                            state["source_service"] = str(service)[:64]

                        if canon == "order_created":
                            ca = state.get("created_at")
                            state["created_at"] = ts_naive if ca is None else min(ca, ts_naive)  # type: ignore[arg-type]

                        status_map = {
                            "order_created": "created",
                            "order_paid": "paid",
                            "order_cancelled": "cancelled",
                            "refund_created": "refunded",
                        }
                        new_status = status_map.get(canon)
                        if new_status:
                            prev_ts = state.get("status_ts")
                            if prev_ts is None or ts_naive >= prev_ts:  # type: ignore[operator]
                                state["status"] = new_status
                                state["status_ts"] = ts_naive

                        amt = best_effort_amount(payload_obj)
                        cur = best_effort_currency(payload_obj)
                        if amt is not None:
                            state["total_amount"] = amt
                        if cur is not None:
                            state["currency"] = cur

                        # Track latest payload containing items for this order
                        items = normalize_items(payload_obj)
                        if items:
                            prev = order_item_payloads.get(order_id)
                            if prev is None or ts_naive >= prev[0]:
                                order_item_payloads[order_id] = (ts_naive, payload_obj)

                # Payments (prefer explicit payment/refund events, or anything carrying payment_id)
                if canon in {"payment_failed", "refund_created"} or raw_event_type.lower().startswith("payment") or raw_event_type.lower().startswith("refund"):
                    payment_id = best_effort_payment_id(payload_obj)
                    order_id = best_effort_order_id(None, payload_obj)
                    occurred_at = ts_naive

                    if not payment_id:
                        seed = f"{correlation_id or ''}|{canon}|{ensure_utc(ts_db).isoformat()}|{order_id or ''}"
                        payment_id = deterministic_id(seed, max_len=64)

                    pay_status = "succeeded"
                    if canon == "payment_failed":
                        pay_status = "failed"
                    if canon == "refund_created":
                        pay_status = "refunded"

                    payments.append(
                        {
                            "payment_id": payment_id[:64],
                            "order_id": order_id[:64] if order_id else None,
                            "user_id": str(user_id)[:64] if user_id else None,
                            "status": pay_status,
                            "amount": best_effort_amount(payload_obj),
                            "currency": best_effort_currency(payload_obj),
                            "provider": best_effort_provider(payload_obj),
                            "occurred_at": occurred_at,
                            "correlation_id": str(correlation_id)[:64]
                            if correlation_id
                            else None,
                            "source_service": str(service)[:64] if service else None,
                        }
                    )

                # Reviews
                if canon == "review_created":
                    product_id = best_effort_product_id(payload_obj)
                    rating = best_effort_rating(payload_obj)
                    if not product_id or rating is None:
                        insert_dead_letter(
                            conn,
                            source=str(service or "silver")[:50],
                            reason="silver_review_missing_fields",
                            payload={
                                "event_timestamp": str(ts_db),
                                "event_type": raw_event_type,
                                "entity_id": entity_id,
                                "product_id": product_id,
                                "rating": rating,
                                "payload": payload_raw[:4000],
                            },
                        )
                        continue

                    review_id = best_effort_review_id(None, payload_obj)
                    if not review_id and entity_id and str(entity_id) != product_id:
                        review_id = str(entity_id)
                    if not review_id:
                        seed = f"{product_id}|{user_id or ''}|{rating}|{ensure_utc(ts_db).isoformat()}"
                        review_id = deterministic_id(seed, max_len=64)

                    reviews.append(
                        {
                            "review_id": review_id[:64],
                            "product_id": product_id[:64],
                            "user_id": str(user_id)[:64] if user_id else None,
                            "rating": int(rating),
                            "comment": best_effort_comment(payload_obj),
                            "created_at": ts_naive,
                            "correlation_id": str(correlation_id)[:64]
                            if correlation_id
                            else None,
                        }
                    )

            # Finalize orders with fallbacks
            order_rows: list[dict[str, object]] = []
            for oid, st in orders.items():
                created_at = st.get("created_at") or st.get("first_seen_at")
                updated_at = st.get("updated_at") or created_at
                order_rows.append(
                    {
                        "order_id": oid,
                        "user_id": st.get("user_id"),
                        "created_at": created_at,
                        "status": st.get("status") or "created",
                        "currency": st.get("currency"),
                        "total_amount": st.get("total_amount"),
                        "correlation_id": st.get("correlation_id"),
                        "source_service": st.get("source_service"),
                        "updated_at": updated_at,
                    }
                )

            rows_inserted = 0

            # Upsert orders
            if order_rows:
                for i in range(0, len(order_rows), 500):
                    batch = order_rows[i : i + 500]
                    ids = [str(x["order_id"]) for x in batch]
                    existing = _existing_ids(conn, "silver.orders", "order_id", ids)

                    inserts = [x for x in batch if str(x["order_id"]) not in existing]
                    updates = [x for x in batch if str(x["order_id"]) in existing]

                    if inserts:
                        res = conn.execute(
                            text(
                                """
                                INSERT INTO silver.orders
                                    (order_id, user_id, created_at, status, currency, total_amount, correlation_id, source_service, updated_at)
                                VALUES
                                    (:order_id, :user_id, :created_at, :status, :currency, :total_amount, :correlation_id, :source_service, :updated_at);
                                """
                            ),
                            inserts,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

                    if updates:
                        res = conn.execute(
                            text(
                                """
                                UPDATE silver.orders
                                SET
                                    user_id = COALESCE(:user_id, user_id),
                                    created_at = CASE WHEN created_at <= :created_at THEN created_at ELSE :created_at END,
                                    status = CASE WHEN updated_at <= :updated_at THEN :status ELSE status END,
                                    currency = CASE WHEN updated_at <= :updated_at THEN COALESCE(:currency, currency) ELSE currency END,
                                    total_amount = CASE WHEN updated_at <= :updated_at THEN COALESCE(:total_amount, total_amount) ELSE total_amount END,
                                    correlation_id = COALESCE(:correlation_id, correlation_id),
                                    source_service = COALESCE(:source_service, source_service),
                                    updated_at = CASE WHEN updated_at >= :updated_at THEN updated_at ELSE :updated_at END
                                WHERE order_id = :order_id;
                                """
                            ),
                            updates,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

            # Recompute order_items for touched orders
            if order_item_payloads:
                touched_orders = list(order_item_payloads.keys())
                deleted = 0
                for i in range(0, len(touched_orders), 500):
                    deleted += _delete_order_items(conn, touched_orders[i : i + 500])

                item_rows: list[dict[str, object]] = []
                for oid, (_ts, pobj) in order_item_payloads.items():
                    for it in normalize_items(pobj):
                        line_total = it.line_total
                        if line_total is None and it.unit_price is not None:
                            line_total = it.unit_price * it.quantity
                        item_rows.append(
                            {
                                "order_id": oid,
                                "product_id": it.product_id,
                                "quantity": int(it.quantity),
                                "unit_price": it.unit_price,
                                "line_total": line_total,
                            }
                        )

                if item_rows:
                    res = conn.execute(
                        text(
                            """
                            INSERT INTO silver.order_items (order_id, product_id, quantity, unit_price, line_total)
                            VALUES (:order_id, :product_id, :quantity, :unit_price, :line_total);
                            """
                        ),
                        item_rows,
                    )
                    rows_inserted += int(getattr(res, "rowcount", 0) or 0)
                rows_inserted += deleted

            # Upsert payments
            if payments:
                for i in range(0, len(payments), 500):
                    batch = payments[i : i + 500]
                    ids = [str(x["payment_id"]) for x in batch]
                    existing = _existing_ids(conn, "silver.payments", "payment_id", ids)

                    inserts = [x for x in batch if str(x["payment_id"]) not in existing]
                    updates = [x for x in batch if str(x["payment_id"]) in existing]

                    if inserts:
                        res = conn.execute(
                            text(
                                """
                                INSERT INTO silver.payments
                                    (payment_id, order_id, user_id, status, amount, currency, provider, occurred_at, correlation_id, source_service)
                                VALUES
                                    (:payment_id, :order_id, :user_id, :status, :amount, :currency, :provider, :occurred_at, :correlation_id, :source_service);
                                """
                            ),
                            inserts,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

                    if updates:
                        res = conn.execute(
                            text(
                                """
                                UPDATE silver.payments
                                SET
                                    order_id = COALESCE(:order_id, order_id),
                                    user_id = COALESCE(:user_id, user_id),
                                    status = CASE WHEN occurred_at <= :occurred_at THEN :status ELSE status END,
                                    amount = CASE WHEN occurred_at <= :occurred_at THEN COALESCE(:amount, amount) ELSE amount END,
                                    currency = CASE WHEN occurred_at <= :occurred_at THEN COALESCE(:currency, currency) ELSE currency END,
                                    provider = CASE WHEN occurred_at <= :occurred_at THEN COALESCE(:provider, provider) ELSE provider END,
                                    occurred_at = CASE WHEN occurred_at >= :occurred_at THEN occurred_at ELSE :occurred_at END,
                                    correlation_id = COALESCE(:correlation_id, correlation_id),
                                    source_service = COALESCE(:source_service, source_service)
                                WHERE payment_id = :payment_id;
                                """
                            ),
                            updates,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

            # Upsert reviews
            if reviews:
                for i in range(0, len(reviews), 500):
                    batch = reviews[i : i + 500]
                    ids = [str(x["review_id"]) for x in batch]
                    existing = _existing_ids(conn, "silver.reviews", "review_id", ids)

                    inserts = [x for x in batch if str(x["review_id"]) not in existing]
                    updates = [x for x in batch if str(x["review_id"]) in existing]

                    if inserts:
                        res = conn.execute(
                            text(
                                """
                                INSERT INTO silver.reviews
                                    (review_id, product_id, user_id, rating, comment, created_at, correlation_id)
                                VALUES
                                    (:review_id, :product_id, :user_id, :rating, :comment, :created_at, :correlation_id);
                                """
                            ),
                            inserts,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

                    if updates:
                        res = conn.execute(
                            text(
                                """
                                UPDATE silver.reviews
                                SET
                                    product_id = COALESCE(:product_id, product_id),
                                    user_id = COALESCE(:user_id, user_id),
                                    rating = CASE WHEN created_at <= :created_at THEN :rating ELSE rating END,
                                    comment = CASE WHEN created_at <= :created_at THEN COALESCE(:comment, comment) ELSE comment END,
                                    created_at = CASE WHEN created_at <= :created_at THEN created_at ELSE :created_at END,
                                    correlation_id = COALESCE(:correlation_id, correlation_id)
                                WHERE review_id = :review_id;
                                """
                            ),
                            updates,
                        )
                        rows_inserted += int(getattr(res, "rowcount", 0) or 0)

            # Purchases -> product_interactions (after orders + order_items are refreshed)
            res = conn.execute(
                text(
                    """
                    INSERT INTO silver.product_interactions
                        (event_timestamp, session_id, user_id, product_id, interaction_type, properties)
                    SELECT DISTINCT
                        o.updated_at AS event_timestamp,
                        COALESCE(o.correlation_id, o.order_id) AS session_id,
                        o.user_id,
                        oi.product_id,
                        'purchase' AS interaction_type,
                        CONCAT(
                            '{',
                                '\"order_id\":\"', REPLACE(COALESCE(o.order_id, ''), '\"', ''), '\",',
                                '\"source\":\"silver.orders\",',
                                '\"status\":\"', REPLACE(COALESCE(o.status, ''), '\"', ''), '\"',
                            '}'
                        ) AS properties
                    FROM silver.orders o
                    INNER JOIN silver.order_items oi ON o.order_id = oi.order_id
                    WHERE o.status = 'paid'
                      AND o.updated_at >= :since;
                    """
                ),
                {"since": since_interactions},
            )
            rows_inserted += int(getattr(res, "rowcount", 0) or 0)

            purchase_rows_inserted = build_silver_purchases(conn)
            session_rows_inserted = build_silver_session_facts(conn, days=args.days)

            finish_run(
                conn,
                run,
                rows_inserted=int(rows_inserted)
                + int(web_rows_inserted)
                + int(purchase_rows_inserted)
                + int(session_rows_inserted),
            )
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print(
        "Silver layer built: user_sessions, page_sequence, product_interactions, orders, order_items, payments, reviews."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
