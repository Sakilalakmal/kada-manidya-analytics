from __future__ import annotations

from datetime import timedelta

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run
from src.utils.time import utc_now


def _exec(conn, sql: str) -> None:
    conn.execute(text(sql))


def _exec_params(conn, sql: str, params: dict[str, object]) -> None:
    conn.execute(text(sql), params)


def _ensure_behavior_gold_tables(conn) -> None:
    conn.execute(
        text(
            """
            IF OBJECT_ID('gold.behavior_daily', 'U') IS NULL
            BEGIN
                CREATE TABLE gold.behavior_daily(
                    metric_date date NOT NULL CONSTRAINT PK_gold_behavior_daily PRIMARY KEY,
                    sessions int NOT NULL,
                    unique_users int NOT NULL,
                    page_views int NOT NULL,
                    clicks int NOT NULL,
                    add_to_cart int NOT NULL,
                    begin_checkout int NOT NULL,
                    purchases int NOT NULL
                );
            END

            IF OBJECT_ID('gold.funnel_daily', 'U') IS NULL
            BEGIN
                CREATE TABLE gold.funnel_daily(
                    metric_date date NOT NULL CONSTRAINT PK_gold_funnel_daily PRIMARY KEY,
                    view_sessions int NOT NULL,
                    cart_sessions int NOT NULL,
                    checkout_sessions int NOT NULL,
                    purchase_sessions int NOT NULL,
                    view_to_cart_rate float NOT NULL,
                    cart_to_checkout_rate float NOT NULL,
                    checkout_to_purchase_rate float NOT NULL,
                    view_to_purchase_rate float NOT NULL
                );
            END
            """
        )
    )


def _ensure_conversion_funnel_schema(conn) -> None:
    conn.execute(
        text(
            """
            IF COL_LENGTH('gold.conversion_funnel', 'drop_off_rate') IS NOT NULL
            BEGIN
                DECLARE @precision int;
                DECLARE @scale int;

                SELECT
                    @precision = c.[precision],
                    @scale = c.[scale]
                FROM sys.columns c
                INNER JOIN sys.objects o ON c.object_id = o.object_id
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = 'gold'
                  AND o.name = 'conversion_funnel'
                  AND c.name = 'drop_off_rate';

                IF @precision < 9 OR @scale < 6
                    ALTER TABLE gold.conversion_funnel
                    ALTER COLUMN drop_off_rate decimal(9,6) NULL;
            END
            """
        )
    )


def _safe_rowcount(result) -> int:
    rc = getattr(result, "rowcount", 0)
    try:
        n = int(rc or 0)
    except Exception:
        n = 0
    return n if n > 0 else 0


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)
    show_seed_data = bool(getattr(settings, "show_seed_data", False))

    with engine.begin() as conn:
        run = start_run(conn, "build_gold")
        try:
            since_date = (utc_now() - timedelta(days=30)).date()
            rows_inserted = 0

            _ensure_behavior_gold_tables(conn)
            _ensure_conversion_funnel_schema(conn)

            _exec_params(
                conn,
                """
                ;WITH seed_orders AS (
                    SELECT DISTINCT
                        COALESCE(
                            be.entity_id,
                            JSON_VALUE(be.payload, '$.order_id'),
                            JSON_VALUE(be.payload, '$.data.order_id'),
                            JSON_VALUE(be.payload, '$.order.order_id')
                        ) AS order_id
                    FROM bronze.business_events be
                    WHERE be.event_timestamp >= :since_date
                      AND be.payload LIKE '%seed_run_id%'
                ),
                pv AS (
                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        session_id,
                        page_url
                    FROM bronze.page_view_events
                    WHERE event_timestamp >= :since_date
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                ),
                checkout_events AS (
                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        session_id
                    FROM bronze.page_view_events
                    WHERE event_timestamp >= :since_date
                      AND page_url = '/checkout'
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')

                    UNION

                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        session_id
                    FROM bronze.click_events
                    WHERE event_timestamp >= :since_date
                      AND element_id = 'btn_checkout'
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                ),
                steps AS (
                    SELECT
                        funnel_date,
                        'visit_home' AS funnel_step,
                        1 AS step_order,
                        COUNT(DISTINCT session_id) AS users_count
                    FROM pv
                    WHERE page_url = '/'
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        funnel_date,
                        'visit_products' AS funnel_step,
                        2 AS step_order,
                        COUNT(DISTINCT session_id) AS users_count
                    FROM pv
                    WHERE page_url = '/products'
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        funnel_date,
                        'product_view' AS funnel_step,
                        3 AS step_order,
                        COUNT(DISTINCT session_id) AS users_count
                    FROM pv
                    WHERE page_url LIKE '/products/%'
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        'add_to_cart' AS funnel_step,
                        4 AS step_order,
                        COUNT(DISTINCT session_id) AS users_count
                    FROM silver.product_interactions
                    WHERE interaction_type = 'add_to_cart'
                      AND event_timestamp >= :since_date
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY CAST(event_timestamp AS date)

                    UNION ALL

                    SELECT
                        funnel_date,
                        'checkout_started' AS funnel_step,
                        5 AS step_order,
                        COUNT(DISTINCT session_id) AS users_count
                    FROM checkout_events
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        CAST(updated_at AS date) AS funnel_date,
                        'purchase' AS funnel_step,
                        6 AS step_order,
                        COUNT(DISTINCT COALESCE(correlation_id, order_id)) AS users_count
                    FROM silver.orders o
                    WHERE o.status = 'paid'
                      AND o.updated_at >= :since_date
                      AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = o.order_id))
                    GROUP BY CAST(updated_at AS date)
                ),
                with_prev AS (
                    SELECT
                        funnel_date,
                        funnel_step,
                        step_order,
                        users_count,
                        LAG(users_count) OVER (PARTITION BY funnel_date ORDER BY step_order) AS prev_users_count
                    FROM steps
                ),
                with_drop AS (
                    SELECT
                        funnel_date,
                        funnel_step,
                        step_order,
                        users_count,
                        CASE
                            WHEN prev_users_count IS NULL THEN NULL
                            WHEN prev_users_count = 0 THEN NULL
                            WHEN d.raw_drop_off_rate < 0 THEN CAST(0 AS decimal(9,6))
                            WHEN d.raw_drop_off_rate > 1 THEN CAST(1 AS decimal(9,6))
                            ELSE CAST(d.raw_drop_off_rate AS decimal(9,6))
                        END AS drop_off_rate
                    FROM with_prev
                    OUTER APPLY (
                        SELECT CAST(
                            (CAST(prev_users_count AS decimal(18,6)) - CAST(users_count AS decimal(18,6)))
                            / NULLIF(CAST(prev_users_count AS decimal(18,6)), 0)
                            AS decimal(18,6)
                        ) AS raw_drop_off_rate
                    ) d
                )
                MERGE gold.conversion_funnel AS tgt
                USING with_drop AS src
                ON tgt.funnel_date = src.funnel_date AND tgt.funnel_step = src.funnel_step
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.step_order = src.step_order,
                        tgt.users_count = src.users_count,
                        tgt.drop_off_rate = src.drop_off_rate
                WHEN NOT MATCHED THEN
                    INSERT (funnel_date, funnel_step, step_order, users_count, drop_off_rate)
                    VALUES (src.funnel_date, src.funnel_step, src.step_order, src.users_count, src.drop_off_rate);
                """,
                {"since_date": since_date, "show_seed_data": 1 if show_seed_data else 0},
            )

            rows_inserted += _safe_rowcount(
                conn.execute(
                    text("DELETE FROM gold.behavior_daily WHERE metric_date >= :since_date;"),
                    {"since_date": since_date},
                )
            )
            rows_inserted += _safe_rowcount(
                conn.execute(
                    text(
                        """
                        ;WITH seed_orders AS (
                            SELECT DISTINCT
                                COALESCE(
                                    be.entity_id,
                                    JSON_VALUE(be.payload, '$.order_id'),
                                    JSON_VALUE(be.payload, '$.data.order_id'),
                                    JSON_VALUE(be.payload, '$.order.order_id')
                                ) AS order_id
                            FROM bronze.business_events be
                            WHERE be.event_timestamp >= :since_date
                              AND be.payload LIKE '%seed_run_id%'
                        ),
                        we AS (
                            SELECT
                                CAST(event_timestamp AS date) AS metric_date,
                                COUNT(DISTINCT session_id) AS sessions,
                                COUNT(DISTINCT CASE WHEN user_id IS NULL THEN NULL ELSE user_id END) AS unique_users,
                                SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
                                SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
                                SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
                                SUM(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS begin_checkout
                            FROM silver.web_events
                            WHERE event_timestamp >= :since_date
                              AND (:show_seed_data = 1 OR COALESCE(properties_json, '') NOT LIKE '%seed_run_id%')
                            GROUP BY CAST(event_timestamp AS date)
                        ),
                        p AS (
                            SELECT
                                event_date AS metric_date,
                                COUNT(DISTINCT order_id) AS purchases
                            FROM silver.purchases
                            WHERE event_timestamp >= :since_date
                              AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = silver.purchases.order_id))
                            GROUP BY event_date
                        ),
                        merged AS (
                            SELECT
                                COALESCE(we.metric_date, p.metric_date) AS metric_date,
                                ISNULL(we.sessions, 0) AS sessions,
                                ISNULL(we.unique_users, 0) AS unique_users,
                                ISNULL(we.page_views, 0) AS page_views,
                                ISNULL(we.clicks, 0) AS clicks,
                                ISNULL(we.add_to_cart, 0) AS add_to_cart,
                                ISNULL(we.begin_checkout, 0) AS begin_checkout,
                                ISNULL(p.purchases, 0) AS purchases
                            FROM we
                            FULL OUTER JOIN p
                                ON we.metric_date = p.metric_date
                        )
                        INSERT INTO gold.behavior_daily
                            (metric_date, sessions, unique_users, page_views, clicks, add_to_cart, begin_checkout, purchases)
                        SELECT
                            metric_date, sessions, unique_users, page_views, clicks, add_to_cart, begin_checkout, purchases
                        FROM merged
                        WHERE metric_date >= :since_date;
                        """
                    ),
                    {"since_date": since_date, "show_seed_data": 1 if show_seed_data else 0},
                )
            )

            rows_inserted += _safe_rowcount(
                conn.execute(
                    text("DELETE FROM gold.funnel_daily WHERE metric_date >= :since_date;"),
                    {"since_date": since_date},
                )
            )
            rows_inserted += _safe_rowcount(
                conn.execute(
                    text(
                        """
                        ;WITH seed_orders AS (
                            SELECT DISTINCT
                                COALESCE(
                                    be.entity_id,
                                    JSON_VALUE(be.payload, '$.order_id'),
                                    JSON_VALUE(be.payload, '$.data.order_id'),
                                    JSON_VALUE(be.payload, '$.order.order_id')
                                ) AS order_id
                            FROM bronze.business_events be
                            WHERE be.event_timestamp >= :since_date
                              AND be.payload LIKE '%seed_run_id%'
                        ),
                        session_flags AS (
                            SELECT
                                CAST(event_timestamp AS date) AS metric_date,
                                session_id,
                                MAX(CASE WHEN event_type = 'page_view' AND product_id IS NOT NULL THEN 1 ELSE 0 END) AS has_view,
                                MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_cart,
                                MAX(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS has_checkout
                            FROM silver.web_events
                            WHERE event_timestamp >= :since_date
                              AND (:show_seed_data = 1 OR COALESCE(properties_json, '') NOT LIKE '%seed_run_id%')
                            GROUP BY CAST(event_timestamp AS date), session_id
                        ),
                        session_counts AS (
                            SELECT
                                metric_date,
                                COUNT(DISTINCT CASE WHEN has_view = 1 THEN session_id END) AS view_sessions,
                                COUNT(DISTINCT CASE WHEN has_cart = 1 THEN session_id END) AS cart_sessions,
                                COUNT(DISTINCT CASE WHEN has_checkout = 1 THEN session_id END) AS checkout_sessions
                            FROM session_flags
                            GROUP BY metric_date
                        ),
                        session_start AS (
                            SELECT
                                CAST(event_timestamp AS date) AS metric_date,
                                session_id,
                                MAX(user_id) AS user_id,
                                MIN(event_timestamp) AS session_start_ts
                            FROM silver.web_events
                            WHERE event_timestamp >= :since_date
                              AND (:show_seed_data = 1 OR COALESCE(properties_json, '') NOT LIKE '%seed_run_id%')
                            GROUP BY CAST(event_timestamp AS date), session_id
                        ),
                        purchase_base AS (
                            SELECT
                                event_timestamp,
                                event_date AS metric_date,
                                order_id,
                                user_id,
                                correlation_id
                            FROM silver.purchases
                            WHERE event_timestamp >= :since_date
                              AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = silver.purchases.order_id))
                        ),
                        purchase_mapped AS (
                            SELECT
                                p.metric_date,
                                p.order_id,
                                p.user_id,
                                COALESCE(sid.session_id, suser.session_id) AS mapped_session_id
                            FROM purchase_base p
                            OUTER APPLY (
                                SELECT TOP 1 s.session_id
                                FROM session_start s
                                WHERE s.metric_date = p.metric_date
                                  AND p.correlation_id IS NOT NULL
                                  AND s.session_id = p.correlation_id
                            ) sid
                            OUTER APPLY (
                                SELECT TOP 1 s.session_id
                                FROM session_start s
                                WHERE s.metric_date = p.metric_date
                                  AND sid.session_id IS NULL
                                  AND p.user_id IS NOT NULL
                                  AND s.user_id = p.user_id
                                ORDER BY ABS(DATEDIFF(SECOND, s.session_start_ts, p.event_timestamp))
                            ) suser
                        ),
                        purchase_session_counts AS (
                            SELECT
                                metric_date,
                                COUNT(DISTINCT mapped_session_id) AS purchase_sessions_assigned,
                                COUNT(DISTINCT CASE WHEN user_id IS NULL THEN NULL ELSE user_id END) AS purchase_users
                            FROM purchase_mapped
                            GROUP BY metric_date
                        ),
                        merged AS (
                            SELECT
                                COALESCE(sc.metric_date, psc.metric_date) AS metric_date,
                                ISNULL(sc.view_sessions, 0) AS view_sessions,
                                ISNULL(sc.cart_sessions, 0) AS cart_sessions,
                                ISNULL(sc.checkout_sessions, 0) AS checkout_sessions,
                                CASE
                                    WHEN ISNULL(psc.purchase_sessions_assigned, 0) > 0 THEN ISNULL(psc.purchase_sessions_assigned, 0)
                                    ELSE ISNULL(psc.purchase_users, 0)
                                END AS purchase_sessions
                            FROM session_counts sc
                            FULL OUTER JOIN purchase_session_counts psc
                                ON sc.metric_date = psc.metric_date
                        ),
                        with_rates AS (
                            SELECT
                                metric_date,
                                view_sessions,
                                cart_sessions,
                                checkout_sessions,
                                purchase_sessions,
                                CASE WHEN view_sessions = 0 THEN 0.0 ELSE CAST(cart_sessions AS float) / CAST(view_sessions AS float) END AS view_to_cart_rate,
                                CASE WHEN cart_sessions = 0 THEN 0.0 ELSE CAST(checkout_sessions AS float) / CAST(cart_sessions AS float) END AS cart_to_checkout_rate,
                                CASE WHEN checkout_sessions = 0 THEN 0.0 ELSE CAST(purchase_sessions AS float) / CAST(checkout_sessions AS float) END AS checkout_to_purchase_rate,
                                CASE WHEN view_sessions = 0 THEN 0.0 ELSE CAST(purchase_sessions AS float) / CAST(view_sessions AS float) END AS view_to_purchase_rate
                            FROM merged
                        )
                        INSERT INTO gold.funnel_daily
                            (metric_date, view_sessions, cart_sessions, checkout_sessions, purchase_sessions,
                             view_to_cart_rate, cart_to_checkout_rate, checkout_to_purchase_rate, view_to_purchase_rate)
                        SELECT
                            metric_date, view_sessions, cart_sessions, checkout_sessions, purchase_sessions,
                            view_to_cart_rate, cart_to_checkout_rate, checkout_to_purchase_rate, view_to_purchase_rate
                        FROM with_rates
                        WHERE metric_date >= :since_date;
                        """
                    ),
                    {"since_date": since_date, "show_seed_data": 1 if show_seed_data else 0},
                )
            )

            rows_inserted += _safe_rowcount(
                conn.execute(
                    text("DELETE FROM gold.product_metrics WHERE metric_date >= :since_date;"),
                    {"since_date": since_date},
                )
            )
            rows_inserted += _safe_rowcount(
                conn.execute(
                    text(
                        """
                ;WITH interactions AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        product_id,
                        SUM(CASE WHEN interaction_type = 'view' THEN 1 ELSE 0 END) AS views_count,
                        SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS clicks_count,
                        SUM(CASE WHEN interaction_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
                    FROM silver.product_interactions
                    WHERE event_timestamp >= :since_date
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY CAST(event_timestamp AS date), product_id
                ),
                seed_orders AS (
                    SELECT DISTINCT
                        COALESCE(
                            be.entity_id,
                            JSON_VALUE(be.payload, '$.order_id'),
                            JSON_VALUE(be.payload, '$.data.order_id'),
                            JSON_VALUE(be.payload, '$.order.order_id')
                        ) AS order_id
                    FROM bronze.business_events be
                    WHERE be.event_timestamp >= :since_date
                      AND be.payload LIKE '%seed_run_id%'
                ),
                purchases AS (
                    SELECT
                        CAST(o.updated_at AS date) AS metric_date,
                        oi.product_id,
                        SUM(oi.quantity) AS purchases_count,
                        SUM(COALESCE(oi.line_total, oi.unit_price * oi.quantity, 0)) AS revenue
                    FROM silver.order_items oi
                    INNER JOIN silver.orders o ON oi.order_id = o.order_id
                    WHERE o.status = 'paid' AND o.updated_at >= :since_date
                      AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = o.order_id))
                    GROUP BY CAST(o.updated_at AS date), oi.product_id
                ),
                reviews AS (
                    SELECT
                        CAST(created_at AS date) AS metric_date,
                        product_id,
                        COUNT(*) AS reviews_count,
                        AVG(CAST(rating AS decimal(3,2))) AS avg_rating
                    FROM silver.reviews
                    WHERE created_at >= :since_date
                    GROUP BY CAST(created_at AS date), product_id
                ),
                keys AS (
                    SELECT metric_date, product_id FROM interactions
                    UNION
                    SELECT metric_date, product_id FROM purchases
                    UNION
                    SELECT metric_date, product_id FROM reviews
                ),
                merged AS (
                    SELECT
                        k.metric_date,
                        k.product_id,
                        ISNULL(i.views_count, 0) AS views_count,
                        ISNULL(i.clicks_count, 0) AS clicks_count,
                        ISNULL(i.add_to_cart_count, 0) AS add_to_cart_count,
                        ISNULL(p.purchases_count, 0) AS purchases_count,
                        ISNULL(p.revenue, 0) AS revenue,
                        r.avg_rating,
                        ISNULL(r.reviews_count, 0) AS reviews_count,
                        CASE
                            WHEN ISNULL(i.views_count, 0) = 0 THEN NULL
                            ELSE
                                CASE
                                    WHEN (
                                        CAST(ISNULL(i.add_to_cart_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.views_count AS decimal(18,6)), 0)
                                    ) < 0 THEN CAST(0 AS decimal(9,6))
                                    WHEN (
                                        CAST(ISNULL(i.add_to_cart_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.views_count AS decimal(18,6)), 0)
                                    ) > 1 THEN CAST(1 AS decimal(9,6))
                                    ELSE CAST(
                                        CAST(ISNULL(i.add_to_cart_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.views_count AS decimal(18,6)), 0)
                                        AS decimal(9,6)
                                    )
                                END
                        END AS view_to_cart_rate,
                        CASE
                            WHEN ISNULL(i.add_to_cart_count, 0) = 0 THEN NULL
                            ELSE
                                CASE
                                    WHEN (
                                        CAST(ISNULL(p.purchases_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.add_to_cart_count AS decimal(18,6)), 0)
                                    ) < 0 THEN CAST(0 AS decimal(9,6))
                                    WHEN (
                                        CAST(ISNULL(p.purchases_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.add_to_cart_count AS decimal(18,6)), 0)
                                    ) > 1 THEN CAST(1 AS decimal(9,6))
                                    ELSE CAST(
                                        CAST(ISNULL(p.purchases_count, 0) AS decimal(18,6))
                                        / NULLIF(CAST(i.add_to_cart_count AS decimal(18,6)), 0)
                                        AS decimal(9,6)
                                    )
                                END
                        END AS cart_to_purchase_rate
                    FROM keys k
                    LEFT JOIN interactions i
                        ON k.metric_date = i.metric_date AND k.product_id = i.product_id
                    LEFT JOIN purchases p
                        ON k.metric_date = p.metric_date AND k.product_id = p.product_id
                    LEFT JOIN reviews r
                        ON k.metric_date = r.metric_date AND k.product_id = r.product_id
                    WHERE k.product_id IS NOT NULL
                )
                INSERT INTO gold.product_metrics (
                    product_id, metric_date, views_count, clicks_count, add_to_cart_count,
                    purchases_count, revenue, avg_rating, reviews_count, view_to_cart_rate, cart_to_purchase_rate
                )
                SELECT
                    product_id, metric_date, views_count, clicks_count, add_to_cart_count,
                    purchases_count, revenue, avg_rating, reviews_count, view_to_cart_rate, cart_to_purchase_rate
                FROM merged
                WHERE metric_date >= :since_date;
                            """
                    ),
                    {"since_date": since_date, "show_seed_data": 1 if show_seed_data else 0},
                )
            )

            rows_inserted += _safe_rowcount(
                conn.execute(
                    text("DELETE FROM gold.reviews_quality WHERE metric_date >= :since_date;"),
                    {"since_date": since_date},
                )
            )
            rows_inserted += _safe_rowcount(
                conn.execute(
                    text(
                        """
                ;WITH reviews AS (
                    SELECT
                        CAST(created_at AS date) AS metric_date,
                        product_id,
                        COUNT(*) AS total_reviews,
                        SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) AS five_star_reviews,
                        AVG(CAST(rating AS decimal(3,2))) AS avg_rating
                    FROM silver.reviews
                    WHERE created_at >= :since_date
                    GROUP BY CAST(created_at AS date), product_id
                )
                INSERT INTO gold.reviews_quality (metric_date, product_id, total_reviews, five_star_reviews, avg_rating)
                SELECT metric_date, product_id, total_reviews, five_star_reviews, avg_rating
                FROM reviews
                WHERE metric_date >= :since_date;
                            """
                    ),
                    {"since_date": since_date},
                )
            )

            rows_inserted += _safe_rowcount(
                conn.execute(
                    text("DELETE FROM gold.orders_payments_daily WHERE metric_date >= :since_date;"),
                    {"since_date": since_date},
                )
            )
            rows_inserted += _safe_rowcount(
                conn.execute(
                    text(
                        """
                ;WITH raw AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        entity_id,
                        LOWER(LTRIM(RTRIM(event_type))) AS raw_event_type,
                        payload
                    FROM bronze.business_events
                    WHERE event_timestamp >= :since_date
                      AND (:show_seed_data = 1 OR payload NOT LIKE '%seed_run_id%')
                ),
                normalized AS (
                    SELECT
                        metric_date,
                        entity_id,
                        CASE
                            WHEN raw_event_type IN ('order.created', 'order_created') THEN 'ORDER_CREATED'
                            WHEN raw_event_type IN ('order_paid', 'order.paid', 'payment.succeeded', 'payment.success', 'payment_success') THEN 'ORDER_PAID'
                            WHEN raw_event_type IN ('order.cancelled', 'order_cancelled') THEN 'ORDER_CANCELLED'
                            WHEN raw_event_type IN ('review_submitted', 'review.submitted') THEN 'REVIEW_SUBMITTED'
                            ELSE NULL
                        END AS normalized_type,
                        TRY_CONVERT(
                            decimal(12,2),
                            COALESCE(
                                JSON_VALUE(payload, '$.total_amount'),
                                JSON_VALUE(payload, '$.data.total_amount'),
                                JSON_VALUE(payload, '$.order.total_amount'),
                                JSON_VALUE(payload, '$.amount'),
                                JSON_VALUE(payload, '$.data.amount')
                            )
                        ) AS total_amount
                    FROM raw
                ),
                orders_by_day AS (
                    SELECT
                        metric_date,
                        COUNT(DISTINCT CASE WHEN normalized_type = 'ORDER_CREATED' THEN entity_id END) AS total_orders,
                        COUNT(DISTINCT CASE WHEN normalized_type = 'ORDER_PAID' THEN entity_id END) AS paid_orders,
                        COUNT(DISTINCT CASE WHEN normalized_type = 'ORDER_CANCELLED' THEN entity_id END) AS cancelled_orders,
                        SUM(CASE WHEN normalized_type = 'ORDER_PAID' THEN ISNULL(total_amount, 0) ELSE 0 END) AS total_revenue
                    FROM normalized
                    WHERE normalized_type IN ('ORDER_CREATED', 'ORDER_PAID', 'ORDER_CANCELLED')
                    GROUP BY metric_date
                ),
                merged AS (
                    SELECT
                        metric_date,
                        ISNULL(total_orders, 0) AS total_orders,
                        ISNULL(paid_orders, 0) AS paid_orders,
                        ISNULL(cancelled_orders, 0) AS cancelled_orders,
                        CASE
                            WHEN ISNULL(total_orders, 0) > 0
                                THEN CAST(ISNULL(paid_orders, 0) AS decimal(10,4)) / CAST(total_orders AS decimal(10,4))
                            ELSE CAST(0 AS decimal(10,4))
                        END AS payment_success_rate,
                        ISNULL(total_revenue, 0) AS total_revenue,
                        CAST(0 AS int) AS refunds_count
                    FROM orders_by_day
                )
                INSERT INTO gold.orders_payments_daily
                    (metric_date, total_orders, paid_orders, cancelled_orders, payment_success_rate, total_revenue, refunds_count)
                SELECT
                    metric_date, total_orders, paid_orders, cancelled_orders, payment_success_rate, total_revenue, refunds_count
                FROM merged
                WHERE metric_date >= :since_date;
                            """
                    ),
                    {"since_date": since_date, "show_seed_data": 1 if show_seed_data else 0},
                )
            )

            _exec_params(
                conn,
                """
                ;WITH pv AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        page_url,
                        COUNT(*) AS views,
                        COUNT(DISTINCT COALESCE(user_id, session_id)) AS unique_visitors,
                        AVG(TRY_CONVERT(decimal(10,2), JSON_VALUE(properties, '$.load_time_ms'))) AS avg_load_time_ms
                    FROM bronze.page_view_events
                    WHERE (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY CAST(event_timestamp AS date), page_url
                ),
                time_on_page AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        referrer_url AS page_url,
                        AVG(CAST(time_on_prev_page_seconds AS decimal(10,2))) AS avg_time_on_page_seconds
                    FROM bronze.page_view_events
                    WHERE referrer_url IS NOT NULL AND time_on_prev_page_seconds IS NOT NULL
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY CAST(event_timestamp AS date), referrer_url
                ),
                scroll AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        page_url,
                        AVG(scroll_depth_pct) AS avg_scroll_depth
                    FROM bronze.scroll_events
                    WHERE scroll_depth_pct IS NOT NULL
                      AND (:show_seed_data = 1 OR COALESCE(properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY CAST(event_timestamp AS date), page_url
                ),
                seed_sessions AS (
                    SELECT DISTINCT
                        CAST(event_timestamp AS date) AS metric_date,
                        session_id
                    FROM silver.web_events
                    WHERE COALESCE(properties_json, '') LIKE '%seed_run_id%'
                ),
                seq_max AS (
                    SELECT session_id, MAX(step_number) AS max_step
                    FROM silver.page_sequence
                    WHERE (:show_seed_data = 1 OR NOT EXISTS (
                        SELECT 1
                        FROM seed_sessions ss
                        WHERE ss.metric_date = CAST(silver.page_sequence.event_timestamp AS date)
                          AND ss.session_id = silver.page_sequence.session_id
                    ))
                    GROUP BY session_id
                ),
                entry AS (
                    SELECT
                        CAST(ps.event_timestamp AS date) AS metric_date,
                        ps.page_url,
                        COUNT(DISTINCT ps.session_id) AS entry_sessions,
                        SUM(CASE WHEN mx.max_step = 1 THEN 1 ELSE 0 END) AS bounced_sessions
                    FROM silver.page_sequence ps
                    INNER JOIN seq_max mx ON ps.session_id = mx.session_id
                    WHERE ps.step_number = 1
                      AND (:show_seed_data = 1 OR NOT EXISTS (
                          SELECT 1
                          FROM seed_sessions ss
                          WHERE ss.metric_date = CAST(ps.event_timestamp AS date)
                            AND ss.session_id = ps.session_id
                      ))
                    GROUP BY CAST(ps.event_timestamp AS date), ps.page_url
                ),
                exitp AS (
                    SELECT
                        CAST(ps.event_timestamp AS date) AS metric_date,
                        ps.page_url,
                        COUNT(DISTINCT ps.session_id) AS exit_sessions
                    FROM silver.page_sequence ps
                    INNER JOIN seq_max mx
                        ON ps.session_id = mx.session_id AND ps.step_number = mx.max_step
                    WHERE (:show_seed_data = 1 OR NOT EXISTS (
                        SELECT 1
                        FROM seed_sessions ss
                        WHERE ss.metric_date = CAST(ps.event_timestamp AS date)
                          AND ss.session_id = ps.session_id
                    ))
                    GROUP BY CAST(ps.event_timestamp AS date), ps.page_url
                ),
                sessions_viewed AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        page_url,
                        COUNT(DISTINCT session_id) AS sessions_viewed
                    FROM silver.page_sequence
                    WHERE (:show_seed_data = 1 OR NOT EXISTS (
                        SELECT 1
                        FROM seed_sessions ss
                        WHERE ss.metric_date = CAST(silver.page_sequence.event_timestamp AS date)
                          AND ss.session_id = silver.page_sequence.session_id
                    ))
                    GROUP BY CAST(event_timestamp AS date), page_url
                ),
                merged AS (
                    SELECT
                        pv.page_url,
                        pv.metric_date,
                        pv.views,
                        pv.unique_visitors,
                        tp.avg_time_on_page_seconds,
                        sc.avg_scroll_depth,
                        CASE
                            WHEN e.entry_sessions IS NULL OR e.entry_sessions = 0 THEN NULL
                            ELSE CAST(e.bounced_sessions AS decimal(10,4))
                                / CAST(e.entry_sessions AS decimal(10,4))
                        END AS bounce_rate,
                        CASE
                            WHEN sv.sessions_viewed IS NULL OR sv.sessions_viewed = 0 THEN NULL
                            ELSE CAST(ISNULL(x.exit_sessions, 0) AS decimal(10,4))
                                / CAST(sv.sessions_viewed AS decimal(10,4))
                        END AS exit_rate,
                        pv.avg_load_time_ms
                    FROM pv
                    LEFT JOIN time_on_page tp
                        ON pv.metric_date = tp.metric_date AND pv.page_url = tp.page_url
                    LEFT JOIN scroll sc
                        ON pv.metric_date = sc.metric_date AND pv.page_url = sc.page_url
                    LEFT JOIN entry e
                        ON pv.metric_date = e.metric_date AND pv.page_url = e.page_url
                    LEFT JOIN exitp x
                        ON pv.metric_date = x.metric_date AND pv.page_url = x.page_url
                    LEFT JOIN sessions_viewed sv
                        ON pv.metric_date = sv.metric_date AND pv.page_url = sv.page_url
                )
                MERGE gold.page_performance AS tgt
                USING merged AS src
                ON tgt.page_url = src.page_url AND tgt.metric_date = src.metric_date
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.views = src.views,
                        tgt.unique_visitors = src.unique_visitors,
                        tgt.avg_time_on_page_seconds = src.avg_time_on_page_seconds,
                        tgt.avg_scroll_depth = src.avg_scroll_depth,
                        tgt.bounce_rate = src.bounce_rate,
                        tgt.exit_rate = src.exit_rate,
                        tgt.avg_load_time_ms = TRY_CONVERT(int, src.avg_load_time_ms)
                WHEN NOT MATCHED THEN
                    INSERT (
                        page_url, metric_date, views, unique_visitors, avg_time_on_page_seconds,
                        avg_scroll_depth, bounce_rate, exit_rate, avg_load_time_ms
                    )
                    VALUES (
                        src.page_url, src.metric_date, src.views, src.unique_visitors, src.avg_time_on_page_seconds,
                        src.avg_scroll_depth, src.bounce_rate, src.exit_rate, TRY_CONVERT(int, src.avg_load_time_ms)
                    );
                """,
                {"show_seed_data": 1 if show_seed_data else 0},
            )

            _exec(
                conn,
                """
                ;WITH base AS (
                    SELECT
                        CAST([timestamp] AS date) AS metric_date,
                        service,
                        response_time_ms,
                        status_code
                    FROM bronze.api_request_logs
                ),
                w AS (
                    SELECT
                        metric_date,
                        service,
                        CAST(
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_ms)
                            OVER (PARTITION BY metric_date, service) AS int
                        ) AS p50_latency_ms,
                        CAST(
                            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms)
                            OVER (PARTITION BY metric_date, service) AS int
                        ) AS p95_latency_ms,
                        CAST(
                            SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)
                            OVER (PARTITION BY metric_date, service) AS decimal(10,4)
                        ) / NULLIF(
                            CAST(COUNT(*) OVER (PARTITION BY metric_date, service) AS decimal(10,4)), 0
                        ) AS error_rate
                    FROM base
                ),
                agg AS (
                    SELECT DISTINCT metric_date, service, p50_latency_ms, p95_latency_ms, error_rate
                    FROM w
                ),
                dlq AS (
                    SELECT
                        CAST(failed_at AS date) AS metric_date,
                        source AS service,
                        COUNT(*) AS dlq_count
                    FROM ops.dead_letter_events
                    GROUP BY CAST(failed_at AS date), source
                ),
                merged AS (
                    SELECT
                        a.metric_date,
                        a.service,
                        a.p50_latency_ms,
                        a.p95_latency_ms,
                        a.error_rate,
                        CAST(0 AS bigint) AS event_lag_ms,
                        0 AS retries_count,
                        ISNULL(d.dlq_count, 0) AS dlq_count
                    FROM agg a
                    LEFT JOIN dlq d
                        ON a.metric_date = d.metric_date AND a.service = d.service
                )
                MERGE gold.system_health_daily AS tgt
                USING merged AS src
                ON tgt.metric_date = src.metric_date AND tgt.service = src.service
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.p50_latency_ms = src.p50_latency_ms,
                        tgt.p95_latency_ms = src.p95_latency_ms,
                        tgt.error_rate = src.error_rate,
                        tgt.event_lag_ms = src.event_lag_ms,
                        tgt.retries_count = src.retries_count,
                        tgt.dlq_count = src.dlq_count
                WHEN NOT MATCHED THEN
                    INSERT (
                        metric_date, service, p50_latency_ms, p95_latency_ms,
                        error_rate, event_lag_ms, retries_count, dlq_count
                    )
                    VALUES (
                        src.metric_date, src.service, src.p50_latency_ms, src.p95_latency_ms,
                        src.error_rate, src.event_lag_ms, src.retries_count, src.dlq_count
                    );
                """,
            )

            _exec_params(
                conn,
                """
                ;WITH n AS (
                    SELECT
                        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, SYSDATETIME()), 0) AS metric_timestamp,
                        DATEADD(MINUTE, -5, SYSDATETIME()) AS since_5m,
                        CAST(SYSDATETIME() AS date) AS today
                ),
                seed_orders AS (
                    SELECT DISTINCT
                        COALESCE(
                            be.entity_id,
                            JSON_VALUE(be.payload, '$.order_id'),
                            JSON_VALUE(be.payload, '$.data.order_id'),
                            JSON_VALUE(be.payload, '$.order.order_id')
                        ) AS order_id
                    FROM bronze.business_events be
                    WHERE be.event_timestamp >= DATEADD(day, -7, SYSDATETIME())
                      AND be.payload LIKE '%seed_run_id%'
                ),
                active AS (
                    SELECT COUNT(DISTINCT COALESCE(pv.user_id, pv.session_id)) AS active_users_now
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE pv.event_timestamp >= n.since_5m
                      AND (:show_seed_data = 1 OR COALESCE(pv.properties, '') NOT LIKE '%seed_run_id%')
                ),
                sessions_today AS (
                    SELECT COUNT(DISTINCT pv.session_id) AS sessions_today
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE CAST(pv.event_timestamp AS date) = n.today
                      AND (:show_seed_data = 1 OR COALESCE(pv.properties, '') NOT LIKE '%seed_run_id%')
                ),
                orders_today AS (
                    SELECT
                        COUNT(*) AS orders_today,
                        SUM(ISNULL(total_amount, 0)) AS revenue_today
                    FROM silver.orders o
                    CROSS JOIN n
                    WHERE o.status = 'paid' AND CAST(o.updated_at AS date) = n.today
                      AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = o.order_id))
                ),
                top_product AS (
                    SELECT TOP 1
                        oi.product_id AS top_product_id
                    FROM silver.order_items oi
                    INNER JOIN silver.orders o ON oi.order_id = o.order_id
                    CROSS JOIN n
                    WHERE o.status = 'paid' AND CAST(o.updated_at AS date) = n.today
                      AND (:show_seed_data = 1 OR NOT EXISTS (SELECT 1 FROM seed_orders so WHERE so.order_id = o.order_id))
                    GROUP BY oi.product_id
                    ORDER BY SUM(COALESCE(oi.line_total, oi.unit_price * oi.quantity, 0)) DESC
                ),
                top_page AS (
                    SELECT TOP 1 page_url AS top_page_url
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE CAST(pv.event_timestamp AS date) = n.today
                      AND (:show_seed_data = 1 OR COALESCE(pv.properties, '') NOT LIKE '%seed_run_id%')
                    GROUP BY page_url
                    ORDER BY COUNT(*) DESC
                ),
                api_5m AS (
                    SELECT
                        AVG(CAST(response_time_ms AS decimal(10,2))) AS avg_latency_ms,
                        CAST(
                            100.0 * SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
                            AS decimal(5,2)
                        ) AS error_rate_percent
                    FROM bronze.api_request_logs l
                    CROSS JOIN n
                    WHERE l.[timestamp] >= n.since_5m
                ),
                src AS (
                    SELECT
                        n.metric_timestamp,
                        a.active_users_now,
                        s.sessions_today,
                        ISNULL(o.revenue_today, 0) AS revenue_today,
                        ISNULL(o.orders_today, 0) AS orders_today,
                        (SELECT TOP 1 top_product_id FROM top_product) AS top_product_id,
                        (SELECT TOP 1 top_page_url FROM top_page) AS top_page_url,
                        TRY_CONVERT(int, api.avg_latency_ms) AS avg_latency_ms,
                        api.error_rate_percent
                    FROM n
                    CROSS JOIN active a
                    CROSS JOIN sessions_today s
                    CROSS JOIN orders_today o
                    CROSS JOIN api_5m api
                )
                MERGE gold.realtime_metrics AS tgt
                USING src
                ON tgt.metric_timestamp = src.metric_timestamp
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.active_users_now = src.active_users_now,
                        tgt.sessions_today = src.sessions_today,
                        tgt.revenue_today = src.revenue_today,
                        tgt.orders_today = src.orders_today,
                        tgt.top_product_id = src.top_product_id,
                        tgt.top_page_url = src.top_page_url,
                        tgt.avg_latency_ms = src.avg_latency_ms,
                        tgt.error_rate_percent = src.error_rate_percent
                WHEN NOT MATCHED THEN
                    INSERT (
                        metric_timestamp, active_users_now, sessions_today, revenue_today, orders_today,
                        top_product_id, top_page_url, avg_latency_ms, error_rate_percent
                    )
                    VALUES (
                        src.metric_timestamp, src.active_users_now, src.sessions_today, src.revenue_today, src.orders_today,
                        src.top_product_id, src.top_page_url, src.avg_latency_ms, src.error_rate_percent
                    );
                """,
                {"show_seed_data": 1 if show_seed_data else 0},
            )

            finish_run(conn, run, rows_inserted=rows_inserted)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print("Gold layer built.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
