from __future__ import annotations

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run


def _exec(conn, sql: str) -> None:
    conn.execute(text(sql))


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.begin() as conn:
        run = start_run(conn, "build_gold")
        try:
            _exec(
                conn,
                """
                ;WITH base AS (
                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        COALESCE(user_id, session_id) AS visitor_id,
                        page_url
                    FROM bronze.page_view_events
                ),
                steps AS (
                    SELECT
                        funnel_date,
                        'visit' AS funnel_step,
                        1 AS step_order,
                        COUNT(DISTINCT visitor_id) AS users_count
                    FROM base
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        funnel_date,
                        'product_view' AS funnel_step,
                        2 AS step_order,
                        COUNT(DISTINCT visitor_id) AS users_count
                    FROM base
                    WHERE page_url LIKE '/products/%'
                    GROUP BY funnel_date

                    UNION ALL

                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        'add_to_cart' AS funnel_step,
                        3 AS step_order,
                        COUNT(DISTINCT COALESCE(user_id, session_id)) AS users_count
                    FROM silver.product_interactions
                    WHERE interaction_type = 'add_to_cart'
                    GROUP BY CAST(event_timestamp AS date)

                    UNION ALL

                    SELECT
                        CAST(event_timestamp AS date) AS funnel_date,
                        'purchase' AS funnel_step,
                        4 AS step_order,
                        COUNT(DISTINCT COALESCE(user_id, correlation_id)) AS users_count
                    FROM bronze.business_events
                    WHERE event_type = 'order_paid'
                    GROUP BY CAST(event_timestamp AS date)
                ),
                with_drop AS (
                    SELECT
                        funnel_date,
                        funnel_step,
                        step_order,
                        users_count,
                        CASE
                            WHEN LAG(users_count) OVER (PARTITION BY funnel_date ORDER BY step_order) IS NULL THEN NULL
                            WHEN LAG(users_count) OVER (PARTITION BY funnel_date ORDER BY step_order) = 0 THEN NULL
                            ELSE CAST(
                                1 - CAST(users_count AS decimal(10,4)) /
                                    CAST(LAG(users_count) OVER (PARTITION BY funnel_date ORDER BY step_order) AS decimal(10,4))
                                AS decimal(5,4)
                            )
                        END AS drop_off_rate
                    FROM steps
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
            )

            _exec(
                conn,
                """
                ;WITH interactions AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        product_id,
                        SUM(CASE WHEN interaction_type = 'view' THEN 1 ELSE 0 END) AS views_count,
                        SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS clicks_count,
                        SUM(CASE WHEN interaction_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count
                    FROM silver.product_interactions
                    GROUP BY CAST(event_timestamp AS date), product_id
                ),
                purchases AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        JSON_VALUE(payload, '$.product_id') AS product_id,
                        COUNT(*) AS purchases_count,
                        SUM(TRY_CONVERT(decimal(12,2), JSON_VALUE(payload, '$.revenue'))) AS revenue
                    FROM bronze.business_events
                    WHERE event_type = 'order_paid'
                    GROUP BY CAST(event_timestamp AS date), JSON_VALUE(payload, '$.product_id')
                ),
                reviews AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        JSON_VALUE(payload, '$.product_id') AS product_id,
                        COUNT(*) AS reviews_count,
                        AVG(TRY_CONVERT(decimal(3,2), JSON_VALUE(payload, '$.rating'))) AS avg_rating
                    FROM bronze.business_events
                    WHERE event_type = 'review_submitted'
                    GROUP BY CAST(event_timestamp AS date), JSON_VALUE(payload, '$.product_id')
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
                            ELSE CAST(ISNULL(i.add_to_cart_count, 0) AS decimal(10,4))
                                / CAST(i.views_count AS decimal(10,4))
                        END AS view_to_cart_rate,
                        CASE
                            WHEN ISNULL(i.add_to_cart_count, 0) = 0 THEN NULL
                            ELSE CAST(ISNULL(p.purchases_count, 0) AS decimal(10,4))
                                / CAST(i.add_to_cart_count AS decimal(10,4))
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
                MERGE gold.product_metrics AS tgt
                USING merged AS src
                ON tgt.product_id = src.product_id AND tgt.metric_date = src.metric_date
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.views_count = src.views_count,
                        tgt.clicks_count = src.clicks_count,
                        tgt.add_to_cart_count = src.add_to_cart_count,
                        tgt.purchases_count = src.purchases_count,
                        tgt.revenue = src.revenue,
                        tgt.avg_rating = src.avg_rating,
                        tgt.reviews_count = src.reviews_count,
                        tgt.view_to_cart_rate = src.view_to_cart_rate,
                        tgt.cart_to_purchase_rate = src.cart_to_purchase_rate
                WHEN NOT MATCHED THEN
                    INSERT (
                        product_id, metric_date, views_count, clicks_count, add_to_cart_count,
                        purchases_count, revenue, avg_rating, reviews_count, view_to_cart_rate, cart_to_purchase_rate
                    )
                    VALUES (
                        src.product_id, src.metric_date, src.views_count, src.clicks_count, src.add_to_cart_count,
                        src.purchases_count, src.revenue, src.avg_rating, src.reviews_count,
                        src.view_to_cart_rate, src.cart_to_purchase_rate
                    );
                """,
            )

            _exec(
                conn,
                """
                ;WITH reviews AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        JSON_VALUE(payload, '$.product_id') AS product_id,
                        COUNT(*) AS total_reviews,
                        SUM(CASE WHEN TRY_CONVERT(int, JSON_VALUE(payload, '$.rating')) = 5 THEN 1 ELSE 0 END)
                            AS five_star_reviews,
                        AVG(TRY_CONVERT(decimal(3,2), JSON_VALUE(payload, '$.rating'))) AS avg_rating
                    FROM bronze.business_events
                    WHERE event_type = 'review_submitted'
                    GROUP BY CAST(event_timestamp AS date), JSON_VALUE(payload, '$.product_id')
                )
                MERGE gold.reviews_quality AS tgt
                USING reviews AS src
                ON tgt.metric_date = src.metric_date AND tgt.product_id = src.product_id
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.total_reviews = src.total_reviews,
                        tgt.five_star_reviews = src.five_star_reviews,
                        tgt.avg_rating = src.avg_rating
                WHEN NOT MATCHED THEN
                    INSERT (metric_date, product_id, total_reviews, five_star_reviews, avg_rating)
                    VALUES (src.metric_date, src.product_id, src.total_reviews, src.five_star_reviews, src.avg_rating);
                """,
            )

            _exec(
                conn,
                """
                ;WITH created AS (
                    SELECT CAST(event_timestamp AS date) AS metric_date,
                           COUNT(DISTINCT entity_id) AS total_orders
                    FROM bronze.business_events
                    WHERE event_type = 'order_created'
                    GROUP BY CAST(event_timestamp AS date)
                ),
                paid AS (
                    SELECT CAST(event_timestamp AS date) AS metric_date,
                           COUNT(DISTINCT entity_id) AS paid_orders,
                           SUM(TRY_CONVERT(decimal(12,2), JSON_VALUE(payload, '$.revenue'))) AS total_revenue
                    FROM bronze.business_events
                    WHERE event_type = 'order_paid'
                    GROUP BY CAST(event_timestamp AS date)
                ),
                cancelled AS (
                    SELECT CAST(event_timestamp AS date) AS metric_date,
                           COUNT(DISTINCT entity_id) AS cancelled_orders
                    FROM bronze.business_events
                    WHERE event_type = 'order_cancelled'
                    GROUP BY CAST(event_timestamp AS date)
                ),
                refunds AS (
                    SELECT CAST(event_timestamp AS date) AS metric_date,
                           COUNT(*) AS refunds_count
                    FROM bronze.business_events
                    WHERE event_type = 'refund_issued'
                    GROUP BY CAST(event_timestamp AS date)
                ),
                keys AS (
                    SELECT metric_date FROM created
                    UNION SELECT metric_date FROM paid
                    UNION SELECT metric_date FROM cancelled
                    UNION SELECT metric_date FROM refunds
                ),
                merged AS (
                    SELECT
                        k.metric_date,
                        ISNULL(c.total_orders, 0) AS total_orders,
                        ISNULL(p.paid_orders, 0) AS paid_orders,
                        ISNULL(x.cancelled_orders, 0) AS cancelled_orders,
                        CASE
                            WHEN ISNULL(c.total_orders, 0) = 0 THEN NULL
                            ELSE CAST(ISNULL(p.paid_orders, 0) AS decimal(10,4))
                                / CAST(c.total_orders AS decimal(10,4))
                        END AS payment_success_rate,
                        ISNULL(p.total_revenue, 0) AS total_revenue,
                        ISNULL(r.refunds_count, 0) AS refunds_count
                    FROM keys k
                    LEFT JOIN created c ON k.metric_date = c.metric_date
                    LEFT JOIN paid p ON k.metric_date = p.metric_date
                    LEFT JOIN cancelled x ON k.metric_date = x.metric_date
                    LEFT JOIN refunds r ON k.metric_date = r.metric_date
                )
                MERGE gold.orders_payments_daily AS tgt
                USING merged AS src
                ON tgt.metric_date = src.metric_date
                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.total_orders = src.total_orders,
                        tgt.paid_orders = src.paid_orders,
                        tgt.cancelled_orders = src.cancelled_orders,
                        tgt.payment_success_rate = src.payment_success_rate,
                        tgt.total_revenue = src.total_revenue,
                        tgt.refunds_count = src.refunds_count
                WHEN NOT MATCHED THEN
                    INSERT (
                        metric_date, total_orders, paid_orders, cancelled_orders,
                        payment_success_rate, total_revenue, refunds_count
                    )
                    VALUES (
                        src.metric_date, src.total_orders, src.paid_orders, src.cancelled_orders,
                        src.payment_success_rate, src.total_revenue, src.refunds_count
                    );
                """,
            )

            _exec(
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
                    GROUP BY CAST(event_timestamp AS date), page_url
                ),
                time_on_page AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        referrer_url AS page_url,
                        AVG(CAST(time_on_prev_page_seconds AS decimal(10,2))) AS avg_time_on_page_seconds
                    FROM bronze.page_view_events
                    WHERE referrer_url IS NOT NULL AND time_on_prev_page_seconds IS NOT NULL
                    GROUP BY CAST(event_timestamp AS date), referrer_url
                ),
                scroll AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        page_url,
                        AVG(scroll_depth_pct) AS avg_scroll_depth
                    FROM bronze.scroll_events
                    WHERE scroll_depth_pct IS NOT NULL
                    GROUP BY CAST(event_timestamp AS date), page_url
                ),
                seq_max AS (
                    SELECT session_id, MAX(step_number) AS max_step
                    FROM silver.page_sequence
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
                    GROUP BY CAST(ps.event_timestamp AS date), ps.page_url
                ),
                sessions_viewed AS (
                    SELECT
                        CAST(event_timestamp AS date) AS metric_date,
                        page_url,
                        COUNT(DISTINCT session_id) AS sessions_viewed
                    FROM silver.page_sequence
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

            _exec(
                conn,
                """
                ;WITH n AS (
                    SELECT
                        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, SYSDATETIME()), 0) AS metric_timestamp,
                        DATEADD(MINUTE, -5, SYSDATETIME()) AS since_5m,
                        CAST(SYSDATETIME() AS date) AS today
                ),
                active AS (
                    SELECT COUNT(DISTINCT COALESCE(pv.user_id, pv.session_id)) AS active_users_now
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE pv.event_timestamp >= n.since_5m
                ),
                sessions_today AS (
                    SELECT COUNT(DISTINCT pv.session_id) AS sessions_today
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE CAST(pv.event_timestamp AS date) = n.today
                ),
                orders_today AS (
                    SELECT
                        COUNT(DISTINCT entity_id) AS orders_today,
                        SUM(TRY_CONVERT(decimal(12,2), JSON_VALUE(payload, '$.revenue'))) AS revenue_today
                    FROM bronze.business_events be
                    CROSS JOIN n
                    WHERE be.event_type = 'order_paid' AND CAST(be.event_timestamp AS date) = n.today
                ),
                top_product AS (
                    SELECT TOP 1
                        JSON_VALUE(payload, '$.product_id') AS top_product_id
                    FROM bronze.business_events be
                    CROSS JOIN n
                    WHERE be.event_type = 'order_paid' AND CAST(be.event_timestamp AS date) = n.today
                    GROUP BY JSON_VALUE(payload, '$.product_id')
                    ORDER BY SUM(TRY_CONVERT(decimal(12,2), JSON_VALUE(payload, '$.revenue'))) DESC
                ),
                top_page AS (
                    SELECT TOP 1 page_url AS top_page_url
                    FROM bronze.page_view_events pv
                    CROSS JOIN n
                    WHERE CAST(pv.event_timestamp AS date) = n.today
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
            )

            finish_run(conn, run, rows_inserted=0)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print("Gold layer built.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
