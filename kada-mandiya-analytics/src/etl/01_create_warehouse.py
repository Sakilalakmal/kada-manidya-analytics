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

    schema_statements: list[str] = []
    ops_statements: list[str] = []
    statements: list[str] = []

    # Schemas
    for schema in ["bronze", "silver", "gold", "ops"]:
        schema_statements.append(f"""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
                EXEC('CREATE SCHEMA {schema}');
            """)

    # OPS tables
    ops_statements.append("""
        IF OBJECT_ID('ops.etl_runs', 'U') IS NULL
        BEGIN
            CREATE TABLE ops.etl_runs(
                run_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_ops_etl_runs_run_id DEFAULT NEWID()
                    CONSTRAINT PK_ops_etl_runs PRIMARY KEY,
                run_type varchar(50) NOT NULL,
                started_at datetime2 NOT NULL,
                finished_at datetime2 NULL,
                status varchar(20) NOT NULL,
                rows_inserted int NOT NULL CONSTRAINT DF_ops_etl_runs_rows DEFAULT 0,
                error_message nvarchar(max) NULL
            );
        END
        """)
    ops_statements.append("""
        IF OBJECT_ID('ops.dq_checks', 'U') IS NULL
        BEGIN
            CREATE TABLE ops.dq_checks(
                check_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_ops_dq_checks_check_id DEFAULT NEWID()
                    CONSTRAINT PK_ops_dq_checks PRIMARY KEY,
                check_time datetime2 NOT NULL CONSTRAINT DF_ops_dq_checks_time DEFAULT SYSDATETIME(),
                check_name varchar(100) NOT NULL,
                status varchar(20) NOT NULL,
                details nvarchar(max) NULL
            );
        END
        """)
    ops_statements.append("""
        IF OBJECT_ID('ops.dead_letter_events', 'U') IS NULL
        BEGIN
            CREATE TABLE ops.dead_letter_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_ops_dead_letter_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_ops_dead_letter_events PRIMARY KEY,
                failed_at datetime2 NOT NULL CONSTRAINT DF_ops_dead_letter_events_failed_at DEFAULT SYSDATETIME(),
                source varchar(50) NOT NULL,
                reason nvarchar(500) NOT NULL,
                payload nvarchar(max) NOT NULL
            );
        END
        """)

    # BRONZE tables
    statements.append("""
        IF OBJECT_ID('bronze.click_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.click_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_click_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_click_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                page_url varchar(2000) NOT NULL,
                element_id varchar(255) NULL,
                x int NULL,
                y int NULL,
                viewport_w int NULL,
                viewport_h int NULL,
                user_agent varchar(500) NULL,
                ip_address varchar(64) NULL,
                properties nvarchar(max) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.page_view_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.page_view_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_page_view_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_page_view_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                page_url varchar(2000) NOT NULL,
                referrer_url varchar(2000) NULL,
                utm_source varchar(100) NULL,
                utm_medium varchar(100) NULL,
                utm_campaign varchar(100) NULL,
                time_on_prev_page_seconds int NULL,
                properties nvarchar(max) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.scroll_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.scroll_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_scroll_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_scroll_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                page_url varchar(2000) NOT NULL,
                scroll_depth_pct decimal(5,2) NULL,
                properties nvarchar(max) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.form_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.form_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_form_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_form_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                page_url varchar(2000) NOT NULL,
                form_id varchar(255) NULL,
                field_id varchar(255) NULL,
                action varchar(64) NULL,
                error_message nvarchar(500) NULL,
                time_spent_ms int NULL,
                properties nvarchar(max) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.search_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.search_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_search_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_search_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                page_url varchar(2000) NOT NULL,
                query nvarchar(500) NULL,
                results_count int NULL,
                filters nvarchar(max) NULL,
                properties nvarchar(max) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.business_events', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.business_events(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_business_events_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_business_events PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                correlation_id varchar(64) NULL,
                service varchar(64) NOT NULL,
                event_type varchar(128) NOT NULL,
                user_id varchar(64) NULL,
                entity_id varchar(64) NULL,
                payload nvarchar(max) NOT NULL
            );
        END
        """)

    # SILVER business canonical tables
    statements.append("""
        IF OBJECT_ID('silver.orders', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.orders(
                order_id varchar(64) NOT NULL
                    CONSTRAINT PK_silver_orders PRIMARY KEY,
                user_id varchar(64) NULL,
                created_at datetime2 NOT NULL,
                status varchar(32) NOT NULL,
                currency varchar(10) NULL,
                total_amount decimal(12,2) NULL,
                correlation_id varchar(64) NULL,
                source_service varchar(64) NULL,
                updated_at datetime2 NOT NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('silver.order_items', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.order_items(
                order_item_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_silver_order_items_order_item_id DEFAULT NEWID()
                    CONSTRAINT PK_silver_order_items PRIMARY KEY,
                order_id varchar(64) NOT NULL,
                product_id varchar(64) NOT NULL,
                quantity int NOT NULL,
                unit_price decimal(12,2) NULL,
                line_total decimal(12,2) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('silver.payments', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.payments(
                payment_id varchar(64) NOT NULL
                    CONSTRAINT PK_silver_payments PRIMARY KEY,
                order_id varchar(64) NULL,
                user_id varchar(64) NULL,
                status varchar(32) NOT NULL,
                amount decimal(12,2) NULL,
                currency varchar(10) NULL,
                provider varchar(50) NULL,
                occurred_at datetime2 NOT NULL,
                correlation_id varchar(64) NULL,
                source_service varchar(64) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('silver.reviews', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.reviews(
                review_id varchar(64) NOT NULL
                    CONSTRAINT PK_silver_reviews PRIMARY KEY,
                product_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                rating int NOT NULL,
                comment nvarchar(1000) NULL,
                created_at datetime2 NOT NULL,
                correlation_id varchar(64) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.api_request_logs', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.api_request_logs(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_api_request_logs_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_api_request_logs PRIMARY KEY,
                [timestamp] datetime2 NOT NULL,
                service varchar(64) NOT NULL,
                endpoint varchar(500) NOT NULL,
                method varchar(16) NOT NULL,
                status_code int NOT NULL,
                response_time_ms int NOT NULL,
                user_id varchar(64) NULL,
                correlation_id varchar(64) NULL,
                request_size_bytes int NULL,
                response_size_bytes int NULL,
                ip_address varchar(64) NULL,
                user_agent varchar(500) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('bronze.db_query_perf', 'U') IS NULL
        BEGIN
            CREATE TABLE bronze.db_query_perf(
                event_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_bronze_db_query_perf_event_id DEFAULT NEWID()
                    CONSTRAINT PK_bronze_db_query_perf PRIMARY KEY,
                [timestamp] datetime2 NOT NULL,
                service varchar(64) NOT NULL,
                database_name varchar(128) NULL,
                query_type varchar(64) NULL,
                table_name varchar(128) NULL,
                execution_time_ms int NOT NULL,
                rows_affected int NULL,
                query_hash varchar(64) NULL
            );
        END
        """)

    # SILVER tables
    statements.append("""
        IF OBJECT_ID('silver.user_sessions', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.user_sessions(
                session_id varchar(64) NOT NULL CONSTRAINT PK_silver_user_sessions PRIMARY KEY,
                user_id varchar(64) NULL,
                start_time datetime2 NOT NULL,
                end_time datetime2 NOT NULL,
                duration_seconds int NOT NULL,
                page_views int NOT NULL,
                clicks int NOT NULL,
                entry_page varchar(2000) NULL,
                exit_page varchar(2000) NULL,
                utm_source varchar(100) NULL,
                utm_medium varchar(100) NULL,
                utm_campaign varchar(100) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('silver.page_sequence', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.page_sequence(
                session_id varchar(64) NOT NULL,
                step_number int NOT NULL,
                page_url varchar(2000) NOT NULL,
                event_timestamp datetime2 NOT NULL,
                CONSTRAINT PK_silver_page_sequence PRIMARY KEY (session_id, step_number)
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('silver.product_interactions', 'U') IS NULL
        BEGIN
            CREATE TABLE silver.product_interactions(
                interaction_id uniqueidentifier NOT NULL
                    CONSTRAINT DF_silver_product_interactions_interaction_id DEFAULT NEWID()
                    CONSTRAINT PK_silver_product_interactions PRIMARY KEY,
                event_timestamp datetime2 NOT NULL,
                session_id varchar(64) NOT NULL,
                user_id varchar(64) NULL,
                product_id varchar(64) NOT NULL,
                interaction_type varchar(32) NOT NULL,
                properties nvarchar(max) NULL
            );
        END
        """)

    # GOLD tables
    statements.append("""
        IF OBJECT_ID('gold.realtime_metrics', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.realtime_metrics(
                metric_timestamp datetime2 NOT NULL CONSTRAINT PK_gold_realtime_metrics PRIMARY KEY,
                active_users_now int NOT NULL,
                sessions_today int NOT NULL,
                revenue_today decimal(12,2) NOT NULL,
                orders_today int NOT NULL,
                top_product_id varchar(64) NULL,
                top_page_url varchar(2000) NULL,
                avg_latency_ms int NULL,
                error_rate_percent decimal(5,2) NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.conversion_funnel', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.conversion_funnel(
                funnel_date date NOT NULL,
                funnel_step varchar(50) NOT NULL,
                step_order int NOT NULL,
                users_count int NOT NULL,
                drop_off_rate decimal(9,6) NULL,
                CONSTRAINT PK_gold_conversion_funnel PRIMARY KEY (funnel_date, funnel_step)
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.product_metrics', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.product_metrics(
                product_id varchar(64) NOT NULL,
                metric_date date NOT NULL,
                views_count int NOT NULL,
                clicks_count int NOT NULL,
                add_to_cart_count int NOT NULL,
                purchases_count int NOT NULL,
                revenue decimal(12,2) NOT NULL,
                avg_rating decimal(3,2) NULL,
                reviews_count int NOT NULL,
                view_to_cart_rate decimal(5,4) NULL,
                cart_to_purchase_rate decimal(5,4) NULL,
                CONSTRAINT PK_gold_product_metrics PRIMARY KEY (product_id, metric_date)
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.page_performance', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.page_performance(
                page_url varchar(2000) NOT NULL,
                metric_date date NOT NULL,
                views int NOT NULL,
                unique_visitors int NOT NULL,
                avg_time_on_page_seconds decimal(10,2) NULL,
                avg_scroll_depth decimal(5,2) NULL,
                bounce_rate decimal(5,4) NULL,
                exit_rate decimal(5,4) NULL,
                avg_load_time_ms int NULL,
                CONSTRAINT PK_gold_page_performance PRIMARY KEY (page_url, metric_date)
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.reviews_quality', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.reviews_quality(
                metric_date date NOT NULL,
                product_id varchar(64) NOT NULL,
                total_reviews int NOT NULL,
                five_star_reviews int NOT NULL,
                avg_rating decimal(3,2) NULL,
                CONSTRAINT PK_gold_reviews_quality PRIMARY KEY (metric_date, product_id)
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.orders_payments_daily', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.orders_payments_daily(
                metric_date date NOT NULL CONSTRAINT PK_gold_orders_payments_daily PRIMARY KEY,
                total_orders int NOT NULL,
                paid_orders int NOT NULL,
                cancelled_orders int NOT NULL,
                payment_success_rate decimal(5,4) NULL,
                total_revenue decimal(12,2) NOT NULL,
                refunds_count int NOT NULL
            );
        END
        """)
    statements.append("""
        IF OBJECT_ID('gold.system_health_daily', 'U') IS NULL
        BEGIN
            CREATE TABLE gold.system_health_daily(
                metric_date date NOT NULL,
                service varchar(64) NOT NULL,
                p50_latency_ms int NULL,
                p95_latency_ms int NULL,
                error_rate decimal(5,4) NULL,
                event_lag_ms bigint NULL,
                retries_count int NOT NULL,
                dlq_count int NOT NULL,
                CONSTRAINT PK_gold_system_health_daily PRIMARY KEY (metric_date, service)
            );
        END
        """)

    # Indexes (idempotent)
    def ix(table: str, ix_name: str, cols: str, where: str | None = None) -> None:
        w = f" WHERE {where}" if where else ""
        statements.append(f"""
            IF NOT EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = '{ix_name}' AND object_id = OBJECT_ID('{table}')
            )
                CREATE INDEX {ix_name} ON {table} ({cols}){w};
            """)

    for table in [
        "bronze.click_events",
        "bronze.page_view_events",
        "bronze.scroll_events",
        "bronze.form_events",
        "bronze.search_events",
    ]:
        ix(table, f"IX_{table.replace('.', '_')}_event_timestamp", "event_timestamp")
        ix(table, f"IX_{table.replace('.', '_')}_session_id", "session_id")
        ix(
            table,
            f"IX_{table.replace('.', '_')}_user_id",
            "user_id",
            "user_id IS NOT NULL",
        )

    ix(
        "bronze.business_events",
        "IX_bronze_business_events_event_timestamp",
        "event_timestamp",
    )
    ix("bronze.business_events", "IX_bronze_business_events_event_type", "event_type")
    ix("bronze.business_events", "IX_bronze_business_events_service", "service")
    ix(
        "bronze.business_events",
        "IX_bronze_business_events_service_type_ts",
        "service, event_type, event_timestamp",
    )

    ix("silver.orders", "IX_silver_orders_status_created_at", "status, created_at")
    ix("silver.order_items", "IX_silver_order_items_order_id", "order_id")
    ix("silver.order_items", "IX_silver_order_items_product_id", "product_id")
    ix("silver.payments", "IX_silver_payments_status_occurred_at", "status, occurred_at")
    ix("silver.reviews", "IX_silver_reviews_product_id_created_at", "product_id, created_at")

    ix("bronze.api_request_logs", "IX_bronze_api_request_logs_timestamp", "[timestamp]")
    ix(
        "bronze.api_request_logs",
        "IX_bronze_api_request_logs_service_endpoint",
        "service, endpoint",
    )
    ix(
        "bronze.api_request_logs",
        "IX_bronze_api_request_logs_status_code",
        "status_code",
    )

    ix("bronze.db_query_perf", "IX_bronze_db_query_perf_timestamp", "[timestamp]")
    ix("bronze.db_query_perf", "IX_bronze_db_query_perf_service", "service")

    with engine.begin() as conn:
        for sql in schema_statements:
            _exec(conn, sql)
        for sql in ops_statements:
            _exec(conn, sql)

        run = start_run(conn, "create_warehouse")
        try:
            for sql in statements:
                _exec(conn, sql)
            finish_run(conn, run, rows_inserted=0)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print("Warehouse ready: schemas + tables + indexes created (if missing).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
