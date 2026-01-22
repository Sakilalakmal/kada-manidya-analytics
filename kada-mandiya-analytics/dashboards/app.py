from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import load_settings  # noqa: E402
from src.db.engine import get_engine as build_engine  # noqa: E402

st.set_page_config(page_title="Kada Mandiya Analytics", layout="wide")


@st.cache_resource
def get_settings():
    return load_settings()


@st.cache_resource
def get_engine():
    return build_engine(get_settings())


@st.cache_data(ttl=60, show_spinner=False)
def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_engine(), params=params)


def _safe_df(
    sql: str, params: dict | None = None, *, show_error: bool = True
) -> pd.DataFrame:
    try:
        return query_df(sql, params=params)
    except Exception as exc:
        if show_error:
            st.error(f"{exc}\n\nSQL:\n{sql}")
        return pd.DataFrame()


def _latest_date(table: str, date_col: str = "metric_date") -> str | None:
    df = _safe_df(
        f"""
        SELECT TOP 1 CAST({date_col} AS date) AS d
        FROM {table}
        WHERE {date_col} IS NOT NULL
        ORDER BY {date_col} DESC;
        """,
        show_error=False,
    )
    return None if df.empty else str(df.loc[0, "d"])


def _fmt_lkr(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        v = float(value)
    except Exception:
        return "N/A"
    return f"LKR {v:,.2f}"


def _fmt_int(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        return f"{int(value):,}"
    except Exception:
        return "N/A"


def _fmt_pct(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        v = float(value)
    except Exception:
        return "N/A"
    if v > 1.0:
        v = v / 100.0
    return f"{v * 100:.1f}%"


def executive():
    st.header("Executive")

    kpis = _safe_df(
        """
        SELECT
            CAST(GETDATE() AS date) AS metric_date,
            COUNT(1) AS rows_count,
            SUM(CAST(total_revenue AS float)) AS total_revenue,
            SUM(CAST(total_orders AS float)) AS total_orders,
            SUM(CAST(cancelled_orders AS float)) AS cancelled_orders,
            CASE
                WHEN SUM(CAST(total_orders AS float)) = 0 THEN NULL
                ELSE SUM(CAST(cancelled_orders AS float)) / NULLIF(SUM(CAST(total_orders AS float)), 0)
            END AS cancel_rate,
            CASE
                WHEN COUNT(payment_success_rate) = 0 THEN NULL
                ELSE AVG(CAST(payment_success_rate AS float))
            END AS payment_success_rate
        FROM gold.orders_payments_daily
        WHERE CAST(metric_date AS date) = CAST(GETDATE() AS date);
        """
    )

    if kpis.empty:
        st.warning("No gold.orders_payments_daily data yet. Run the ETL first.")
        return

    r = kpis.iloc[0].to_dict()
    rows_count = int(r.get("rows_count") or 0)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue Today", _fmt_lkr(r.get("total_revenue")))
    c2.metric("Orders Today", _fmt_int(r.get("total_orders")))
    c3.metric("Cancel Rate", _fmt_pct(r.get("cancel_rate")))
    c4.metric("Payment Success Rate", _fmt_pct(r.get("payment_success_rate")))
    if rows_count == 0:
        st.info("No data available yet.")
        latest = _latest_date("gold.orders_payments_daily", "metric_date")
        if latest:
            st.caption(f"Latest available date: {latest}")

    trend = _safe_df(
        """
        SELECT TOP 14
            CAST(metric_date AS date) AS metric_date,
            SUM(CAST(total_revenue AS float)) AS total_revenue
        FROM gold.orders_payments_daily
        WHERE CAST(metric_date AS date) >= DATEADD(day, -13, CAST(GETDATE() AS date))
        GROUP BY CAST(metric_date AS date)
        ORDER BY metric_date DESC;
        """
    )
    st.subheader("Revenue Trend (Last 14 Days)")
    if trend.empty:
        st.info("No data available yet.")
        latest = _latest_date("gold.orders_payments_daily", "metric_date")
        if latest:
            st.caption(f"Latest available date: {latest}")
        return
    trend = trend.sort_values("metric_date")
    trend["metric_date"] = pd.to_datetime(trend["metric_date"])
    st.line_chart(trend.set_index("metric_date")["total_revenue"])

    st.subheader("Funnel (Latest Day)")
    funnel_date = _latest_date("gold.funnel_daily", "metric_date")
    if not funnel_date:
        st.caption("gold.funnel_daily not available (or empty).")
        return

    df = _safe_df(
        """
        SELECT TOP 1
            CAST(metric_date AS date) AS metric_date,
            view_sessions,
            cart_sessions,
            checkout_sessions,
            purchase_sessions,
            view_to_cart_rate,
            cart_to_checkout_rate,
            checkout_to_purchase_rate,
            view_to_purchase_rate
        FROM gold.funnel_daily
        WHERE CAST(metric_date AS date) = CAST(:d AS date);
        """,
        {"d": funnel_date},
        show_error=False,
    )
    if df.empty:
        st.caption("No funnel rows for latest day yet.")
        return

    r = df.iloc[0].to_dict()
    st.caption(f"Latest funnel date: {funnel_date}")
    steps = pd.DataFrame(
        {
            "step": ["view", "add_to_cart", "begin_checkout", "purchase"],
            "sessions": [
                int(r.get("view_sessions") or 0),
                int(r.get("cart_sessions") or 0),
                int(r.get("checkout_sessions") or 0),
                int(r.get("purchase_sessions") or 0),
            ],
        }
    )
    st.bar_chart(steps.set_index("step")["sessions"])
    st.dataframe(df, use_container_width=True, hide_index=True)


def product():
    st.header("Product")
    st.caption("Top products are computed from the last 30 days of gold metrics.")

    top_rev = _safe_df(
        """
        SELECT TOP 5
            product_id,
            SUM(CAST(revenue AS float)) AS revenue
        FROM gold.product_metrics
        WHERE CAST(metric_date AS date) >= DATEADD(day, -29, CAST(GETDATE() AS date))
        GROUP BY product_id
        ORDER BY revenue DESC;
        """
    )
    top_purchases = _safe_df(
        """
        SELECT TOP 5
            product_id,
            SUM(CAST(purchases_count AS float)) AS purchases_count
        FROM gold.product_metrics
        WHERE CAST(metric_date AS date) >= DATEADD(day, -29, CAST(GETDATE() AS date))
        GROUP BY product_id
        ORDER BY purchases_count DESC;
        """
    )

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top 5 Products by Revenue (30d)")
        if top_rev.empty:
            st.info("No data available yet.")
            latest = _latest_date("gold.product_metrics", "metric_date")
            if latest:
                st.caption(f"Latest available date: {latest}")
        else:
            top_rev_display = top_rev.copy()
            top_rev_display["revenue"] = top_rev_display["revenue"].map(_fmt_lkr)
            st.dataframe(top_rev_display, use_container_width=True, hide_index=True)
            chart = top_rev.set_index("product_id")["revenue"]
            st.bar_chart(chart)
    with c2:
        st.subheader("Top 5 Products by Purchases (30d)")
        if top_purchases.empty:
            st.info("No data available yet.")
            latest = _latest_date("gold.product_metrics", "metric_date")
            if latest:
                st.caption(f"Latest available date: {latest}")
        else:
            top_purchases_display = top_purchases.copy()
            top_purchases_display["purchases_count"] = top_purchases_display[
                "purchases_count"
            ].map(_fmt_int)
            st.dataframe(
                top_purchases_display, use_container_width=True, hide_index=True
            )
            chart = top_purchases.set_index("product_id")["purchases_count"]
            st.bar_chart(chart)

    st.subheader("Avg Rating + Reviews per Product (30d)")
    reviews = _safe_df(
        """
        SELECT
            product_id,
            AVG(CAST(avg_rating AS float)) AS avg_rating_30d,
            SUM(CAST(total_reviews AS int)) AS total_reviews_30d,
            SUM(CAST(five_star_reviews AS int)) AS five_star_reviews_30d
        FROM gold.reviews_quality
        WHERE CAST(metric_date AS date) >= DATEADD(day, -29, CAST(GETDATE() AS date))
        GROUP BY product_id
        ORDER BY total_reviews_30d DESC;
        """
    )
    if reviews.empty:
        st.info("No data available yet.")
        latest = _latest_date("gold.reviews_quality", "metric_date")
        if latest:
            st.caption(f"Latest available date: {latest}")
    else:
        reviews_display = reviews.copy()
        reviews_display["avg_rating_30d"] = reviews_display["avg_rating_30d"].map(
            lambda v: "N/A" if pd.isna(v) else f"{float(v):.2f}"
        )
        reviews_display["total_reviews_30d"] = reviews_display["total_reviews_30d"].map(
            _fmt_int
        )
        reviews_display["five_star_reviews_30d"] = reviews_display[
            "five_star_reviews_30d"
        ].map(_fmt_int)
        st.dataframe(reviews_display, use_container_width=True, hide_index=True)

        top10 = reviews.head(10).copy()
        if not top10.empty:
            st.bar_chart(top10.set_index("product_id")["total_reviews_30d"])


def ops():
    st.header("Ops")
    completed_runs = _safe_df(
        """
        SELECT TOP 10
            run_type,
            status,
            started_at,
            finished_at,
            DATEDIFF(second, started_at, finished_at) AS duration_seconds,
            rows_inserted,
            error_message
        FROM ops.etl_runs
        WHERE finished_at IS NOT NULL
        ORDER BY finished_at DESC;
        """
    )
    running_runs = _safe_df(
        """
        SELECT TOP 10
            run_type,
            status,
            started_at,
            finished_at,
            DATEDIFF(second, started_at, finished_at) AS duration_seconds,
            rows_inserted,
            error_message
        FROM ops.etl_runs
        WHERE finished_at IS NULL
        ORDER BY started_at DESC;
        """
    )

    runs = pd.concat([running_runs, completed_runs], ignore_index=True).head(10)
    st.subheader("Recent ETL Runs (Last 10)")
    if runs.empty:
        st.info("No data available yet.")
        latest = _latest_date("ops.etl_runs", "finished_at")
        if latest:
            st.caption(f"Latest available date: {latest}")
    else:
        runs_display = runs.copy()

        def _normalize_status(status: str | None, finished_at) -> str:
            if pd.isna(finished_at):
                return "running"
            v = (status or "").strip().lower()
            if v in {"success", "succeeded", "ok", "passed", "completed"}:
                return "success"
            if v in {"failed", "fail", "error"}:
                return "failed"
            return v or "unknown"

        runs_display["status_normalized"] = runs_display.apply(
            lambda row: _normalize_status(row.get("status"), row.get("finished_at")),
            axis=1,
        )
        runs_display["status_indicator"] = runs_display["status_normalized"].map(
            lambda v: str(v).upper()
        )
        runs_display["duration"] = runs_display.apply(
            lambda row: "running"
            if pd.isna(row.get("finished_at"))
            else ("N/A" if pd.isna(row.get("duration_seconds")) else f"{int(row.get('duration_seconds'))}s"),
            axis=1,
        )

        cols = [
            "run_type",
            "status_indicator",
            "started_at",
            "finished_at",
            "duration",
            "rows_inserted",
            "error_message",
        ]
        runs_display = runs_display[cols]

        def _style_status(col: pd.Series) -> list[str]:
            styles: list[str] = []
            for v in col.astype(str):
                val = v.lower()
                if "success" in val:
                    styles.append("color: #15803d; font-weight: 600;")
                elif "failed" in val:
                    styles.append("color: #b91c1c; font-weight: 600;")
                elif "running" in val:
                    styles.append("color: #a16207; font-weight: 600;")
                else:
                    styles.append("")
            return styles

        st.dataframe(
            runs_display.style.apply(_style_status, subset=["status_indicator"]),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("System Health (If Available)")
    health = _safe_df(
        """
        SELECT TOP 14
            CAST(metric_date AS date) AS metric_date,
            AVG(CAST(p95_latency_ms AS float)) AS p95_latency_ms,
            AVG(CAST(error_rate AS float)) AS error_rate
        FROM gold.system_health_daily
        WHERE CAST(metric_date AS date) >= DATEADD(day, -13, CAST(GETDATE() AS date))
        GROUP BY CAST(metric_date AS date)
        ORDER BY metric_date DESC;
        """
        ,
        show_error=False,
    )
    if health.empty:
        st.caption("gold.system_health_daily not available (or empty).")
    else:
        health = health.sort_values("metric_date")
        health["metric_date"] = pd.to_datetime(health["metric_date"])
        c1, c2 = st.columns(2)
        with c1:
            st.caption("P95 latency (ms) - last 14 days")
            st.line_chart(health.set_index("metric_date")["p95_latency_ms"])
        with c2:
            st.caption("Error rate - last 14 days")
            st.line_chart(health.set_index("metric_date")["error_rate"])


def behavior():
    st.header("Behavior")
    window_days = st.selectbox("Window", [14, 30], index=0)

    today = _safe_df(
        """
        SELECT TOP 1
            CAST(metric_date AS date) AS metric_date,
            sessions,
            page_views,
            add_to_cart,
            purchases
        FROM gold.behavior_daily
        WHERE CAST(metric_date AS date) = CAST(GETDATE() AS date);
        """,
        show_error=False,
    )
    funnel_today = _safe_df(
        """
        SELECT TOP 1
            CAST(metric_date AS date) AS metric_date,
            view_sessions,
            cart_sessions,
            checkout_sessions,
            purchase_sessions,
            view_to_purchase_rate
        FROM gold.funnel_daily
        WHERE CAST(metric_date AS date) = CAST(GETDATE() AS date);
        """,
        show_error=False,
    )

    sessions = 0 if today.empty else int(today.loc[0, "sessions"] or 0)
    page_views = 0 if today.empty else int(today.loc[0, "page_views"] or 0)
    add_to_cart = 0 if today.empty else int(today.loc[0, "add_to_cart"] or 0)
    purchases = 0 if today.empty else int(today.loc[0, "purchases"] or 0)
    view_to_purchase = None if funnel_today.empty else funnel_today.loc[0, "view_to_purchase_rate"]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sessions Today", _fmt_int(sessions))
    c2.metric("Page Views Today", _fmt_int(page_views))
    c3.metric("Add-to-cart Today", _fmt_int(add_to_cart))
    c4.metric("Purchases Today", _fmt_int(purchases))
    c5.metric("View->Purchase %", _fmt_pct(view_to_purchase))

    series = _safe_df(
        f"""
        SELECT TOP {int(window_days)}
            CAST(metric_date AS date) AS metric_date,
            sessions,
            page_views,
            purchases
        FROM gold.behavior_daily
        WHERE CAST(metric_date AS date) >= DATEADD(day, -{int(window_days) - 1}, CAST(GETDATE() AS date))
        ORDER BY metric_date DESC;
        """,
        show_error=False,
    )
    st.subheader(f"Last {window_days} Days")
    if series.empty:
        st.info("No behavior data available yet. Run the silver+gold ETL to populate gold.behavior_daily.")
    else:
        series = series.sort_values("metric_date")
        series["metric_date"] = pd.to_datetime(series["metric_date"])
        st.line_chart(series.set_index("metric_date")[["sessions", "page_views", "purchases"]])

    st.subheader("Funnel (Latest Day)")
    funnel_date = _latest_date("gold.funnel_daily", "metric_date")
    if not funnel_date:
        st.caption("gold.funnel_daily not available (or empty).")
        return

    funnel = _safe_df(
        """
        SELECT TOP 1
            CAST(metric_date AS date) AS metric_date,
            view_sessions,
            cart_sessions,
            checkout_sessions,
            purchase_sessions,
            view_to_cart_rate,
            cart_to_checkout_rate,
            checkout_to_purchase_rate,
            view_to_purchase_rate
        FROM gold.funnel_daily
        WHERE CAST(metric_date AS date) = CAST(:d AS date);
        """,
        {"d": funnel_date},
        show_error=False,
    )
    if funnel.empty:
        st.caption("No funnel rows for latest day yet.")
        return

    st.caption(f"Latest funnel date: {funnel_date}")
    st.dataframe(funnel, use_container_width=True, hide_index=True)

    r = funnel.iloc[0].to_dict()
    steps = pd.DataFrame(
        {
            "step": ["view", "add_to_cart", "begin_checkout", "purchase"],
            "sessions": [
                int(r.get("view_sessions") or 0),
                int(r.get("cart_sessions") or 0),
                int(r.get("checkout_sessions") or 0),
                int(r.get("purchase_sessions") or 0),
            ],
        }
    )
    st.bar_chart(steps.set_index("step")["sessions"])


PAGES = {
    "Executive": executive,
    "Behavior": behavior,
    "Product": product,
    "Ops": ops,
}

try:
    st.sidebar.caption(
        f"Seed data: {'ON' if bool(get_settings().show_seed_data) else 'OFF'} (SHOW_SEED_DATA)"
    )
except Exception:
    pass

choice = st.sidebar.radio("Navigation", list(PAGES.keys()))
PAGES[choice]()
