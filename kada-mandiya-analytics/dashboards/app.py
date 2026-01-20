from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config import load_settings  # noqa: E402
from src.db.engine import get_engine  # noqa: E402

st.set_page_config(page_title="Kada Mandiya Analytics", layout="wide")


@st.cache_resource
def _engine():
    settings = load_settings()
    return get_engine(settings)


@st.cache_data(ttl=60)
def _df(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _engine())


def _latest_date(table: str, date_col: str = "metric_date") -> str | None:
    q = f"SELECT TOP 1 CAST({date_col} AS date) AS d FROM {table} ORDER BY {date_col} DESC;"
    df = _df(q)
    if df.empty:
        return None
    return str(df.loc[0, "d"])


def executive():
    st.header("Executive")
    rt = _df(
        "SELECT TOP 1 * FROM gold.realtime_metrics ORDER BY metric_timestamp DESC;"
    )
    if rt.empty:
        st.warning("No realtime metrics yet. Run the ETL first.")
        return

    r = rt.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active Users (last 5m)", int(r["active_users_now"]))
    c2.metric("Sessions Today", int(r["sessions_today"]))
    c3.metric("Revenue Today", float(r["revenue_today"]))
    c4.metric("Orders Today", int(r["orders_today"]))

    trend = _df("""
        SELECT TOP 30 metric_date, total_revenue
        FROM gold.orders_payments_daily
        ORDER BY metric_date DESC;
        """).sort_values("metric_date")
    if not trend.empty:
        fig = px.line(
            trend,
            x="metric_date",
            y="total_revenue",
            title="Revenue Trend (Last 30 Days)",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No orders_payments_daily data yet.")

    latest = _latest_date("gold.conversion_funnel", "funnel_date")
    if latest:
        funnel = _df(f"""
            SELECT funnel_step, step_order, users_count, drop_off_rate
            FROM gold.conversion_funnel
            WHERE funnel_date = '{latest}'
            ORDER BY step_order;
            """)
        st.subheader(f"Conversion Funnel ({latest})")
        st.dataframe(funnel, use_container_width=True, hide_index=True)
    else:
        st.info("No conversion_funnel data yet.")


def product():
    st.header("Product")
    latest = _latest_date("gold.product_metrics")
    if not latest:
        st.warning("No product metrics yet. Run the ETL first.")
        return

    top = _df(f"""
        SELECT TOP 10 product_id, revenue, purchases_count, add_to_cart_count, views_count
        FROM gold.product_metrics
        WHERE metric_date = '{latest}'
        ORDER BY revenue DESC;
        """)
    c1, c2 = st.columns([2, 3])
    with c1:
        st.subheader(f"Top Products by Revenue ({latest})")
        st.dataframe(top, use_container_width=True, hide_index=True)
    with c2:
        if not top.empty:
            fig = px.bar(
                top, x="product_id", y="revenue", title="Top Products (Revenue)"
            )
            st.plotly_chart(fig, use_container_width=True)


def behavior():
    st.header("Behavior")
    latest = _latest_date("gold.page_performance")
    if not latest:
        st.warning("No page performance yet. Run the ETL first.")
        return

    pages = _df(f"""
        SELECT TOP 20 page_url, views, unique_visitors, avg_time_on_page_seconds, avg_scroll_depth, bounce_rate
        FROM gold.page_performance
        WHERE metric_date = '{latest}'
        ORDER BY views DESC;
        """)
    st.subheader(f"Top Pages ({latest})")
    st.dataframe(pages, use_container_width=True, hide_index=True)


def ops():
    st.header("Ops")
    latest = _latest_date("gold.system_health_daily")
    if not latest:
        st.warning("No system health yet. Run the ETL first.")
        return

    health = _df(f"""
        SELECT service, p50_latency_ms, p95_latency_ms, error_rate, dlq_count
        FROM gold.system_health_daily
        WHERE metric_date = '{latest}'
        ORDER BY service;
        """)
    st.subheader(f"System Health ({latest})")
    st.dataframe(health, use_container_width=True, hide_index=True)


PAGES = {
    "Executive": executive,
    "Product": product,
    "Behavior": behavior,
    "Ops": ops,
}

choice = st.sidebar.radio("Navigation", list(PAGES.keys()))
PAGES[choice]()
