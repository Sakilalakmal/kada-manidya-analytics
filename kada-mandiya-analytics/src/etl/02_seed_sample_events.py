from __future__ import annotations

import json
import random
import string
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.etl._ops import fail_run, finish_run, start_run


def _rand_choice(items):
    return items[random.randint(0, len(items) - 1)]


def _rand_id(prefix: str, n: int = 8) -> str:
    return (
        prefix
        + "-"
        + "".join(random.choices(string.ascii_uppercase + string.digits, k=n))
    )


def main() -> int:
    random.seed(42)

    settings = load_settings()
    engine = get_engine(settings)

    now = datetime.now()
    products = [f"P{idx:03d}" for idx in range(1, 11)]
    services = ["auth-service", "product-service", "order-service", "analytics-service"]
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/121.0 Mobile Safari/537.36",
    ]
    utm_sources = [None, "google", "facebook", "instagram", "newsletter"]
    utm_media = [None, "cpc", "organic", "social", "email"]
    utm_campaigns = [None, "new_year", "sale", "brand", "retargeting"]

    insert_counts = {
        "page_view_events": 0,
        "click_events": 0,
        "scroll_events": 0,
        "form_events": 0,
        "search_events": 0,
        "business_events": 0,
        "api_request_logs": 0,
        "db_query_perf": 0,
    }

    with engine.begin() as conn:
        run = start_run(conn, "seed_sample_events")
        try:
            sessions: list[dict] = []
            for _ in range(100):
                session_id = uuid4().hex
                user_id = None
                if random.random() < 0.65:
                    user_id = f"U{random.randint(1, 50):03d}"

                start = now - timedelta(
                    days=random.randint(0, 6), minutes=random.randint(0, 12 * 60)
                )
                sessions.append(
                    {"session_id": session_id, "user_id": user_id, "start": start}
                )

            page_view_rows: list[dict] = []
            click_rows: list[dict] = []
            scroll_rows: list[dict] = []
            form_rows: list[dict] = []
            search_rows: list[dict] = []
            business_rows: list[dict] = []
            api_rows: list[dict] = []
            db_rows: list[dict] = []

            for s in sessions:
                session_id = s["session_id"]
                user_id = s["user_id"]
                t = s["start"]

                entry_utm_source = _rand_choice(utm_sources)
                entry_utm_medium = _rand_choice(utm_media)
                entry_utm_campaign = _rand_choice(utm_campaigns)

                ip = f"192.168.1.{random.randint(2, 254)}"
                ua = _rand_choice(user_agents)

                n_pages = random.randint(2, 10)

                seq_pages: list[str] = ["/", "/products"]
                for _ in range(max(0, n_pages - 2)):
                    r = random.random()
                    if r < 0.45:
                        product_id = _rand_choice(products)
                        seq_pages.append(f"/products/{product_id}")
                    elif r < 0.60:
                        seq_pages.append("/search")
                    elif r < 0.75:
                        seq_pages.append("/cart")
                    elif r < 0.90:
                        seq_pages.append("/checkout")
                    else:
                        seq_pages.append("/profile")

                prev_page = None
                for i, page in enumerate(seq_pages):
                    t = t + timedelta(seconds=random.randint(8, 60))
                    load_time_ms = int(max(80, random.gauss(450, 120)))

                    row = {
                        "event_timestamp": t,
                        "session_id": session_id,
                        "user_id": user_id,
                        "page_url": page,
                        "referrer_url": prev_page,
                        "utm_source": entry_utm_source if i == 0 else None,
                        "utm_medium": entry_utm_medium if i == 0 else None,
                        "utm_campaign": entry_utm_campaign if i == 0 else None,
                        "time_on_prev_page_seconds": (
                            random.randint(5, 160) if i > 0 else None
                        ),
                        "properties": json.dumps(
                            {
                                "load_time_ms": load_time_ms,
                                "device": _rand_choice(["mobile", "desktop"]),
                            }
                        ),
                    }
                    page_view_rows.append(row)
                    prev_page = page

                    if random.random() < 0.55:
                        scroll_rows.append(
                            {
                                "event_timestamp": t
                                + timedelta(seconds=random.randint(1, 25)),
                                "session_id": session_id,
                                "user_id": user_id,
                                "page_url": page,
                                "scroll_depth_pct": round(random.uniform(10, 100), 2),
                                "properties": None,
                            }
                        )

                    clicks_here = random.randint(0, 4)
                    for _ in range(clicks_here):
                        click_t = t + timedelta(seconds=random.randint(1, 40))
                        element_id = _rand_choice(
                            [
                                "nav-home",
                                "nav-products",
                                "btn-search",
                                "btn-add-to-cart",
                                "product-card",
                                "btn-checkout",
                            ]
                        )

                        props: dict | None = None
                        if page.startswith("/products/"):
                            product_id = page.split("/")[-1]
                            interaction_type = "click"
                            if (
                                element_id == "btn-add-to-cart"
                                and random.random() < 0.6
                            ):
                                interaction_type = "add_to_cart"
                            props = {
                                "product_id": product_id,
                                "interaction_type": interaction_type,
                            }

                        click_rows.append(
                            {
                                "event_timestamp": click_t,
                                "session_id": session_id,
                                "user_id": user_id,
                                "page_url": page,
                                "element_id": element_id,
                                "x": random.randint(0, 1280),
                                "y": random.randint(0, 720),
                                "viewport_w": 1280,
                                "viewport_h": 720,
                                "user_agent": ua,
                                "ip_address": ip,
                                "properties": json.dumps(props) if props else None,
                            }
                        )

                    if page == "/search" and random.random() < 0.7:
                        q = _rand_choice(
                            ["rice", "oil", "spices", "dal", "tea", "soap"]
                        )
                        search_rows.append(
                            {
                                "event_timestamp": t
                                + timedelta(seconds=random.randint(1, 15)),
                                "session_id": session_id,
                                "user_id": user_id,
                                "page_url": page,
                                "query": q,
                                "results_count": random.randint(0, 40),
                                "filters": json.dumps(
                                    {
                                        "category": _rand_choice(
                                            ["grocery", "home", "all"]
                                        )
                                    }
                                ),
                                "properties": None,
                            }
                        )

                    if page == "/checkout" and random.random() < 0.35:
                        form_rows.append(
                            {
                                "event_timestamp": t
                                + timedelta(seconds=random.randint(5, 40)),
                                "session_id": session_id,
                                "user_id": user_id,
                                "page_url": page,
                                "form_id": "checkout-form",
                                "field_id": _rand_choice(
                                    ["address", "phone", "payment_method"]
                                ),
                                "action": _rand_choice(["focus", "change", "submit"]),
                                "error_message": None,
                                "time_spent_ms": random.randint(500, 8000),
                                "properties": None,
                            }
                        )

                # Business outcomes for a subset of sessions
                if random.random() < 0.25:
                    product_id = _rand_choice(products)
                    order_id = _rand_id("ORD")
                    correlation_id = session_id[:64]
                    order_total = round(random.uniform(150, 2500), 2)

                    created_t = s["start"] + timedelta(seconds=random.randint(60, 600))
                    paid_t = created_t + timedelta(seconds=random.randint(30, 300))
                    cancelled = random.random() < 0.05

                    business_rows.append(
                        {
                            "event_timestamp": created_t,
                            "correlation_id": correlation_id,
                            "service": "order-service",
                            "event_type": "order_created",
                            "user_id": user_id,
                            "entity_id": order_id,
                            "payload": json.dumps(
                                {
                                    "order_id": order_id,
                                    "product_id": product_id,
                                    "revenue": order_total,
                                }
                            ),
                        }
                    )
                    if cancelled:
                        business_rows.append(
                            {
                                "event_timestamp": paid_t,
                                "correlation_id": correlation_id,
                                "service": "order-service",
                                "event_type": "order_cancelled",
                                "user_id": user_id,
                                "entity_id": order_id,
                                "payload": json.dumps(
                                    {"order_id": order_id, "reason": "user_cancelled"}
                                ),
                            }
                        )
                    else:
                        business_rows.append(
                            {
                                "event_timestamp": paid_t,
                                "correlation_id": correlation_id,
                                "service": "order-service",
                                "event_type": "order_paid",
                                "user_id": user_id,
                                "entity_id": order_id,
                                "payload": json.dumps(
                                    {
                                        "order_id": order_id,
                                        "product_id": product_id,
                                        "revenue": order_total,
                                    }
                                ),
                            }
                        )

                        if random.random() < 0.10:
                            refund_t = paid_t + timedelta(hours=random.randint(1, 72))
                            business_rows.append(
                                {
                                    "event_timestamp": refund_t,
                                    "correlation_id": correlation_id,
                                    "service": "order-service",
                                    "event_type": "refund_issued",
                                    "user_id": user_id,
                                    "entity_id": order_id,
                                    "payload": json.dumps(
                                        {
                                            "order_id": order_id,
                                            "product_id": product_id,
                                            "refund": order_total,
                                        }
                                    ),
                                }
                            )

                        if random.random() < 0.35 and user_id is not None:
                            review_t = paid_t + timedelta(hours=random.randint(1, 96))
                            rating = random.choices(
                                [3, 4, 5], weights=[0.2, 0.35, 0.45]
                            )[0]
                            business_rows.append(
                                {
                                    "event_timestamp": review_t,
                                    "correlation_id": correlation_id,
                                    "service": "product-service",
                                    "event_type": "review_submitted",
                                    "user_id": user_id,
                                    "entity_id": product_id,
                                    "payload": json.dumps(
                                        {
                                            "product_id": product_id,
                                            "rating": rating,
                                            "comment": "sample review",
                                        }
                                    ),
                                }
                            )

                # API + DB perf logs
                api_calls = random.randint(8, 25)
                for _ in range(api_calls):
                    service = _rand_choice(services)
                    endpoint = _rand_choice(
                        [
                            "/api/products",
                            "/api/orders",
                            "/api/cart",
                            "/api/auth/login",
                            "/api/reviews",
                        ]
                    )
                    status = random.choices(
                        [200, 201, 400, 401, 404, 500], weights=[72, 8, 6, 4, 6, 4]
                    )[0]
                    rt = int(max(10, random.gauss(180, 80)))
                    api_rows.append(
                        {
                            "timestamp": s["start"]
                            + timedelta(seconds=random.randint(1, 1800)),
                            "service": service,
                            "endpoint": endpoint,
                            "method": _rand_choice(["GET", "POST"]),
                            "status_code": status,
                            "response_time_ms": rt,
                            "user_id": user_id,
                            "correlation_id": session_id[:64],
                            "request_size_bytes": int(max(0, random.gauss(900, 300))),
                            "response_size_bytes": int(max(0, random.gauss(2800, 900))),
                            "ip_address": ip,
                            "user_agent": ua,
                        }
                    )

                db_calls = random.randint(3, 12)
                for _ in range(db_calls):
                    db_rows.append(
                        {
                            "timestamp": s["start"]
                            + timedelta(seconds=random.randint(1, 1800)),
                            "service": _rand_choice(services),
                            "database_name": settings.db_name,
                            "query_type": _rand_choice(["SELECT", "INSERT", "UPDATE"]),
                            "table_name": _rand_choice(
                                [
                                    "products",
                                    "orders",
                                    "customers",
                                    "reviews",
                                    "payments",
                                ]
                            ),
                            "execution_time_ms": int(max(1, random.gauss(35, 25))),
                            "rows_affected": random.randint(0, 50),
                            "query_hash": uuid4().hex[:32],
                        }
                    )

            if page_view_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.page_view_events
                            (event_timestamp, session_id, user_id, page_url, referrer_url,
                             utm_source, utm_medium, utm_campaign, time_on_prev_page_seconds, properties)
                        VALUES
                            (:event_timestamp, :session_id, :user_id, :page_url, :referrer_url,
                             :utm_source, :utm_medium, :utm_campaign, :time_on_prev_page_seconds, :properties);
                        """),
                    page_view_rows,
                )
                insert_counts["page_view_events"] = len(page_view_rows)

            if click_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.click_events
                            (event_timestamp, session_id, user_id, page_url, element_id,
                             x, y, viewport_w, viewport_h, user_agent, ip_address, properties)
                        VALUES
                            (:event_timestamp, :session_id, :user_id, :page_url, :element_id,
                             :x, :y, :viewport_w, :viewport_h, :user_agent, :ip_address, :properties);
                        """),
                    click_rows,
                )
                insert_counts["click_events"] = len(click_rows)

            if scroll_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.scroll_events
                            (event_timestamp, session_id, user_id, page_url, scroll_depth_pct, properties)
                        VALUES
                            (:event_timestamp, :session_id, :user_id, :page_url, :scroll_depth_pct, :properties);
                        """),
                    scroll_rows,
                )
                insert_counts["scroll_events"] = len(scroll_rows)

            if form_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.form_events
                            (event_timestamp, session_id, user_id, page_url, form_id, field_id, action,
                             error_message, time_spent_ms, properties)
                        VALUES
                            (:event_timestamp, :session_id, :user_id, :page_url, :form_id, :field_id, :action,
                             :error_message, :time_spent_ms, :properties);
                        """),
                    form_rows,
                )
                insert_counts["form_events"] = len(form_rows)

            if search_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.search_events
                            (event_timestamp, session_id, user_id, page_url, query, results_count, filters, properties)
                        VALUES
                            (:event_timestamp, :session_id, :user_id, :page_url, :query, :results_count, :filters, :properties);
                        """),
                    search_rows,
                )
                insert_counts["search_events"] = len(search_rows)

            if business_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.business_events
                            (event_timestamp, correlation_id, service, event_type, user_id, entity_id, payload)
                        VALUES
                            (:event_timestamp, :correlation_id, :service, :event_type, :user_id, :entity_id, :payload);
                        """),
                    business_rows,
                )
                insert_counts["business_events"] = len(business_rows)

            if api_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.api_request_logs
                            ([timestamp], service, endpoint, method, status_code, response_time_ms, user_id,
                             correlation_id, request_size_bytes, response_size_bytes, ip_address, user_agent)
                        VALUES
                            (:timestamp, :service, :endpoint, :method, :status_code, :response_time_ms, :user_id,
                             :correlation_id, :request_size_bytes, :response_size_bytes, :ip_address, :user_agent);
                        """),
                    api_rows,
                )
                insert_counts["api_request_logs"] = len(api_rows)

            if db_rows:
                conn.execute(
                    text("""
                        INSERT INTO bronze.db_query_perf
                            ([timestamp], service, database_name, query_type, table_name,
                             execution_time_ms, rows_affected, query_hash)
                        VALUES
                            (:timestamp, :service, :database_name, :query_type, :table_name,
                             :execution_time_ms, :rows_affected, :query_hash);
                        """),
                    db_rows,
                )
                insert_counts["db_query_perf"] = len(db_rows)

            total_inserted = sum(insert_counts.values())
            finish_run(conn, run, rows_inserted=total_inserted)

        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print("Seed complete (rows inserted):")
    for k, v in insert_counts.items():
        print(f"- {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
