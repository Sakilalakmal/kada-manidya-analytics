from __future__ import annotations

import argparse
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


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Silver layer from Bronze events")
    p.add_argument(
        "--days",
        type=int,
        default=30,
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


def main() -> int:
    args = _parse_args()
    settings = load_settings()
    engine = get_engine(settings)

    with engine.begin() as conn:
        run = start_run(conn, "build_silver")
        try:
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

            finish_run(conn, run, rows_inserted=rows_inserted)
        except Exception as exc:
            fail_run(conn, run, str(exc))
            raise

    print(
        "Silver layer built: user_sessions, page_sequence, product_interactions, orders, order_items, payments, reviews."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
