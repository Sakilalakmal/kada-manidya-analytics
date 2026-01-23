# Kada Mandiya Analytics (Phase 1)

Warehouse foundation (Bronze/Silver/Gold/Ops) + ETL skeleton + Streamlit dashboard skeleton.

## Setup (Windows PowerShell)

```powershell
cd analytics/kada-mandiya-analytics
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
notepad .env
python src/scripts/test_db_connection.py
```

## Run Collector (FastAPI)

- Writes validated events into Bronze tables (append-only) and sends invalid/unknown events to `ops.dead_letter_events`.
- Auth: `X-ANALYTICS-KEY` header, configured via `ANALYTICS_API_KEY` in `.env`.

```powershell
uvicorn src.api.analytics_collector:app --host 127.0.0.1 --port 8000
```

## Run Real User Tracking Ingest API (FastAPI)

Accepts real web events and publishes them to RabbitMQ exchange `domain.events` using these routing keys:

- `ui.page_view`, `ui.click`, `ui.add_to_cart`, `ui.begin_checkout`

The `analytics_consumer` ingests them and writes to:

- `bronze.page_view_events`
- `bronze.click_events`
- `bronze.cart_events` (for `ui.add_to_cart`)
- `bronze.checkout_events` (for `ui.begin_checkout`)

```powershell
python -m src.tracking.run_server
```

If port `9000` is already in use:

```powershell
python -m src.tracking.run_server --port 9001
# and point the web app to it:
# $env:NEXT_PUBLIC_TRACKING_API_BASE_URL="http://localhost:9001"
```

Supported `event_type` values:

- `page_view`, `click`, `add_to_cart`, `begin_checkout`

Health:

```powershell
curl.exe http://127.0.0.1:9000/health
```

Example (PowerShell, `ui.add_to_cart`):

```powershell
curl.exe -X POST http://127.0.0.1:9000/track `
  -H "Content-Type: application/json" `
  -d "{\"event_type\":\"add_to_cart\",\"session_id\":\"s1\",\"user_id\":\"u1\",\"page_url\":\"/products/abc\",\"product_id\":\"abc\",\"quantity\":1,\"element_id\":\"btn_add_to_cart\",\"properties\":{\"page_url\":\"/products/abc\"}}"
```

## Run RabbitMQ -> Analytics Consumer

Durable RabbitMQ topic consumer that ingests service/domain messages into `bronze.business_events` (append-only).

- Exchange (default): `domain.events` (topic)
- Routing keys (default): `ui.page_view`, `ui.click`, `ui.add_to_cart`, `ui.begin_checkout`, `order.*`, `payment.*`, `review.*`
- Idempotency: computes a deterministic SHA-256 fingerprint and claims it in `ops.event_fingerprints` (auto-created if missing). Duplicates are ACKed and skipped.
- Failures: invalid JSON or DB failures after retries are written to `ops.dead_letter_events` and ACKed (analytics is not source of truth).

Start consumer (PowerShell):

```powershell
cd analytics/kada-mandiya-analytics
venv\Scripts\activate
python -m src.scripts.run_consumer
```

Local dev (recommended): keep Gold fresh automatically

```powershell
# terminal 1: consumer (business events)
python -m src.scripts.run_consumer

# terminal 2: ETL watch (silver+gold)
python -m src.jobs.runner --watch --interval-seconds 30 --window-days 2
```

Consumer env vars (in `.env`):

- `ANALYTICS_CONSUMER_ENABLED` (`yes`/`no`)
- `RABBITMQ_URL` (e.g. `amqp://guest:guest@localhost:5672/`)
- `RABBITMQ_EXCHANGE` (default `domain.events`)
- `RABBITMQ_EXCHANGE_TYPE` (default `topic`)
- `RABBITMQ_QUEUE` (default `analytics.business.events`)
- `RABBITMQ_ROUTING_KEYS` (CSV, default `order.*,payment.*,review.*`)
- `RABBITMQ_PREFETCH` (default `50`)
- `RABBITMQ_DLQ` (optional; if set, consumer declares a local DLX `analytics.dlx` and routes rejected messages)

Verify ingestion (SQL Server):

```sql
SELECT TOP 20 *
FROM bronze.click_events
ORDER BY event_timestamp DESC;

SELECT COUNT(*)
FROM bronze.click_events
WHERE CAST(event_timestamp AS date)=CAST(GETDATE() AS date);

SELECT TOP 20 *
FROM bronze.business_events
ORDER BY event_timestamp DESC;
```

Publish test UI events (no web app needed):

```powershell
python -m src.scripts.publish_test_ui_events --session-id test-session --page-url /test --product-id sku_test_001 --quantity 1
```

## Run ETL

```powershell
python -m src.etl.01_create_warehouse
python -m src.etl.02b_seed_business_events
python -m src.etl.02c_seed_behavior_events
python -m src.etl.03_build_silver
python -m src.etl.04_build_gold
```

## Run ETL Scheduler (APScheduler)

Runs Silver then Gold on a timer with a SQL Server application lock (`sp_getapplock`) to prevent overlapping runs across processes.

```powershell
python -m src.jobs.scheduler
```

One-shot (debug):

```powershell
python -m src.jobs.runner --once
python -m src.jobs.runner --once --no-seed
python -m src.jobs.runner --once --seed-all
```

Watch mode (recommended for keeping Gold fresh automatically):

```powershell
python -m src.jobs.runner --watch
python -m src.jobs.runner --watch --interval-seconds 30
python -m src.jobs.runner --watch --interval-seconds 30 --window-days 2
```

Watch recompute window:

- `--window-days 2` sets `ETL_RECENT_DAYS=2` for that process (Silver/Gold rebuild only recent dates for speed).

Schedule config (in `.env`):

- `ETL_INTERVAL_SECONDS` (default `120`)
- `ETL_ENABLE_SILVER` / `ETL_ENABLE_GOLD` (`yes`/`no`)
- `ETL_MAX_INSTANCES` (default `1`)
- `ETL_COALESCE` (`yes`/`no`)
- `ETL_MISFIRE_GRACE_SECONDS` (default `30`)
- `SHOW_SEED_DATA` (`yes`/`no`, default `no`) to include seeded demo data in Gold builds

## Run Dashboard

```powershell
streamlit run dashboards/app.py
```

Verify behavior + funnel tables:

```powershell
python -m src.ops.verify_behavior
python -m src.ops.verify_tracking
```

## Notes

- Login failures: check `DB_USER`/`DB_PASSWORD` and SQL Server authentication mode (Mixed Mode).
- Network/port failures: ensure SQL Server is running, TCP/IP is enabled, and port `1433` is reachable.
- Local dev TLS: uses ODBC Driver 18 with `Encrypt=yes` and `TrustServerCertificate=yes`.
