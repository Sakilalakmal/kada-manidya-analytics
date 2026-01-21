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

Supported `event_type` values:
- `page_view`, `click`, `scroll`, `form_interaction`, `search`
- `performance`, `frontend_error`
- `cart_action`, `checkout`, `purchase_view`
- `api_request_log`, `db_query_perf`
- Any other `event_type` is treated as a service/domain `BusinessEvent` and written to `bronze.business_events` (must include `service` and `payload`).

Example (PowerShell):
```powershell
curl.exe -X POST http://127.0.0.1:8000/events `
  -H "Content-Type: application/json" `
  -H "X-ANALYTICS-KEY: change-me" `
  -d "{\"event_type\":\"page_view\",\"event_timestamp\":\"2026-01-20T00:00:00Z\",\"session_id\":\"s1\",\"user_id\":\"u1\",\"source\":\"web\",\"page_url\":\"/\",\"referrer_url\":null,\"properties\":{\"load_time_ms\":420}}"
```

## Run RabbitMQ -> Analytics Consumer

Durable RabbitMQ topic consumer that ingests service/domain messages into `bronze.business_events` (append-only).

- Exchange (default): `domain.events` (topic)
- Routing keys (default): `order.*`, `payment.*`, `review.*`
- Idempotency: computes a deterministic SHA-256 fingerprint and claims it in `ops.event_fingerprints` (auto-created if missing). Duplicates are ACKed and skipped.
- Failures: invalid JSON or DB failures after retries are written to `ops.dead_letter_events` and ACKed (analytics is not source of truth).

Start consumer (PowerShell):

```powershell
cd analytics/kada-mandiya-analytics
venv\Scripts\activate
python -m src.scripts.run_consumer
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
FROM bronze.business_events
ORDER BY event_timestamp DESC;
```

## Run ETL

```powershell
python -m src.etl.01_create_warehouse
python -m src.etl.02_seed_sample_events
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
```

Schedule config (in `.env`):
- `ETL_INTERVAL_SECONDS` (default `120`)
- `ETL_ENABLE_SILVER` / `ETL_ENABLE_GOLD` (`yes`/`no`)
- `ETL_MAX_INSTANCES` (default `1`)
- `ETL_COALESCE` (`yes`/`no`)
- `ETL_MISFIRE_GRACE_SECONDS` (default `30`)

## Run Dashboard

```powershell
streamlit run dashboards/app.py
```

## Notes

- Login failures: check `DB_USER`/`DB_PASSWORD` and SQL Server authentication mode (Mixed Mode).
- Network/port failures: ensure SQL Server is running, TCP/IP is enabled, and port `1433` is reachable.
- Local dev TLS: uses ODBC Driver 18 with `Encrypt=yes` and `TrustServerCertificate=yes`.
