# 📊 Kada Mandiya Analytics

<div align="center">

**Enterprise-grade Analytics Platform for Kada Mandiya E-commerce**

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)](https://www.microsoft.com/sql-server)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-FF6600?style=for-the-badge&logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)

</div>

---

## 🎯 Overview

**Kada Mandiya Analytics** is a comprehensive, production-ready analytics and business intelligence platform designed specifically for the [Kada Mandiya](https://github.com/Sakilalakmal/kada_mandiya_microservice) e-commerce ecosystem.  Built with modern data engineering principles, this platform provides real-time event tracking, advanced ETL pipelines, and actionable business insights through a multi-layered data warehouse architecture.

### 🌟 Key Capabilities

- **📈 Real-time Event Processing**: Event-driven architecture consuming domain events from RabbitMQ
- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **🔄 Automated ETL Jobs**: Scheduled data processing with distributed locking mechanisms
- **📊 Business Intelligence**: Pre-built analytics models for conversion funnels, revenue analysis, and customer insights
- **🚀 High Performance**: Optimized SQL Server warehousing with connection pooling and retry logic
- **🔐 Secure API**: Protected REST endpoints for analytics data access
- **🎭 Multiple Event Types**: Tracks user behavior, business events, API performance, and database queries

---

## 🏛️ Architecture

### Data Flow Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Kada Mandiya Microservices                       │
│         (Order, Payment, Review, Product, User Services)            │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │   RabbitMQ     │
                    │  Event Broker  │
                    └───────┬────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │  Analytics Consumer     │
              │  (Event Ingestion)      │
              └──────────┬──────────────┘
                         │
                         ▼
              ┌──────────────────────────┐
              │    BRONZE Layer          │
              │  (Raw Event Storage)     │
              │  - page_view_events      │
              │  - click_events          │
              │  - business_events       │
              │  - api_request_logs      │
              └──────────┬───────────────┘
                         │
                         ▼
              ┌──────────────────────────┐
              │    SILVER Layer          │
              │ (Cleaned & Enriched)     │
              │  - orders                │
              │  - product_interactions  │
              │  - user_sessions         │
              └──────────┬───────────────┘
                         │
                         ▼
              ┌──────────────────────────┐
              │    GOLD Layer            │
              │  (Business Metrics)      │
              │  - conversion_funnel     │
              │  - revenue_metrics       │
              │  - customer_analytics    │
              └──────────────────────────┘
                         │
                         ▼
              ┌──────────────────────────┐
              │   Analytics API          │
              │  (FastAPI REST)          │
              └──────────────────────────┘
```

### Data Warehouse Layers

| Layer | Purpose | Examples |
|-------|---------|----------|
| **🥉 Bronze** | Raw, immutable event data | Page views, clicks, business events, API logs |
| **🥈 Silver** | Cleaned, validated, deduplicated | Orders, user sessions, product interactions |
| **🥇 Gold** | Aggregated business metrics | Conversion funnels, revenue analytics, cohort analysis |
| **⚙️ Ops** | Operational metadata | ETL run logs, dead letter queue, locks |

---

## 🚀 Getting Started

### Prerequisites

- **Python**:  3.11 or higher
- **SQL Server**: 2019+ or Azure SQL Database
- **RabbitMQ**: 3.9+ (for event consumption)
- **ODBC Driver**:  ODBC Driver 18 for SQL Server

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Sakilalakmal/kada-manidya-analytics.git
   cd kada-manidya-analytics
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

### Configuration

Create a `.env` file with the following settings:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=1433
DB_USER=analytics_user
DB_PASSWORD=your_secure_password
DB_NAME=kada_analytics
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_TRUST_CERT=yes

# RabbitMQ Configuration
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_EXCHANGE=domain. events
RABBITMQ_EXCHANGE_TYPE=topic
RABBITMQ_QUEUE=analytics.business.events
RABBITMQ_ROUTING_KEYS=order.*,payment.*,review.*
RABBITMQ_PREFETCH=50
ANALYTICS_CONSUMER_ENABLED=true

# ETL Job Configuration
ETL_INTERVAL_SECONDS=300
ETL_ENABLE_SILVER=true
ETL_ENABLE_GOLD=true
ETL_MAX_INSTANCES=1
ETL_COALESCE=true
ETL_MISFIRE_GRACE_SECONDS=30

# API Security
ANALYTICS_API_KEY=your_api_key_here
```

### Database Setup

1. **Create the data warehouse**
   ```bash
   python -m src.etl.01_create_warehouse
   ```

2. **Run initial ETL pipeline**
   ```bash
   python -m src.jobs.runner --once
   ```

---

## 📦 Core Components

### 1. Event Models (`src/models/`)

Comprehensive event schema definitions using Pydantic:

- **`BaseEvent`**: Foundation for all analytics events
- **`PageViewEvent`**: User page navigation tracking
- **`ClickEvent`**: Click-stream analytics
- **`SearchEvent`**: Search behavior analysis
- **`CartActionEvent`**: Shopping cart interactions
- **`BusinessEvent`**: Domain events (orders, payments, reviews)
- **`PerformanceEvent`**: Application performance metrics
- **`ApiRequestLogEvent`**: API call tracking
- **`DbQueryPerfEvent`**: Database query performance

### 2. ETL Pipeline (`src/etl/`)

Multi-stage data transformation pipeline:

- **`01_create_warehouse. py`**: Schema initialization
- **`02b_seed_business_events.py`**: Bronze layer event ingestion
- **`03_build_silver. py`**: Data cleaning and enrichment
- **`04_build_gold.py`**: Business metrics aggregation

### 3. Job Orchestration (`src/jobs/`)

- **`runner.py`**: One-time ETL execution
- **`scheduler.py`**: Scheduled background jobs using APScheduler
- **`pipeline.py`**: Orchestrates multi-stage ETL flows
- **`locking.py`**: Distributed lock mechanism to prevent concurrent runs

### 4. Database Layer (`src/db/`)

- **`engine.py`**: SQLAlchemy connection management with retry logic
- **`writers.py`**: Bulk event insertion with duplicate handling

### 5. API (`src/api/`)

- **`security.py`**: API key authentication
- REST endpoints for analytics data retrieval

---

## 🎮 Usage

### Running ETL Pipeline

**One-time execution:**
```bash
python -m src.jobs.runner --once
```

**Skip seed step (development):**
```bash
python -m src.jobs.runner --once --no-seed
```

**Scheduled execution:**
```bash
python -m src.jobs.scheduler
```

### Event Consumption

Start the RabbitMQ consumer to ingest events: 
```bash
python -m src.consumer.rabbitmq_consumer
```

### Analytics API

Launch the FastAPI server:
```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Access API documentation: 
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## 📊 Analytics Capabilities

### Business Metrics

- **Conversion Funnel Analysis**: Track user journey from visit → product view → add to cart → purchase
- **Revenue Analytics**: Daily/weekly/monthly revenue trends and projections
- **Customer Segmentation**: RFM (Recency, Frequency, Monetary) analysis
- **Product Performance**: Best sellers, inventory turnover, category analytics
- **User Behavior**: Session analysis, page flow, bounce rates

### Event Tracking

```python
from src.models.events import PageViewEvent
from src.db.writers import insert_page_view

event = PageViewEvent(
    event_timestamp=datetime.utcnow(),
    session_id="sess_123",
    user_id="user_456",
    source="web",
    page_url="/products/smartphone-x",
    utm_source="google",
    utm_campaign="summer_sale"
)
```

---

## 🔒 Security Features

- **API Key Authentication**: Secured REST endpoints
- **SQL Injection Prevention**: Parameterized queries via SQLAlchemy
- **Connection Encryption**: TLS/SSL for database connections
- **Environment Isolation**: Sensitive credentials via environment variables

---

## 🧪 Development

### Project Structure

```
kada-mandiya-analytics/
├── src/
│   ├── api/              # FastAPI REST endpoints
│   ├── consumer/         # RabbitMQ event consumers
│   ├── db/               # Database engine and writers
│   ├── etl/              # ETL pipeline scripts
│   ├── jobs/             # Job orchestration
│   ├── models/           # Pydantic event models
│   ├── ops/              # Operational utilities
│   └── utils/            # Helper functions
├── tests/                # Test suites
├── . env.example          # Environment template
├── requirements.txt      # Python dependencies
└── README.md
```

### Code Quality

- **Type Hints**: Full type annotations with `mypy` support
- **Pydantic Validation**: Automatic data validation
- **Structured Logging**: JSON-formatted logs with Loguru
- **Error Handling**: Comprehensive exception management with retry logic

---

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-analytics`)
3. Commit your changes (`git commit -m 'Add customer cohort analysis'`)
4. Push to the branch (`git push origin feature/amazing-analytics`)
5. Open a Pull Request

---

## 📝 License

This project is part of the **Kada Mandiya** e-commerce platform ecosystem. 

---

## 🔗 Related Projects

- **[Kada Mandiya Microservices](https://github.com/Sakilalakmal/kada_mandiya_microservice)**: Core e-commerce platform

---

## 📧 Contact

**Developer**: Sakilalakmal  
**GitHub**: [@Sakilalakmal](https://github.com/Sakilalakmal)

---

<div align="center">

**Built with ❤️ for Kada Mandiya E-commerce Platform**

</div>
