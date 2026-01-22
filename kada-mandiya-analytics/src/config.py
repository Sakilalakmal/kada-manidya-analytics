from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


class Settings(BaseModel):
    model_config = ConfigDict(extra="ignore")

    db_host: str = Field(alias="DB_HOST")
    db_port: int = Field(default=1433, alias="DB_PORT")
    db_user: str = Field(alias="DB_USER")
    db_password: SecretStr = Field(alias="DB_PASSWORD")
    db_name: str = Field(alias="DB_NAME")
    db_driver: str = Field(default="ODBC Driver 18 for SQL Server", alias="DB_DRIVER")
    db_trust_cert: bool = Field(default=True, alias="DB_TRUST_CERT")
    analytics_api_key: SecretStr | None = Field(default=None, alias="ANALYTICS_API_KEY")
    show_seed_data: bool = Field(default=False, alias="SHOW_SEED_DATA")

    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/", alias="RABBITMQ_URL"
    )
    rabbitmq_exchange: str = Field(default="domain.events", alias="RABBITMQ_EXCHANGE")
    rabbitmq_exchange_type: str = Field(default="topic", alias="RABBITMQ_EXCHANGE_TYPE")
    rabbitmq_queue: str = Field(
        default="analytics.business.events", alias="RABBITMQ_QUEUE"
    )
    rabbitmq_routing_keys: str = Field(
        default="order.*,payment.*,review.*", alias="RABBITMQ_ROUTING_KEYS"
    )
    rabbitmq_prefetch: int = Field(default=50, alias="RABBITMQ_PREFETCH")
    rabbitmq_dlq: str | None = Field(default=None, alias="RABBITMQ_DLQ")
    analytics_consumer_enabled: bool = Field(
        default=False, alias="ANALYTICS_CONSUMER_ENABLED"
    )

    etl_interval_seconds: int = Field(default=120, alias="ETL_INTERVAL_SECONDS")
    etl_enable_silver: bool = Field(default=True, alias="ETL_ENABLE_SILVER")
    etl_enable_gold: bool = Field(default=True, alias="ETL_ENABLE_GOLD")
    etl_max_instances: int = Field(default=1, alias="ETL_MAX_INSTANCES")
    etl_coalesce: bool = Field(default=True, alias="ETL_COALESCE")
    etl_misfire_grace_seconds: int = Field(
        default=30, alias="ETL_MISFIRE_GRACE_SECONDS"
    )


def load_settings(env_path: Path | None = None) -> Settings:
    env_file = env_path or (project_root() / ".env")
    if env_file.exists():
        load_dotenv(env_file, override=False)
    try:
        return Settings.model_validate(dict(os.environ))
    except ValidationError as exc:
        raise RuntimeError(
            "Invalid or missing environment variables. Copy `.env.example` to `.env` and edit it."
        ) from exc
