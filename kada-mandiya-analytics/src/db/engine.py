from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

from src.config import Settings


def _bracket_quote(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


@dataclass(frozen=True)
class DbEnsureResult:
    existed: bool
    created: bool


def build_sqlalchemy_url(settings: Settings, database: str) -> URL:
    query = {
        "driver": settings.db_driver,
        "Encrypt": "yes",
        "TrustServerCertificate": "yes" if settings.db_trust_cert else "no",
    }
    return URL.create(
        "mssql+pyodbc",
        username=settings.db_user,
        password=settings.db_password.get_secret_value(),
        host=settings.db_host,
        port=settings.db_port,
        database=database,
        query=query,
    )


def get_engine(settings: Settings, database: str | None = None) -> Engine:
    db = database or settings.db_name
    url = build_sqlalchemy_url(settings, db)
    return create_engine(url, pool_pre_ping=True)


def ensure_database_exists(settings: Settings) -> DbEnsureResult:
    master_engine = get_engine(settings, database="master").execution_options(
        isolation_level="AUTOCOMMIT"
    )
    with master_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM sys.databases WHERE name = :name"),
            {"name": settings.db_name},
        ).first()
        if exists:
            return DbEnsureResult(existed=True, created=False)

        conn.execute(text(f"CREATE DATABASE {_bracket_quote(settings.db_name)}"))
        return DbEnsureResult(existed=False, created=True)
