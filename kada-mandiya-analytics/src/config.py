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
