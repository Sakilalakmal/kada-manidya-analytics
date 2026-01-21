from __future__ import annotations

from functools import lru_cache

from fastapi import Header, HTTPException

from src.config import load_settings


@lru_cache(maxsize=1)
def _expected_key() -> str | None:
    settings = load_settings()
    if settings.analytics_api_key is None:
        return None
    return settings.analytics_api_key.get_secret_value()


def require_api_key(
    x_analytics_key: str | None = Header(default=None, alias="X-ANALYTICS-KEY")
) -> None:
    expected = _expected_key()
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="Server misconfigured: ANALYTICS_API_KEY missing",
        )
    if x_analytics_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
