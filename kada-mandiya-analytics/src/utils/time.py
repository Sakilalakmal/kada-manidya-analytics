from __future__ import annotations

from datetime import UTC, datetime, timedelta

MAX_FUTURE_SKEW_SECONDS = 10 * 60
MAX_PAST_AGE_DAYS = 365


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("event_timestamp must be timezone-aware UTC datetime")
    return dt.astimezone(UTC)


def to_sqlserver_utc_naive(dt: datetime) -> datetime:
    utc = ensure_utc(dt)
    return utc.replace(tzinfo=None)


def validate_event_timestamp(dt: datetime, now: datetime | None = None) -> None:
    now_utc = now or utc_now()
    ts = ensure_utc(dt)
    if ts > now_utc + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
        raise ValueError("event_timestamp is too far in the future")
    if ts < now_utc - timedelta(days=MAX_PAST_AGE_DAYS):
        raise ValueError("event_timestamp is too far in the past")
