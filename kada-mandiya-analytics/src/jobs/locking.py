from __future__ import annotations

from contextlib import contextmanager

from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

LOCK_NAME = "kada_mandiya_etl_lock"


class LockNotAcquired(Exception):
    pass


def _get_app_lock(conn: Connection, lock_name: str, timeout_ms: int = 0) -> int:
    return int(
        conn.execute(
            text("""
                DECLARE @res int;
                EXEC @res = sp_getapplock
                    @Resource = :resource,
                    @LockMode = 'Exclusive',
                    @LockOwner = 'Session',
                    @LockTimeout = :timeout_ms;
                SELECT @res AS res;
                """),
            {"resource": lock_name, "timeout_ms": int(timeout_ms)},
        ).scalar_one()
    )


def _release_app_lock(conn: Connection, lock_name: str) -> None:
    conn.execute(
        text("""
            DECLARE @res int;
            EXEC @res = sp_releaseapplock
                @Resource = :resource,
                @LockOwner = 'Session';
            """),
        {"resource": lock_name},
    )


@contextmanager
def db_lock(conn: Connection, lock_name: str = LOCK_NAME, timeout_ms: int = 0):
    res = _get_app_lock(conn, lock_name=lock_name, timeout_ms=timeout_ms)
    if res < 0:
        logger.info("ETL lock not acquired (res={}): {}", res, lock_name)
        raise LockNotAcquired(lock_name)

    logger.info("ETL lock acquired: {}", lock_name)
    try:
        yield
    finally:
        try:
            _release_app_lock(conn, lock_name)
            logger.info("ETL lock released: {}", lock_name)
        except Exception as exc:
            logger.warning("Failed to release lock '{}': {}", lock_name, exc)
