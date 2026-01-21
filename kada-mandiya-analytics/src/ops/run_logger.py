from __future__ import annotations

from functools import lru_cache

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine


@lru_cache(maxsize=1)
def _engine():
    settings = load_settings()
    return get_engine(settings)


def fail_stale_running_runs(
    run_type_prefix: str | None = None, older_than_minutes: int = 10
) -> int:
    prefix = None if run_type_prefix is None else f"{run_type_prefix}%"
    with _engine().begin() as conn:
        res = conn.execute(
            text("""
                UPDATE ops.etl_runs
                SET finished_at = SYSUTCDATETIME(),
                    status = 'failed',
                    error_message = CONCAT(
                        'Auto-failed stale running run (older than ',
                        CAST(:older_than_minutes AS varchar(10)),
                        ' minutes).'
                    )
                WHERE status = 'running'
                  AND finished_at IS NULL
                  AND (
                      started_at < DATEADD(minute, -:older_than_minutes, SYSUTCDATETIME())
                      OR started_at < DATEADD(minute, -:older_than_minutes, SYSDATETIME())
                  )
                  AND (:run_type_prefix IS NULL OR run_type LIKE :run_type_prefix);
                """),
            {"older_than_minutes": int(older_than_minutes), "run_type_prefix": prefix},
        )
        return int(getattr(res, "rowcount", 0) or 0)


def start_run(run_type: str) -> str:
    with _engine().begin() as conn:
        row = conn.execute(
            text("""
                INSERT INTO ops.etl_runs (run_type, started_at, status, rows_inserted)
                OUTPUT inserted.run_id
                VALUES (:run_type, SYSUTCDATETIME(), 'running', 0);
                """),
            {"run_type": str(run_type)[:200]},
        ).one()
        return str(row[0])


def finish_run(
    run_id: str,
    status: str,
    rows_inserted: int = 0,
    error_message: str | None = None,
) -> None:
    msg = error_message
    if msg is not None and len(msg) > 3800:
        msg = msg[:3800] + "..."

    with _engine().begin() as conn:
        conn.execute(
            text("""
                UPDATE ops.etl_runs
                SET finished_at = SYSUTCDATETIME(),
                    status = :status,
                    rows_inserted = :rows_inserted,
                    error_message = :error_message
                WHERE run_id = :run_id
                  AND finished_at IS NULL;
                """),
            {
                "run_id": run_id,
                "status": str(status)[:32],
                "rows_inserted": int(rows_inserted),
                "error_message": msg,
            },
        )
