from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection


@dataclass(frozen=True)
class JobRun:
    run_id: str
    run_type: str


def start_job_run(conn: Connection, run_type: str) -> JobRun:
    row = conn.execute(
        text("""
            INSERT INTO ops.etl_runs (run_type, started_at, status, rows_inserted)
            OUTPUT inserted.run_id
            VALUES (:run_type, SYSDATETIME(), 'running', 0);
            """),
        {"run_type": run_type},
    ).one()
    return JobRun(run_id=str(row[0]), run_type=run_type)


def finish_job_run(
    conn: Connection, run: JobRun, status: str, error_message: str | None = None
) -> None:
    msg = error_message
    if msg is not None and len(msg) > 3800:
        msg = msg[:3800] + "…"
    conn.execute(
        text("""
            UPDATE ops.etl_runs
            SET finished_at = SYSDATETIME(),
                status = :status,
                error_message = :error_message
            WHERE run_id = :run_id;
            """),
        {"run_id": run.run_id, "status": status, "error_message": msg},
    )
