from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection


@dataclass(frozen=True)
class EtlRun:
    run_id: str
    run_type: str


def assert_ops_ready(conn: Connection) -> None:
    exists = conn.execute(text("SELECT OBJECT_ID('ops.etl_runs', 'U');")).scalar()
    if exists is None:
        raise RuntimeError(
            "ops.etl_runs not found. Run: python -m src.etl.01_create_warehouse"
        )


def start_run(conn: Connection, run_type: str) -> EtlRun:
    assert_ops_ready(conn)
    row = conn.execute(
        text("""
            INSERT INTO ops.etl_runs (run_type, started_at, status, rows_inserted)
            OUTPUT inserted.run_id
            VALUES (:run_type, SYSDATETIME(), 'running', 0);
            """),
        {"run_type": run_type},
    ).one()
    return EtlRun(run_id=str(row[0]), run_type=run_type)


def finish_run(conn: Connection, run: EtlRun, rows_inserted: int = 0) -> None:
    conn.execute(
        text("""
            UPDATE ops.etl_runs
            SET finished_at = SYSDATETIME(),
                status = 'success',
                rows_inserted = :rows_inserted,
                error_message = NULL
            WHERE run_id = :run_id;
            """),
        {"run_id": run.run_id, "rows_inserted": int(rows_inserted)},
    )


def fail_run(conn: Connection, run: EtlRun, error_message: str) -> None:
    msg = error_message
    if len(msg) > 3800:
        msg = msg[:3800] + "…"
    conn.execute(
        text("""
            UPDATE ops.etl_runs
            SET finished_at = SYSDATETIME(),
                status = 'failed',
                error_message = :error_message
            WHERE run_id = :run_id;
            """),
        {"run_id": run.run_id, "error_message": msg},
    )
