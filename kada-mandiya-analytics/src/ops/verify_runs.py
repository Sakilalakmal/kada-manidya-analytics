from __future__ import annotations

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine


def main() -> int:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.connect() as conn:
        running = conn.execute(
            text("SELECT COUNT(1) FROM ops.etl_runs WHERE status = 'running';")
        ).scalar_one()

        rows = conn.execute(
            text("""
                SELECT TOP 10
                    run_type, status, started_at, finished_at, error_message
                FROM ops.etl_runs
                ORDER BY COALESCE(finished_at, started_at) DESC;
                """)
        ).mappings()

        print(f"running_count={int(running)}")
        print("last_10_runs:")
        for r in rows:
            print(
                f"- run_type={r.get('run_type')} status={r.get('status')} "
                f"started_at={r.get('started_at')} finished_at={r.get('finished_at')} "
                f"error_message={r.get('error_message')}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

