from __future__ import annotations

import argparse
import sys

from loguru import logger
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from src.config import load_settings
from src.db.engine import get_engine
from src.jobs.locking import LockNotAcquired, db_lock
from src.jobs.ops_logger import finish_job_run, start_job_run
from src.jobs.pipeline import run_pipeline

logger.remove()
logger.add(sys.stderr, level="INFO")


TRANSIENT_DB_EXC = (OperationalError, DBAPIError)


@retry(
    retry=retry_if_exception_type(TRANSIENT_DB_EXC),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)
def _run_pipeline(include_seed: bool) -> None:
    run_pipeline(include_seed=include_seed)


def run_pipeline_once(*, include_seed: bool = True) -> dict[str, str]:
    settings = load_settings()
    engine = get_engine(settings)

    enable_silver = bool(getattr(settings, "etl_enable_silver", True))
    enable_gold = bool(getattr(settings, "etl_enable_gold", True))

    if not include_seed and not enable_silver and not enable_gold:
        logger.info(
            "Pipeline skipped (ETL_ENABLE_SILVER=no and ETL_ENABLE_GOLD=no and --no-seed)."
        )
        return {"status": "skipped"}

    parts: list[str] = []
    if include_seed:
        parts.append("seed")
    if enable_silver:
        parts.append("silver")
    if enable_gold:
        parts.append("gold")
    run_type = "+".join(parts) if parts else "pipeline"

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            with db_lock(conn):
                try:
                    run = start_job_run(conn, run_type)
                except Exception as exc:
                    logger.error("Failed to start ops.etl_runs row: {}", exc)
                    return {"status": "failed"}
                try:
                    _run_pipeline(include_seed=include_seed)

                    finish_job_run(conn, run, status="success", error_message=None)
                    return {"status": "success"}
                except Exception as exc:
                    finish_job_run(conn, run, status="failed", error_message=str(exc))
                    logger.error("ETL failed: {}", exc)
                    return {"status": "failed"}
        except LockNotAcquired:
            logger.info("Another ETL run is in progress; skipping.")
            return {"status": "skipped"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run analytics ETL pipeline (seed -> silver -> gold)."
    )
    parser.add_argument(
        "--once", action="store_true", help="Run a single cycle and exit."
    )
    parser.add_argument(
        "--no-seed", action="store_true", help="Skip the seed step (DEV-only)."
    )
    args = parser.parse_args()

    if args.once:
        result = run_pipeline_once(include_seed=not args.no_seed)
        return 0 if result["status"] in {"success", "skipped"} else 1

    result = run_pipeline_once(include_seed=not args.no_seed)
    return 0 if result["status"] in {"success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
