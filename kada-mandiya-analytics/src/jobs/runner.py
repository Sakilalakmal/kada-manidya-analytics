from __future__ import annotations

import argparse
import importlib
import sys

from loguru import logger
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from src.config import load_settings
from src.db.engine import get_engine
from src.jobs.locking import LockNotAcquired, db_lock
from src.jobs.ops_logger import finish_job_run, start_job_run

logger.remove()
logger.add(sys.stderr, level="INFO")


TRANSIENT_DB_EXC = (OperationalError, DBAPIError)


def _call_etl_main(module_name: str) -> None:
    mod = importlib.import_module(module_name)
    main = getattr(mod, "main", None)
    if not callable(main):
        raise RuntimeError(f"{module_name}.main() not found")
    rc = int(main())
    if rc != 0:
        raise RuntimeError(f"{module_name}.main() returned {rc}")


@retry(
    retry=retry_if_exception_type(TRANSIENT_DB_EXC),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)
def _run_stage(stage: str) -> None:
    if stage == "silver":
        _call_etl_main("src.etl.03_build_silver")
        return
    if stage == "gold":
        _call_etl_main("src.etl.04_build_gold")
        return
    raise ValueError(f"unknown stage: {stage}")


def run_pipeline_once() -> dict[str, str]:
    settings = load_settings()
    engine = get_engine(settings)

    enable_silver = bool(getattr(settings, "etl_enable_silver", True))
    enable_gold = bool(getattr(settings, "etl_enable_gold", True))

    if not enable_silver and not enable_gold:
        logger.info("ETL disabled (ETL_ENABLE_SILVER=no and ETL_ENABLE_GOLD=no).")
        return {"status": "skipped"}

    run_type = (
        "silver+gold"
        if enable_silver and enable_gold
        else ("silver" if enable_silver else "gold")
    )

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
                    if enable_silver:
                        logger.info("Running silver...")
                        _run_stage("silver")
                    else:
                        logger.info("Skipping silver (ETL_ENABLE_SILVER=no).")

                    if enable_gold:
                        logger.info("Running gold...")
                        _run_stage("gold")
                    else:
                        logger.info("Skipping gold (ETL_ENABLE_GOLD=no).")

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
        description="Run analytics ETL pipeline (silver + gold)."
    )
    parser.add_argument(
        "--once", action="store_true", help="Run a single cycle and exit."
    )
    args = parser.parse_args()

    if args.once:
        result = run_pipeline_once()
        return 0 if result["status"] in {"success", "skipped"} else 1

    result = run_pipeline_once()
    return 0 if result["status"] in {"success", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
