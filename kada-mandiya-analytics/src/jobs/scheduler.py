from __future__ import annotations

import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger

from src.config import load_settings
from src.db.engine import get_engine
from src.jobs.locking import LockNotAcquired, db_lock
from src.jobs.pipeline import run_pipeline

logger.remove()
logger.add(sys.stderr, level="INFO")

PIPELINE_INTERVAL_SECONDS = 300


def _run_pipeline_job() -> None:
    settings = load_settings()
    engine = get_engine(settings)

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            with db_lock(conn):
                run_pipeline(run_type="pipeline_scheduled", include_seed=False)
        except LockNotAcquired:
            logger.info("Another ETL run is in progress; skipping.")
        except Exception:
            logger.exception("Scheduled pipeline failed")


def main() -> int:
    settings = load_settings()

    scheduler = BackgroundScheduler(
        job_defaults={
            "max_instances": settings.etl_max_instances,
            "coalesce": settings.etl_coalesce,
            "misfire_grace_time": settings.etl_misfire_grace_seconds,
        }
    )

    scheduler.add_job(
        _run_pipeline_job,
        "interval",
        seconds=PIPELINE_INTERVAL_SECONDS,
        id="pipeline_no_seed",
    )

    scheduler.start()
    logger.info("Scheduler started: interval={}s", PIPELINE_INTERVAL_SECONDS)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopping...")
        scheduler.shutdown(wait=False)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
