from __future__ import annotations

import argparse
import sys

from loguru import logger
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from src.config import load_settings
from src.db.engine import get_engine
from src.jobs.locking import LockNotAcquired, db_lock
from src.jobs.pipeline import run_pipeline
from src.ops.run_logger import fail_stale_running_runs

logger.remove()
logger.add(sys.stderr, level="INFO")


TRANSIENT_DB_EXC = (OperationalError, DBAPIError)


@retry(
    retry=retry_if_exception_type(TRANSIENT_DB_EXC),
    stop=stop_after_attempt(3),
    wait=wait_fixed(2),
    reraise=True,
)
def _run_pipeline(*, run_type: str, include_seed: bool) -> None:
    run_pipeline(run_type=run_type, include_seed=include_seed)


def _pipeline_run_type(*, include_seed: bool, enable_silver: bool, enable_gold: bool) -> str:
    parts: list[str] = []
    if include_seed:
        parts.append("seed")
    if enable_silver:
        parts.append("silver")
    if enable_gold:
        parts.append("gold")
    return "+".join(parts) if parts else "pipeline"


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

    run_label = _pipeline_run_type(
        include_seed=include_seed, enable_silver=enable_silver, enable_gold=enable_gold
    )

    fail_stale_running_runs(older_than_minutes=10)

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            with db_lock(conn):
                try:
                    _run_pipeline(run_type=run_label, include_seed=include_seed)
                    return {"status": "success"}
                except Exception as exc:
                    logger.error("ETL failed: {}", exc)
                    return {"status": "failed", "error": str(exc), "run": run_label}
        except LockNotAcquired:
            logger.info("Another ETL run is in progress; skipping.")
            return {"status": "skipped", "run": run_label}


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
