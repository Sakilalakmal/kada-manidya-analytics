from __future__ import annotations

import argparse
import os
import sys
import time

from loguru import logger

from src.config import load_settings
from src.db.engine import get_engine
from src.jobs.locking import LockNotAcquired, db_lock
from src.jobs.pipeline import run_pipeline
from src.ops.run_logger import fail_stale_running_runs, finish_run, start_run

logger.remove()
logger.add(sys.stderr, level="INFO")


def _pipeline_run_type(*, seed_mode: str, enable_silver: bool, enable_gold: bool) -> str:
    parts: list[str] = []
    seed_mode_norm = str(seed_mode or "business").strip().lower()
    if seed_mode_norm == "business":
        parts.append("seed")
    elif seed_mode_norm == "all":
        parts.append("seed-all")
    if enable_silver:
        parts.append("silver")
    if enable_gold:
        parts.append("gold")
    return "+".join(parts) if parts else "pipeline"


def _record_skipped_run(*, run_type: str, reason: str) -> None:
    run_id = start_run(run_type)
    finish_run(run_id, "skipped", rows_inserted=0, error_message=reason)


def run_pipeline_once(*, seed_mode: str = "business", run_type: str | None = None) -> dict[str, str]:
    settings = load_settings()
    engine = get_engine(settings)

    enable_silver = bool(getattr(settings, "etl_enable_silver", True))
    enable_gold = bool(getattr(settings, "etl_enable_gold", True))

    seed_mode_norm = str(seed_mode or "business").strip().lower()
    if seed_mode_norm not in {"none", "business", "all"}:
        raise ValueError("seed_mode must be one of: none, business, all")

    if seed_mode_norm == "none" and not enable_silver and not enable_gold:
        logger.info(
            "Pipeline skipped (ETL_ENABLE_SILVER=no and ETL_ENABLE_GOLD=no and --no-seed)."
        )
        return {"status": "skipped"}

    run_label = run_type or _pipeline_run_type(
        seed_mode=seed_mode_norm, enable_silver=enable_silver, enable_gold=enable_gold
    )

    fail_stale_running_runs(older_than_minutes=10)

    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            with db_lock(conn):
                try:
                    run_pipeline(run_type=run_label, seed_mode=seed_mode_norm)
                    return {"status": "success"}
                except Exception as exc:
                    logger.error("ETL failed: {}", exc)
                    return {"status": "failed", "error": str(exc), "run": run_label}
        except LockNotAcquired:
            logger.info("Another ETL run is in progress; skipping.")
            if run_type is not None:
                _record_skipped_run(run_type=run_label, reason="Skipped: lock not acquired (another ETL run active).")
            return {"status": "skipped", "run": run_label}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run analytics ETL pipeline (seed -> silver -> gold)."
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--once", action="store_true", help="Run a single cycle and exit.")
    mode_group.add_argument("--watch", action="store_true", help="Run forever; rebuild Silver then Gold on an interval.")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Watch interval in seconds (default: 60).",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=2,
        help="Recompute window (days) for watch cycles via ETL_RECENT_DAYS (default: 2).",
    )
    seed_group = parser.add_mutually_exclusive_group()
    seed_group.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip all seed steps (business + behavior).",
    )
    seed_group.add_argument(
        "--seed-all",
        action="store_true",
        help="Run all seed steps (02b + 02c) before silver+gold.",
    )
    args = parser.parse_args()

    settings = load_settings()
    default_seed_mode = "none" if args.watch else "business"
    seed_mode = "all" if args.seed_all else ("none" if args.no_seed else default_seed_mode)

    watch_interval_s = max(5, int(args.interval_seconds or 60))
    loop_interval_s = max(5, int(getattr(settings, "etl_interval_seconds", 120) or 120))

    if args.once:
        result = run_pipeline_once(seed_mode=seed_mode)
        return 0 if result["status"] in {"success", "skipped"} else 1

    if args.watch:
        os.environ["ETL_RECENT_DAYS"] = str(max(1, int(args.window_days or 2)))
        run_type = "watch_silver+gold"
        seed_mode_for_cycle = seed_mode
        seeded_once = False
        while True:
            result = run_pipeline_once(seed_mode=seed_mode_for_cycle, run_type=run_type)
            if result["status"] == "failed":
                logger.error("Watch cycle failed (will retry next interval).")

            if not seeded_once and seed_mode_for_cycle != "none":
                seeded_once = True
                seed_mode_for_cycle = "none"

            time.sleep(watch_interval_s)

    while True:
        result = run_pipeline_once(seed_mode=seed_mode)
        if result["status"] == "failed":
            return 1
        time.sleep(loop_interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
