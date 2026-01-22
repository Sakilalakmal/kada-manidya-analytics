from __future__ import annotations

import contextlib
import importlib
import sys
from datetime import datetime

from sqlalchemy import text

from src.config import load_settings
from src.db.engine import get_engine
from src.ops.run_logger import fail_stale_running_runs, finish_run, start_run


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@contextlib.contextmanager
def _clean_argv(module_name: str):
    prev = sys.argv
    try:
        sys.argv = [module_name]
        yield
    finally:
        sys.argv = prev


def _call_etl_main(module_name: str) -> None:
    print(f"[{_ts()}] START {module_name}")
    mod = importlib.import_module(module_name)
    main = getattr(mod, "main", None)
    if not callable(main):
        raise RuntimeError(f"{module_name}.main() not found")
    with _clean_argv(module_name):
        rc = int(main())
    if rc != 0:
        raise RuntimeError(f"{module_name}.main() returned {rc}")
    print(f"[{_ts()}] END   {module_name}")


def _latest_ops_run(*, engine, run_type: str) -> dict[str, object] | None:
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT TOP 1
                    run_id,
                    run_type,
                    status,
                    rows_inserted,
                    started_at,
                    finished_at
                FROM ops.etl_runs
                WHERE run_type = :run_type
                ORDER BY started_at DESC;
                """
            ),
            {"run_type": run_type},
        ).mappings().first()
        return None if row is None else dict(row)


_STEP_RUN_TYPES: dict[str, str] = {
    "src.etl.02b_seed_business_events": "seed_business_events",
    "src.etl.02c_seed_behavior_events": "seed_behavior_events",
    "src.etl.03_build_silver": "build_silver",
    "src.etl.04_build_gold": "build_gold",
}


def run_pipeline(
    *, run_type: str, seed_mode: str = "business", include_seed: bool | None = None
) -> None:
    settings = load_settings()
    enable_silver = bool(getattr(settings, "etl_enable_silver", True))
    enable_gold = bool(getattr(settings, "etl_enable_gold", True))
    engine = get_engine(settings)

    fail_stale_running_runs(older_than_minutes=10)
    run_id = start_run(run_type)

    status = "failed"
    error_message: str | None = None
    total_rows = 0
    try:
        if include_seed is not None:
            seed_mode = "business" if bool(include_seed) else "none"

        seed_mode_norm = str(seed_mode or "business").strip().lower()
        if seed_mode_norm not in {"none", "business", "all"}:
            raise ValueError("seed_mode must be one of: none, business, all")

        steps: list[str] = []
        if seed_mode_norm in {"business", "all"}:
            steps.append("src.etl.02b_seed_business_events")
        if seed_mode_norm == "all":
            steps.append("src.etl.02c_seed_behavior_events")

        if enable_silver:
            steps.append("src.etl.03_build_silver")
        else:
            print(f"[{_ts()}] SKIP  src.etl.03_build_silver (ETL_ENABLE_SILVER=no)")

        if enable_gold:
            steps.append("src.etl.04_build_gold")
        else:
            print(f"[{_ts()}] SKIP  src.etl.04_build_gold (ETL_ENABLE_GOLD=no)")

        if seed_mode_norm == "none":
            print(f"[{_ts()}] SKIP  src.etl.02b_seed_business_events (--no-seed)")
            print(f"[{_ts()}] SKIP  src.etl.02c_seed_behavior_events (--no-seed)")

        for module_name in steps:
            _call_etl_main(module_name)
            run_type_name = _STEP_RUN_TYPES.get(module_name)
            if run_type_name:
                latest = _latest_ops_run(engine=engine, run_type=run_type_name)
                if latest:
                    rows = int(latest.get("rows_inserted") or 0)
                    total_rows += max(0, rows)
                    print(
                        f"[{_ts()}] STATS {module_name} rows={rows} status={latest.get('status')} run_id={latest.get('run_id')}"
                    )

        status = "success"
    except BaseException as exc:
        error_message = str(exc)
        raise
    finally:
        try:
            finish_run(run_id, status, rows_inserted=total_rows, error_message=error_message)
        except Exception as finish_exc:
            print(
                f"[{_ts()}] WARN  failed to finalize ops.etl_runs for run_id={run_id}: {finish_exc}",
                file=sys.stderr,
            )
