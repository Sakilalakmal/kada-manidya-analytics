from __future__ import annotations

import importlib
from datetime import datetime

from src.config import load_settings


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _call_etl_main(module_name: str) -> None:
    print(f"[{_ts()}] START {module_name}")
    mod = importlib.import_module(module_name)
    main = getattr(mod, "main", None)
    if not callable(main):
        raise RuntimeError(f"{module_name}.main() not found")
    rc = int(main())
    if rc != 0:
        raise RuntimeError(f"{module_name}.main() returned {rc}")
    print(f"[{_ts()}] END   {module_name}")


def run_pipeline(include_seed: bool = True) -> None:
    settings = load_settings()
    enable_silver = bool(getattr(settings, "etl_enable_silver", True))
    enable_gold = bool(getattr(settings, "etl_enable_gold", True))

    if include_seed:
        _call_etl_main("src.etl.02b_seed_business_events")
    else:
        print(f"[{_ts()}] SKIP  src.etl.02b_seed_business_events (--no-seed)")

    if enable_silver:
        _call_etl_main("src.etl.03_build_silver")
    else:
        print(f"[{_ts()}] SKIP  src.etl.03_build_silver (ETL_ENABLE_SILVER=no)")

    if enable_gold:
        _call_etl_main("src.etl.04_build_gold")
    else:
        print(f"[{_ts()}] SKIP  src.etl.04_build_gold (ETL_ENABLE_GOLD=no)")

