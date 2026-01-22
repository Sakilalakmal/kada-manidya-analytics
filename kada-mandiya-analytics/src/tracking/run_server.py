from __future__ import annotations

import argparse
import os

import uvicorn


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Kada Mandiya tracking ingest API (FastAPI).")
    p.add_argument("--host", default=os.getenv("TRACKING_API_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("TRACKING_API_PORT", "9000")))
    p.add_argument("--reload", action="store_true", help="Enable Uvicorn reload (dev only).")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    uvicorn.run(
        "src.tracking.api:app",
        host=str(args.host),
        port=int(args.port),
        reload=bool(args.reload),
        log_level="info",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
