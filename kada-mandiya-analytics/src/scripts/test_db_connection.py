from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy.exc import DBAPIError, OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_settings  # noqa: E402
from src.db.engine import ensure_database_exists, get_engine  # noqa: E402
from src.db.healthcheck import run_healthcheck  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="WARNING")


def _friendly_hint(exc: BaseException) -> str:
    msg = str(getattr(exc, "orig", exc))
    upper = msg.upper()

    if "LOGIN FAILED" in upper or "28000" in upper:
        return (
            "Hint: check DB_USER/DB_PASSWORD and that SQL Server is in Mixed Mode "
            "(SQL Server + Windows Authentication)."
        )

    if "CREATE DATABASE PERMISSION DENIED" in upper:
        return (
            "Hint: your login lacks permission to create databases. Create DB_NAME in SSMS, "
            "or use a login with CREATE DATABASE rights."
        )

    if "CANNOT OPEN DATABASE" in upper or "DOES NOT EXIST" in upper:
        return "Hint: DB_NAME may not exist yet. Re-run without --no-create-db or create it in SSMS."

    if (
        "08001" in upper
        or "SERVER WAS NOT FOUND" in upper
        or "NAMED PIPES PROVIDER" in upper
    ):
        return "Hint: check SQL Server is running, TCP/IP is enabled, and DB_HOST/DB_PORT are correct."

    if "CERTIFICATE" in upper or "SSL" in upper or "TLS" in upper:
        return "Hint: for local dev, keep DB_TRUST_CERT=yes (TrustServerCertificate)."

    return "Hint: verify SQL Server connectivity and credentials."


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
def _healthcheck_with_retry(settings) -> dict:
    engine = get_engine(settings)
    return run_healthcheck(engine)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test SQL Server connectivity for analytics."
    )
    parser.add_argument(
        "--no-create-db",
        action="store_true",
        help="Do not create DB_NAME if it does not exist.",
    )
    args = parser.parse_args()

    try:
        settings = load_settings()
    except Exception as exc:
        print(f"CONFIG ERROR: {exc}")
        return 2

    print(
        f"Connecting to SQL Server {settings.db_host}:{settings.db_port} "
        f"(database: {settings.db_name})"
    )

    try:
        if args.no_create_db:
            print("Skipping database creation check (--no-create-db).")
        else:
            result = ensure_database_exists(settings)
            if result.created:
                print(f"Database created: {settings.db_name}")
            else:
                print(f"Database exists: {settings.db_name}")

        row = _healthcheck_with_retry(settings)
        print(f"SUCCESS: ok={row.get('ok')} now={row.get('now')}")
        return 0

    except (OperationalError, DBAPIError) as exc:
        logger.debug("DB error: {}", exc)
        print("FAILURE: could not connect to SQL Server.")
        print(_friendly_hint(exc))
        print(f"Details: {getattr(exc, 'orig', exc)}")
        return 1
    except Exception as exc:
        logger.debug("Unexpected error: {}", exc)
        print("FAILURE: unexpected error during DB check.")
        print(f"Details: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
