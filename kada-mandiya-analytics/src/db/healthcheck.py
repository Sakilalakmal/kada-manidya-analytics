from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def run_healthcheck(engine: Engine) -> dict:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT 1 as ok, GETDATE() as [now];")).mappings().one()
        return dict(row)
