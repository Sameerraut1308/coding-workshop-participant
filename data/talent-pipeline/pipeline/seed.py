"""Seed step: load the source CSVs into PostgreSQL when the source tables are empty.

In the cloud the VDI cannot reach Aurora (its firewall is managed by the workshop organizers),
so ``load.sh`` cannot be run there. The EKS job can, so it seeds the database itself from the
CSVs and ``schema.sql`` uploaded to S3 by ``scripts/upload_source.py``.

Safe to re-run - what happens depends on the state of the seven source tables:

* every table has rows          -> nothing is done ("skip");
* no table exists               -> ``schema.sql`` is run, then every CSV is loaded ("create_and_load");
* every table exists, all empty -> every CSV is loaded ("load");
* anything in between           -> the step fails with a clear error instead of guessing.

Everything runs in one transaction under an advisory lock, so a failure (or a second job
starting at the same time) never leaves a half-loaded database.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

from .config import SOURCE_TABLES, Settings
from .observability import RunTracker, get_logger, with_retry
from .storage import Storage, open_storage

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

log = get_logger(__name__)

SCHEMA_FILE = "schema.sql"
# Arbitrary constant key for pg_advisory_xact_lock, so two seeding jobs never load at the same time.
SEED_LOCK_ID = 7_412_031


def plan_seed(states: dict[str, str]) -> str:
    """Decide what to do from each table's state ("missing", "empty" or "loaded")."""
    values = set(states.values())
    if values == {"loaded"}:
        return "skip"
    if values == {"missing"}:
        return "create_and_load"
    if values == {"empty"}:
        return "load"
    detail = ", ".join(f"{t}={s}" for t, s in states.items())
    raise ValueError(f"source tables are partly loaded ({detail}); fix the database by hand, then re-run")


def read_seed_files(store: Storage, with_schema: bool) -> dict[str, bytes]:
    """Read ``<table>.csv`` for every source table (plus ``schema.sql`` if needed); fail if any is missing."""
    wanted = [f"{t}.csv" for t in SOURCE_TABLES] + ([SCHEMA_FILE] if with_schema else [])
    missing = [name for name in wanted if not store.exists(name)]
    if missing:
        raise FileNotFoundError(f"seed files missing under {store.uri}: {missing} (run scripts/upload_source.py)")
    return {name: store.get_bytes(name) for name in wanted}


def _table_state(conn: "Connection", table: str) -> str:
    from sqlalchemy import text

    if conn.execute(text("SELECT to_regclass(:t)"), {"t": table}).scalar() is None:
        return "missing"
    # table comes from the fixed SOURCE_TABLES list, not user input
    has_rows = conn.execute(text(f'SELECT EXISTS (SELECT 1 FROM "{table}")')).scalar()  # nosec B608
    return "loaded" if has_rows else "empty"


def seed_database(engine: object, store: Storage, schema: str = "public") -> dict[str, object]:
    """Seed the source tables in ``schema`` if they are empty; returns what was done."""
    from sqlalchemy import text

    if not schema.isidentifier():
        raise ValueError(f"invalid schema name: {schema}")

    with engine.begin() as conn:  # type: ignore[attr-defined]
        conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": SEED_LOCK_ID})
        conn.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        states = {t: _table_state(conn, t) for t in SOURCE_TABLES}
        action = plan_seed(states)
        if action == "skip":
            return {"action": action, "rows_loaded": {}}

        files = read_seed_files(store, with_schema=action == "create_and_load")
        if action == "create_and_load":
            conn.exec_driver_sql(files[SCHEMA_FILE].decode("utf-8"))
        cursor = conn.connection.cursor()
        rows: dict[str, int] = {}
        for table in SOURCE_TABLES:  # foreign-key order
            cursor.copy_expert(
                f'COPY "{table}" FROM STDIN WITH (FORMAT csv, HEADER true)', io.BytesIO(files[f"{table}.csv"])
            )
            rows[table] = cursor.rowcount
        return {"action": action, "rows_loaded": rows}


def run_seed(settings: Settings, tracker: RunTracker, schema: str = "public") -> None:
    """Run the seed step; does nothing unless a PostgreSQL source and a seed location are configured."""
    if settings.source_mode != "postgres" or not settings.seed_source_uri:
        log.info("seed step not configured; skipped", extra={"seed_source_uri": settings.seed_source_uri})
        return

    from sqlalchemy.exc import OperationalError

    from .db import check_connection, make_engine

    with tracker.step("seed.source_tables") as m:
        engine = make_engine(settings)
        try:
            check_connection(engine, settings)
            store = open_storage(settings.seed_source_uri, settings)
            # Only connection problems are retried; the transaction rolls back, so a retry starts clean.
            result = with_retry("postgres.seed", settings.max_retries, settings.retry_base_delay,
                                retry_on=(OperationalError,))(seed_database)(engine, store, schema)
        finally:
            engine.dispose()
        m["source"] = settings.seed_source_uri
        m["action"] = result["action"]
        m["rows_loaded"] = result["rows_loaded"]
        if result["action"] == "skip":
            log.info("source tables already loaded; nothing to seed")
