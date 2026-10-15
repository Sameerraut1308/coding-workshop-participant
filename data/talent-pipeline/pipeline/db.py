"""PostgreSQL access (source extraction and Gold publishing)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from .config import Settings
from .observability import get_logger, with_retry

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

log = get_logger(__name__)


def make_engine(settings: Settings) -> "Engine":
    """Create a SQLAlchemy engine; SSL is required in the cloud (Aurora), not locally."""
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    url = URL.create(
        "postgresql+psycopg2",
        username=settings.pg_user or None,
        password=settings.pg_pass or None,
        host=settings.pg_host,
        port=settings.pg_port,
        database=settings.pg_name,
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"sslmode": settings.pg_sslmode, "connect_timeout": 10},
    )


def check_connection(engine: "Engine", settings: Settings) -> None:
    """Fail fast (after retries) if PostgreSQL is unreachable."""
    from sqlalchemy import text

    def _ping() -> None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    with_retry("postgres.connect", settings.max_retries, settings.retry_base_delay)(_ping)()


def read_table(engine: "Engine", table: str, settings: Settings) -> pd.DataFrame:
    """Read a whole source table, keeping every value as text (raw fidelity for Bronze)."""
    from sqlalchemy import text

    if not table.isidentifier():
        raise ValueError(f"invalid table name: {table}")

    def _read() -> pd.DataFrame:
        with engine.connect() as conn:
            # table is checked with str.isidentifier() above, so it cannot inject SQL
            df = pd.read_sql(text(f'SELECT * FROM "{table}"'), conn)  # nosec B608
        return df.astype("string")

    return with_retry(f"postgres.read.{table}", settings.max_retries, settings.retry_base_delay)(_read)()


def write_table(engine: "Engine", df: pd.DataFrame, table: str, schema: str, settings: Settings) -> None:
    """Replace a table (idempotent): drop-and-recreate inside one transaction."""
    from sqlalchemy import text

    if not (schema.isidentifier() and table.isidentifier()):
        raise ValueError(f"invalid identifier: {schema}.{table}")

    def _write() -> None:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            df.to_sql(table, conn, schema=schema, if_exists="replace", index=False, method="multi", chunksize=1000)

    with_retry(f"postgres.write.{schema}.{table}", settings.max_retries, settings.retry_base_delay)(_write)()
