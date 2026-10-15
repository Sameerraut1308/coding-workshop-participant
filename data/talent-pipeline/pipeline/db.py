"""PostgreSQL access (source extraction and Gold publishing).

Only plain SQLAlchemy Core is used here, never ``pd.read_sql`` / ``DataFrame.to_sql``: pandas ignores
any SQLAlchemy older than its own minimum (pandas 3 needs >= 2.0.36) and then rejects SQLAlchemy
queries outright. The EKS job image ships an older SQLAlchemy, so the pandas helpers fail there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from .config import Settings
from .observability import get_logger, with_retry

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.types import TypeEngine

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
            result = conn.execute(text(f'SELECT * FROM "{table}"'))  # nosec B608
            columns = list(result.keys())
            rows = [tuple(row) for row in result]
        # coerce_float turns NUMERIC (Decimal) values into floats, as pd.read_sql does
        return pd.DataFrame.from_records(rows, columns=columns, coerce_float=True).astype("string")

    return with_retry(f"postgres.read.{table}", settings.max_retries, settings.retry_base_delay)(_read)()


def sql_type(series: pd.Series) -> "TypeEngine[Any]":
    """PostgreSQL column type for a DataFrame column (the same types ``DataFrame.to_sql`` picks)."""
    from sqlalchemy import BigInteger, Boolean, Date, DateTime, Double, Text

    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return Boolean()
    if pd.api.types.is_integer_dtype(dtype):
        return BigInteger()
    if pd.api.types.is_float_dtype(dtype):
        return Double()
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime(timezone=getattr(dtype, "tz", None) is not None)
    inferred = pd.api.types.infer_dtype(series, skipna=True)  # object columns: look at the values
    if inferred == "boolean":
        return Boolean()
    if inferred == "integer":
        return BigInteger()
    if inferred in ("floating", "mixed-integer-float", "decimal"):
        return Double()
    if inferred == "date":
        return Date()
    if inferred == "datetime":
        return DateTime()
    return Text()


def _sql_value(value: Any) -> Any:
    """Python value for the database driver: NaN / NaT / pd.NA become NULL, numpy scalars become Python."""
    if value is None or (pd.api.types.is_scalar(value) and pd.isna(value)):
        return None
    return value.item() if hasattr(value, "item") and not isinstance(value, pd.Timestamp) else value


def write_table(engine: "Engine", df: pd.DataFrame, table: str, schema: str, settings: Settings) -> None:
    """Replace a table (idempotent): drop, create and insert inside one transaction."""
    from sqlalchemy import Column, MetaData, Table, text

    if not (schema.isidentifier() and table.isidentifier()):
        raise ValueError(f"invalid identifier: {schema}.{table}")

    names = [str(c) for c in df.columns]
    types = [sql_type(df[c]) for c in df.columns]
    rows = [dict(zip(names, map(_sql_value, values))) for values in df.itertuples(index=False, name=None)]

    def _write() -> None:
        target = Table(table, MetaData(), *(Column(n, t) for n, t in zip(names, types)), schema=schema)
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            target.drop(conn, checkfirst=True)
            target.create(conn)
            if rows:
                conn.execute(target.insert(), rows)

    with_retry(f"postgres.write.{schema}.{table}", settings.max_retries, settings.retry_base_delay)(_write)()
