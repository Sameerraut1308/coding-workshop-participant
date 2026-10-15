"""PostgreSQL helpers work without pandas' SQL support.

pandas ignores an SQLAlchemy older than its own minimum (the EKS job image has 2.0.22, pandas 3
needs 2.0.36) and then rejects SQLAlchemy queries. ``pandas_without_sqlalchemy`` recreates that
situation, so these tests fail on code that still goes through ``pd.read_sql`` / ``to_sql``.
The write tests need ``RUN_INTEGRATION=1`` and the local PostgreSQL; they use a throwaway schema.
"""

from __future__ import annotations

import os
import uuid

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import BigInteger, Boolean, Date, DateTime, Double, Text, create_engine, text

from pipeline.config import load_settings
from pipeline.db import _sql_value, read_table, sql_type, write_table

integration = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 to run")


@pytest.fixture()
def pandas_without_sqlalchemy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make pandas behave as if SQLAlchemy were too old, as in the EKS job container."""
    import pandas.io.sql as pandas_sql

    real = pandas_sql.import_optional_dependency

    def _fake(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        return None if name == "sqlalchemy" else real(name, *args, **kwargs)

    monkeypatch.setattr(pandas_sql, "import_optional_dependency", _fake)
    engine = create_engine("sqlite://")
    with engine.connect() as conn:  # the failure seen on EKS, reproduced
        with pytest.warns(UserWarning, match="pandas only supports SQLAlchemy"), \
                pytest.raises(TypeError, match="Query must be a string unless using sqlalchemy"):
            pd.read_sql(text("SELECT 1"), conn)
    engine.dispose()


@pytest.fixture()
def sqlite_engine():  # type: ignore[no-untyped-def]
    engine = create_engine("sqlite://")
    yield engine
    engine.dispose()


def test_read_table_without_pandas_sql_support(pandas_without_sqlalchemy, sqlite_engine) -> None:  # type: ignore[no-untyped-def]
    engine = sqlite_engine
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE teams (team_id INTEGER, name TEXT, budget NUMERIC, started DATE)"))
        conn.execute(text("INSERT INTO teams VALUES (1, 'Data', 1.5, '2025-01-31'), (2, NULL, NULL, NULL)"))
    df = read_table(engine, "teams", load_settings())
    assert list(df.columns) == ["team_id", "name", "budget", "started"]
    assert all(str(t) == "string" for t in df.dtypes)  # Bronze keeps every value as text
    assert df.iloc[0].tolist() == ["1", "Data", "1.5", "2025-01-31"]
    assert df.iloc[1].isna().tolist() == [False, True, True, True]
    with pytest.raises(ValueError, match="invalid table name"):
        read_table(engine, "teams; DROP TABLE teams", load_settings())


def test_sql_type_matches_what_to_sql_picked() -> None:
    cases = {
        "int64": (pd.Series([1, 2]), BigInteger),
        "float64": (pd.Series([1.5, np.nan]), Double),
        "bool": (pd.Series([True, False]), Boolean),
        "object bools with NULL (as _postgres_ready leaves them)": (pd.Series([True, None], dtype=object), Boolean),
        "object strings": (pd.Series(["a", None], dtype=object), Text),
        "all NULL": (pd.Series([None, None], dtype=object), Text),
        "datetime64": (pd.Series(pd.to_datetime(["2025-01-01", None])), DateTime),
        "dates": (pd.Series([pd.Timestamp("2025-01-01").date(), None], dtype=object), Date),
        "string dtype": (pd.Series(["a", None], dtype="string"), Text),
    }
    for label, (series, expected) in cases.items():
        assert isinstance(sql_type(series), expected), label
    assert sql_type(pd.Series(pd.to_datetime(["2025-01-01"]).tz_localize("UTC"))).timezone is True
    assert sql_type(pd.Series(pd.to_datetime(["2025-01-01"]))).timezone is False


def test_sql_value_gives_the_driver_plain_python() -> None:
    assert _sql_value(np.int64(3)) == 3 and type(_sql_value(np.int64(3))) is int
    assert type(_sql_value(np.float64(1.5))) is float and type(_sql_value(np.bool_(True))) is bool
    assert [_sql_value(v) for v in (None, np.nan, pd.NaT, pd.NA)] == [None, None, None, None]
    ts = pd.Timestamp("2025-01-01 10:00")
    assert _sql_value(ts) is ts and _sql_value("x") == "x"


# ---- integration tests (local PostgreSQL) -------------------------------------------------------

@pytest.fixture()
def pg_schema(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Engine on the local PostgreSQL plus a schema name that is dropped afterwards."""
    from pipeline.db import make_engine

    monkeypatch.setenv("RETRY_BASE_DELAY", "0")
    settings = load_settings()
    engine = make_engine(settings)
    schema = f"db_test_{uuid.uuid4().hex[:8]}"
    try:
        yield engine, schema, settings
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        engine.dispose()


def _columns(engine, schema: str, table: str) -> dict[str, str]:  # type: ignore[no-untyped-def]
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns "
                                 "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"),
                            {"s": schema, "t": table})
        return dict(rows.fetchall())


@integration
def test_write_table_types_values_and_replace(pandas_without_sqlalchemy, pg_schema) -> None:  # type: ignore[no-untyped-def]
    engine, schema, settings = pg_schema
    df = pd.DataFrame({
        "employee_id": np.array([1, 2], dtype="int64"),
        "score": [3.5, np.nan],
        "is_ready": pd.Series([True, None], dtype=object),
        "reason": pd.Series(["low engagement", None], dtype=object),
        "as_of": pd.to_datetime(["2025-12-31", None]),
    })
    write_table(engine, df, "gold_example", schema, settings)
    assert _columns(engine, schema, "gold_example") == {
        "employee_id": "bigint", "score": "double precision", "is_ready": "boolean",
        "reason": "text", "as_of": "timestamp without time zone",
    }
    with engine.connect() as conn:
        rows = conn.execute(text(f'SELECT * FROM "{schema}".gold_example ORDER BY employee_id')).fetchall()
    assert [tuple(r) for r in rows] == [
        (1, 3.5, True, "low engagement", pd.Timestamp("2025-12-31").to_pydatetime()),
        (2, None, None, None, None),
    ]

    write_table(engine, df.head(1), "gold_example", schema, settings)  # re-run replaces, never appends
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT COUNT(*) FROM "{schema}".gold_example')).scalar() == 1

    write_table(engine, df.head(0), "gold_empty", schema, settings)  # empty frame: table with columns, no rows
    assert list(_columns(engine, schema, "gold_empty")) == list(df.columns)


@integration
def test_write_table_failure_keeps_the_previous_table(pg_schema) -> None:  # type: ignore[no-untyped-def]
    """Drop, create and insert are one transaction: a failing insert leaves the old table in place."""
    engine, schema, settings = pg_schema
    write_table(engine, pd.DataFrame({"x": [7]}), "gold_example", schema, settings)
    bad = pd.DataFrame({"x": pd.Series([1, 2**70], dtype=object)})  # second value does not fit in bigint
    with pytest.raises(Exception, match="out of range"):
        write_table(engine, bad, "gold_example", schema, settings)
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT x FROM "{schema}".gold_example')).scalars().all() == [7]
