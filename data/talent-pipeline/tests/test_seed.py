"""Seed step and source upload: unit tests (no database) plus PostgreSQL integration tests.

The integration tests need ``RUN_INTEGRATION=1`` and the local PostgreSQL. They seed a throwaway
schema (dropped afterwards), so the real ``public`` source tables are never modified.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest

import job
from pipeline.config import SOURCE_TABLES, load_settings
from pipeline.observability import RunTracker
from pipeline.seed import SCHEMA_FILE, plan_seed, read_seed_files, run_seed, seed_database
from pipeline.storage import LocalStorage

integration = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 to run")

REAL_DATASET = Path(os.getenv("SOURCE_DIR", Path.home() / "data-engineering-performance-talent-management"))


def _tracker() -> RunTracker:
    return RunTracker("test-run", "2025-12-31", "test", {})


# ---- unit tests ---------------------------------------------------------------------------------

def test_plan_seed_decisions() -> None:
    assert plan_seed({"a": "loaded", "b": "loaded"}) == "skip"
    assert plan_seed({"a": "missing", "b": "missing"}) == "create_and_load"
    assert plan_seed({"a": "empty", "b": "empty"}) == "load"
    for mixed in ({"a": "loaded", "b": "empty"}, {"a": "missing", "b": "empty"}, {"a": "loaded", "b": "missing"}):
        with pytest.raises(ValueError, match="partly loaded"):
            plan_seed(mixed)


def test_read_seed_files_reports_every_missing_file(dataset_dir: Path) -> None:
    store = LocalStorage(str(dataset_dir))
    assert set(read_seed_files(store, with_schema=False)) == {f"{t}.csv" for t in SOURCE_TABLES}
    with pytest.raises(FileNotFoundError, match=SCHEMA_FILE):
        read_seed_files(store, with_schema=True)
    (dataset_dir / "teams.csv").unlink()
    with pytest.raises(FileNotFoundError, match="teams.csv"):
        read_seed_files(store, with_schema=False)


def test_seed_is_off_unless_configured(offline_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Locally (no SEED_SOURCE_URI) and in CSV mode the step never touches PostgreSQL."""
    def _no_db(*_a, **_k):  # type: ignore[no-untyped-def]
        raise AssertionError("seed step must not connect to PostgreSQL")

    monkeypatch.setattr("pipeline.db.make_engine", _no_db)
    monkeypatch.setenv("SEED_SOURCE_URI", "/somewhere")  # csv mode: still off
    tracker = _tracker()
    run_seed(load_settings(), tracker)
    monkeypatch.setenv("SOURCE_MODE", "postgres")
    monkeypatch.delenv("SEED_SOURCE_URI")  # postgres mode, local default "": off
    run_seed(load_settings(), tracker)
    assert tracker.steps == []

    monkeypatch.setenv("SOURCE_MODE", "csv")  # full offline job: seed is in ALL_STEPS but records nothing
    assert job.main([]) == 0
    (manifest,) = [json.loads(p.read_text()) for p in offline_env.glob("_runs/*/*.json")]
    assert not [s for s in manifest["steps"] if s["name"].startswith("seed")]


def test_settings_seed_location_and_app_id_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("PARTICIPANT_ID", "DATA_LAKE_URI", "SEED_SOURCE_URI", "LEARNING_ACTIVITY_URI"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("APP_ID", "xy9")  # the EKS job gets APP_ID from the workshop, not PARTICIPANT_ID
    monkeypatch.setenv("IS_LOCAL", "true")
    assert load_settings("2025-01-01").seed_source_uri == ""
    monkeypatch.setenv("IS_LOCAL", "false")
    cloud = load_settings("2025-01-01")
    assert cloud.lake_uri == "s3://coding-workshop-xy9-talent-lake-xy9"
    assert cloud.seed_source_uri == "s3://coding-workshop-xy9-talent-lake-xy9/landing/source"
    assert cloud.masked()["seed_source_uri"] == cloud.seed_source_uri
    monkeypatch.setenv("PARTICIPANT_ID", "abc")  # PARTICIPANT_ID still wins when both are set
    assert load_settings("2025-01-01").lake_uri == "s3://coding-workshop-abc-talent-lake-abc"


def test_upload_source_uploads_tables_and_learning_files_once(
        offline_env: Path, dataset_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from scripts.upload_source import main as upload_main

    (dataset_dir / SCHEMA_FILE).write_text("-- schema\n")
    learning_dest, tables_dest = tmp_path / "landing_learning", tmp_path / "landing_source"
    argv = ["--source", str(dataset_dir / "learning_activity"), "--dest", str(learning_dest),
            "--tables-source", str(dataset_dir), "--tables-dest", str(tables_dest)]

    assert upload_main(argv) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["files_uploaded"] == 3 and first["source_tables"]["files_uploaded"] == len(SOURCE_TABLES) + 1
    assert sorted(p.name for p in tables_dest.iterdir()) == sorted([f"{t}.csv" for t in SOURCE_TABLES] + [SCHEMA_FILE])

    assert upload_main(argv) == 0  # re-run: nothing re-uploaded
    second = json.loads(capsys.readouterr().out)
    assert second["files_uploaded"] == 0 and second["files_unchanged_skipped"] == 3
    assert second["source_tables"]["files_uploaded"] == 0

    assert upload_main(argv + ["--skip-tables"]) == 0
    assert "source_tables" not in json.loads(capsys.readouterr().out)

    (dataset_dir / "talent_review.csv").unlink()
    with pytest.raises(FileNotFoundError, match="talent_review.csv"):
        upload_main(argv)


# ---- integration tests (local PostgreSQL) -------------------------------------------------------

@pytest.fixture()
def pg(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Engine on the local PostgreSQL plus a fresh schema that is dropped afterwards."""
    from sqlalchemy import text

    from pipeline.db import make_engine

    monkeypatch.setenv("SOURCE_MODE", "postgres")
    monkeypatch.setenv("RETRY_BASE_DELAY", "0")
    settings = load_settings()
    engine = make_engine(settings)
    schema = f"seed_test_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    try:
        yield engine, schema
    finally:
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        engine.dispose()


def _counts(engine, schema: str) -> dict[str, int]:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    with engine.connect() as conn:
        # schema and table names are generated by the test / fixed list, not user input
        return {t: conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{t}"')).scalar() for t in SOURCE_TABLES}  # nosec B608


@integration
def test_seed_creates_loads_then_skips(pg) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    engine, schema = pg
    store = LocalStorage(str(REAL_DATASET))
    expected = {t: len(pd.read_csv(REAL_DATASET / f"{t}.csv")) for t in SOURCE_TABLES}

    first = seed_database(engine, store, schema)
    assert first == {"action": "create_and_load", "rows_loaded": expected}
    assert _counts(engine, schema) == expected

    assert seed_database(engine, store, schema)["action"] == "skip"  # re-run: no duplicates
    assert _counts(engine, schema) == expected

    tables = ", ".join(f'"{schema}"."{t}"' for t in SOURCE_TABLES)
    with engine.begin() as conn:  # tables exist but are empty -> load into them
        conn.execute(text(f"TRUNCATE {tables}"))
    assert seed_database(engine, store, schema)["action"] == "load"
    assert _counts(engine, schema) == expected


@integration
def test_seed_refuses_partly_loaded_tables(pg) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    engine, schema = pg
    store = LocalStorage(str(REAL_DATASET))
    seed_database(engine, store, schema)
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE "{schema}"."talent_review"'))
    before = _counts(engine, schema)
    with pytest.raises(ValueError, match="talent_review=empty"):
        seed_database(engine, store, schema)
    assert _counts(engine, schema) == before


@integration
def test_seed_failure_leaves_nothing_behind(pg, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """A bad CSV in the last table rolls back everything, including the CREATE TABLEs."""
    from sqlalchemy import text

    engine, schema = pg
    broken = tmp_path / "broken"
    broken.mkdir()
    for name in [f"{t}.csv" for t in SOURCE_TABLES] + [SCHEMA_FILE]:
        (broken / name).write_bytes((REAL_DATASET / name).read_bytes())
    (broken / "talent_review.csv").write_text("employee_id,review_cycle,potential_rating,promotion_readiness,"
                                              "attrition_risk\n999999,2025-H1,Huge,Ready Now,Low\n")
    with pytest.raises(Exception):
        seed_database(engine, LocalStorage(str(broken)), schema)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regclass(:t)"), {"t": f"{schema}.teams"}).scalar() is None


@integration
def test_run_seed_skips_the_loaded_local_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Against the real public tables (already loaded with load.sh) the step records "skip" and changes nothing."""
    monkeypatch.setenv("SOURCE_MODE", "postgres")
    monkeypatch.setenv("SEED_SOURCE_URI", str(REAL_DATASET))
    tracker = _tracker()
    run_seed(load_settings(), tracker)
    (step,) = tracker.steps
    assert step.name == "seed.source_tables" and step.status == "success"
    assert step.metrics["action"] == "skip" and step.metrics["rows_loaded"] == {}
