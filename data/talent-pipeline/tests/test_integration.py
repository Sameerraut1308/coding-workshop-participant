"""Integration tests against the real PostgreSQL and S3 (LocalStack).

Skipped unless ``RUN_INTEGRATION=1``. They use the normal workshop environment
variables (POSTGRES_*, IS_LOCAL, AWS_ENDPOINT_URL), and need the dataset loaded
with ``load.sh`` first.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1 to run")


@pytest.fixture()
def settings(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("SOURCE_MODE", "postgres")
    monkeypatch.setenv("DATA_LAKE_URI", os.getenv("INTEGRATION_LAKE_URI", "s3://talent-lake-integration-test"))
    monkeypatch.setenv("WRITE_GOLD_TO_POSTGRES", "true")
    monkeypatch.setenv("RETRY_BASE_DELAY", "0.2")
    from pipeline.config import load_settings

    return load_settings()


def test_postgres_source_tables_readable(settings) -> None:  # type: ignore[no-untyped-def]
    from pipeline.config import SOURCE_TABLES
    from pipeline.db import check_connection, make_engine, read_table

    engine = make_engine(settings)
    check_connection(engine, settings)
    for table in SOURCE_TABLES:
        assert len(read_table(engine, table, settings)) > 0, table


def test_s3_read_write(settings) -> None:  # type: ignore[no-untyped-def]
    import pandas as pd

    from pipeline.storage import open_lake

    lake = open_lake(settings)
    df = pd.DataFrame({"x": [1, 2, 3]})
    lake.replace_parquet(df, "_integration/check")
    pd.testing.assert_frame_equal(lake.read_parquet("_integration/check"), df)
    assert lake.delete_prefix("_integration/") == 1


def test_full_pipeline_publishes_gold_to_s3_and_postgres(settings) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    import job
    from pipeline.db import make_engine
    from pipeline.gold import BUSINESS_QUESTIONS
    from pipeline.storage import open_lake

    assert job.main([]) == 0
    lake = open_lake(settings)
    with make_engine(settings).connect() as conn:
        for table in BUSINESS_QUESTIONS:
            s3_rows = len(lake.read_parquet(f"gold/{table}/"))
            pg_rows = conn.execute(
                # table names come from the fixed BUSINESS_QUESTIONS list, not user input
                text(f'SELECT COUNT(*) FROM "{settings.gold_schema}"."{table}"')  # nosec B608
            ).scalar()
            assert s3_rows == pg_rows > 0, table


def test_pipeline_reads_raw_learning_events_from_s3(  # type: ignore[no-untyped-def]
        settings, monkeypatch: pytest.MonkeyPatch) -> None:
    """Cloud layout: raw events uploaded to S3 by scripts/upload_source.py, then read by Bronze from there."""
    import json
    # subprocess runs scripts/upload_source.py with the current interpreter (fixed argv, no shell).
    import subprocess  # nosec B404
    import sys
    from pathlib import Path

    import job
    from pipeline.config import load_settings
    from pipeline.storage import open_lake

    root = Path(__file__).resolve().parents[1]
    landing = f"{settings.lake_uri}/landing/learning_activity"
    upload_cmd = [sys.executable, str(root / "scripts/upload_source.py"), "--dest", landing]
    out = subprocess.run(upload_cmd, check=True, capture_output=True, text=True).stdout  # nosec B603
    summary = json.loads(out)
    local_files = len(list(Path(settings.learning_activity_uri).rglob("*.jsonl")))
    assert summary["files_uploaded"] + summary["files_unchanged_skipped"] == local_files > 0

    monkeypatch.setenv("LEARNING_ACTIVITY_URI", landing)
    s3_settings = load_settings()
    assert job.main([]) == 0
    runs = open_lake(s3_settings).list_keys(f"_runs/run_date={s3_settings.run_date}/")
    manifest = max((open_lake(s3_settings).read_json(k) for k in runs), key=lambda m: m["ended_at"])
    bronze = next(s for s in manifest["steps"] if s["name"] == "bronze.learning_activity")["metrics"]
    assert bronze["source"] == landing
    assert bronze["files_copied"] + bronze["files_unchanged_skipped"] == local_files
    assert all(c["passed"] for c in manifest["reconciliation"])
