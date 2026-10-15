"""End-to-end smoke tests: full job run on the synthetic dataset (offline)."""

from __future__ import annotations

import glob
import json
from pathlib import Path

import pandas as pd
import pytest

import job
from pipeline.config import load_settings
from pipeline.reconcile import reconcile
from pipeline.storage import LocalStorage


def _manifest(lake: Path) -> dict:
    # Newest by mtime: run_ids only have second resolution plus a random suffix, so two runs in
    # the same second do not sort chronologically by name.
    newest = max(glob.glob(str(lake / "_runs/*/*.json")), key=lambda p: Path(p).stat().st_mtime_ns)
    return json.loads(Path(newest).read_text())


def _business_snapshot(lake: Path) -> dict[str, pd.DataFrame]:
    out = {}
    for f in sorted(glob.glob(str(lake / "**/*.parquet"), recursive=True)):
        df = pd.read_parquet(f)
        out[f] = df[[c for c in df.columns if not c.startswith("_")]]
    return out


def test_full_run_succeeds_and_reconciles(offline_env: Path) -> None:
    assert job.main([]) == 0
    m = _manifest(offline_env)
    assert m["status"] == "success"
    assert m["reconciliation"] and all(c["passed"] for c in m["reconciliation"])
    steps = {s["name"]: s["metrics"] for s in m["steps"]}

    assert steps["silver.employees"]["rows_valid"] == 4 and steps["silver.employees"]["rows_rejected"] == 2
    assert steps["silver.performance_reviews"]["rows_rejected"] == 1
    la = steps["silver.learning_activity"]
    assert (la["lines_malformed"], la["records_parsed"], la["schema_drift_notes_to_text"]) == (1, 9, 1)
    assert (la["rows_valid"], la["rows_rejected"], la["exact_duplicates_dropped"],
            la["quarantine_rows"]) == (5, 3, 1, 4)
    assert steps["bronze.learning_activity"]["files_empty"] == 1
    for table in ["gold_employee_performance", "gold_promotion_candidates", "gold_skill_distribution"]:
        assert steps[f"gold.{table}"]["rows_written"] > 0


def test_quarantine_contains_reasons(offline_env: Path) -> None:
    job.main([])
    q = pd.read_parquet(offline_env / "quarantine/learning_activity/run_date=2025-12-31/rejected.parquet")
    reasons = " ".join(q["_dq_reasons"])
    for expected in ["malformed_json", "missing_required:employee_id", "invalid_type:event_date",
                     "rule:score_between_0_100"]:
        assert expected in reasons
    emp = pd.read_parquet(offline_env / "quarantine/employees/run_date=2025-12-31/rejected.parquet")
    assert set(emp["employee_id"]) == {"5", "6"}


def test_rerun_is_idempotent(offline_env: Path) -> None:
    assert job.main([]) == 0
    first = _business_snapshot(offline_env)
    assert job.main([]) == 0
    second = _business_snapshot(offline_env)
    assert first.keys() == second.keys()
    for key in first:
        pd.testing.assert_frame_equal(first[key], second[key])
    m = _manifest(offline_env)
    bronze_la = next(s for s in m["steps"] if s["name"] == "bronze.learning_activity")["metrics"]
    assert bronze_la["files_copied"] == 0 and bronze_la["files_unchanged_skipped"] == 3


def test_steps_can_run_separately(offline_env: Path) -> None:
    for step in ["bronze", "silver", "gold", "reconcile"]:
        assert job.main(["--steps", step]) == 0


def test_bronze_matches_source_bytes(offline_env: Path, dataset_dir: Path) -> None:
    job.main(["--steps", "bronze"])
    for src in (dataset_dir / "learning_activity").rglob("*.jsonl"):
        rel = src.relative_to(dataset_dir / "learning_activity")
        assert (offline_env / "bronze/learning_activity" / rel).read_bytes() == src.read_bytes()


def test_reconciliation_detects_lost_rows(offline_env: Path) -> None:
    job.main([])
    store = LocalStorage(str(offline_env))
    silver = store.read_parquet("silver/employees/")
    store.replace_parquet(silver.iloc[:-1], "silver/employees")  # simulate a lost row
    failed = [c for c in reconcile(load_settings(), store) if not c["passed"]]
    assert any("employees" in c["check"] for c in failed)
    assert job.main(["--steps", "reconcile"]) == 1


def test_empty_source_table_fails_fast(offline_env: Path, dataset_dir: Path) -> None:
    (dataset_dir / "teams.csv").write_text("team_id,name,department\n")
    assert job.main(["--steps", "bronze"]) == 1
    m = _manifest(offline_env)
    assert m["status"] == "failed"
    assert next(s for s in m["steps"] if s["name"] == "bronze.teams")["status"] == "failed"


def test_missing_learning_files_fail(offline_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    empty = tmp_path / "nothing"
    empty.mkdir()
    monkeypatch.setenv("LEARNING_ACTIVITY_URI", str(empty))
    assert job.main(["--steps", "bronze"]) == 1


def test_bad_step_name_rejected() -> None:
    with pytest.raises(SystemExit):
        job.parse_args(["--steps", "platinum"])
