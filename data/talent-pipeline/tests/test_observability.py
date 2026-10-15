"""Tests for retry/backoff, run tracking, structured logging and configuration."""

from __future__ import annotations

import json
import logging

import pytest

from pipeline import observability
from pipeline.config import load_settings
from pipeline.observability import JsonFormatter, RunTracker, with_retry


def test_retry_recovers_from_transient_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(observability.time, "sleep", sleeps.append)
    calls = {"n": 0}

    @with_retry("flaky", max_retries=3, base_delay=0.5)
    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("timeout")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3
    assert sleeps == [0.5, 1.0]  # exponential backoff


def test_retry_gives_up_and_reraises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(observability.time, "sleep", lambda _: None)
    calls = {"n": 0}

    @with_retry("down", max_retries=2, base_delay=0.1)
    def down() -> None:
        calls["n"] += 1
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError):
        down()
    assert calls["n"] == 3


def test_retry_does_not_retry_unlisted_errors() -> None:
    calls = {"n": 0}

    @with_retry("strict", max_retries=5, base_delay=0, retry_on=(ConnectionError,))
    def bug() -> None:
        calls["n"] += 1
        raise ValueError("bug")

    with pytest.raises(ValueError):
        bug()
    assert calls["n"] == 1


def test_run_tracker_records_success_and_failure() -> None:
    t = RunTracker("r1", "2025-01-01", "1.0", {})
    with t.step("ok") as m:
        m["rows"] = 5
    with pytest.raises(RuntimeError):
        with t.step("boom"):
            raise RuntimeError("x")
    manifest = t.to_manifest()
    assert [s["status"] for s in manifest["steps"]] == ["success", "failed"]
    assert manifest["steps"][0]["metrics"] == {"rows": 5}
    assert "RuntimeError" in manifest["steps"][1]["error"]
    assert t.metric("ok") == {"rows": 5} and t.metric("missing") == {}


def test_json_formatter_includes_extra_fields() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", (), None)
    record.rows = 3
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "hello" and payload["rows"] == 3 and payload["level"] == "INFO"


def test_settings_local_vs_cloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IS_LOCAL", "true")
    monkeypatch.setenv("PARTICIPANT_ID", "ABC123")
    for var in ("DATA_LAKE_URI", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
        monkeypatch.delenv(var, raising=False)
    local = load_settings("2025-01-01")
    assert local.pg_sslmode == "disable" and local.s3_endpoint_url == "http://localhost:4566"
    assert local.lake_uri == "s3://talent-lake-abc123" and local.lake_is_s3
    assert "pg_pass" not in local.masked()

    monkeypatch.setenv("IS_LOCAL", "false")
    monkeypatch.delenv("LEARNING_ACTIVITY_URI", raising=False)
    cloud = load_settings("2025-01-01")
    assert cloud.pg_sslmode == "require" and cloud.s3_endpoint_url is None
    # matches the workshop IAM pattern <app_name>*<app_id>* for app_name = coding-workshop or coding-workshop-<id>
    assert cloud.lake_uri == "s3://coding-workshop-abc123-talent-lake-abc123"
    assert cloud.learning_activity_uri == "s3://coding-workshop-abc123-talent-lake-abc123/landing/learning_activity"


def test_settings_region_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("AWS_REGION", "AWS_DEFAULT_REGION", "APP_REGION"):
        monkeypatch.delenv(var, raising=False)
    assert load_settings("2025-01-01").aws_region == "us-east-1"
    monkeypatch.setenv("APP_REGION", "ap-south-1")  # injected by the workshop infra
    assert load_settings("2025-01-01").aws_region == "ap-south-1"
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    assert load_settings("2025-01-01").aws_region == "eu-west-1"


def test_settings_reject_bad_run_date() -> None:
    with pytest.raises(ValueError):
        load_settings("15-10-2026")
