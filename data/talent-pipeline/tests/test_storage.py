"""Storage tests: local folder and S3 (mocked with moto, no network needed)."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.config import load_settings
from pipeline.storage import LocalStorage, S3Storage, open_storage


def _roundtrip(store) -> None:  # type: ignore[no-untyped-def]
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    store.replace_parquet(df, "t/x")
    store.replace_parquet(df, "t/x")  # rerun: still exactly one file
    assert store.list_keys("t/x") == ["t/x/part-000.parquet"]
    pd.testing.assert_frame_equal(store.read_parquet("t/x"), df)
    store.put_bytes("raw/f.jsonl", b"hello")
    assert store.size("raw/f.jsonl") == 5 and store.exists("raw/f.jsonl") and not store.exists("raw/nope")
    assert store.md5("raw/f.jsonl") == "5d41402abc4b2a76b9719d911017c592"
    store.write_json({"k": 1}, "m.json")
    assert store.read_json("m.json") == {"k": 1}
    assert store.delete_prefix("t/") == 1 and store.list_keys("t/") == []
    with pytest.raises(FileNotFoundError):
        store.read_parquet("t/")


def test_local_storage_roundtrip(tmp_path) -> None:  # type: ignore[no-untyped-def]
    _roundtrip(LocalStorage(str(tmp_path)))


def test_local_storage_blocks_path_escape(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ValueError):
        LocalStorage(str(tmp_path)).put_bytes("../evil", b"x")


def test_s3_storage_roundtrip_with_moto(monkeypatch: pytest.MonkeyPatch) -> None:
    moto = pytest.importorskip("moto")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("IS_LOCAL", "false")  # no endpoint override -> moto intercepts boto3
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    with moto.mock_aws():
        settings = load_settings("2025-01-01")
        store = open_storage("s3://unit-test-bucket/prefix", settings)
        assert isinstance(store, S3Storage)
        store.ensure_bucket()
        _roundtrip(store)
        # many objects -> exercises pagination and batched deletes
        for n in range(1105):
            store.put_bytes(f"bulk/{n:05d}", b"x")
        assert len(store.list_keys("bulk/")) == 1105
        assert store.delete_prefix("bulk/") == 1105


@pytest.mark.parametrize("region", ["us-east-1", "ap-south-1"])
def test_s3_ensure_bucket_in_any_region(monkeypatch: pytest.MonkeyPatch, region: str) -> None:
    moto = pytest.importorskip("moto")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_REGION", region)
    monkeypatch.setenv("IS_LOCAL", "false")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    with moto.mock_aws():
        store = open_storage(f"s3://region-test-{region}", load_settings("2025-01-01"))
        store.ensure_bucket()
        store.ensure_bucket()  # second call is a no-op
        assert store.client.get_bucket_location(Bucket=store.bucket)["LocationConstraint"] in (None, region)


def test_upload_source_copies_once_and_skips_unchanged(tmp_path) -> None:  # type: ignore[no-untyped-def]
    import importlib.util

    from tests.conftest import ROOT

    spec = importlib.util.spec_from_file_location("upload_source", ROOT / "scripts" / "upload_source.py")
    upload_source = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upload_source)  # type: ignore[union-attr]

    src = tmp_path / "src"
    (src / "year=2025/month=01/day=10").mkdir(parents=True)
    (src / "year=2025/month=01/day=10/events-000.jsonl").write_bytes(b'{"a": 1}\n')
    (src / "notes.txt").write_text("ignored")
    dest = LocalStorage(str(tmp_path / "lake"))
    assert upload_source.upload_folder(src, dest) == {"files_uploaded": 1, "files_unchanged_skipped": 0}
    assert dest.list_keys("") == ["year=2025/month=01/day=10/events-000.jsonl"]
    assert upload_source.upload_folder(src, dest) == {"files_uploaded": 0, "files_unchanged_skipped": 1}
