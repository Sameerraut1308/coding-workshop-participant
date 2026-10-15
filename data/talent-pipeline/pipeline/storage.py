"""One small storage API over S3 (LocalStack or AWS) and the local file system.

The rest of the pipeline only calls ``put_bytes``, ``get_bytes``, ``list_keys``,
``write_parquet`` and so on, so the same code runs against LocalStack, real AWS,
or a temp folder in unit tests.
"""

from __future__ import annotations

import hashlib
import io
import json
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from .config import Settings
from .observability import get_logger, with_retry

log = get_logger(__name__)


class Storage(ABC):
    """Minimal object-store interface; keys are always "/"-separated."""

    uri: str

    @abstractmethod
    def put_bytes(self, key: str, data: bytes) -> None:
        """Write (overwrite) one object."""

    @abstractmethod
    def get_bytes(self, key: str) -> bytes:
        """Read one object."""

    @abstractmethod
    def list_keys(self, prefix: str = "") -> list[str]:
        """List object keys under a prefix, sorted."""

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int:
        """Delete every object under a prefix; returns how many were removed."""

    @abstractmethod
    def size(self, key: str) -> int:
        """Size of one object in bytes."""

    def exists(self, key: str) -> bool:
        """True if the object exists."""
        try:
            self.size(key)
            return True
        except (FileNotFoundError, KeyError):
            return False

    def md5(self, key: str) -> str:
        """MD5 of an object's content (used to skip unchanged raw files)."""
        return hashlib.md5(self.get_bytes(key), usedforsecurity=False).hexdigest()

    # ---- DataFrame helpers -------------------------------------------------
    def write_parquet(self, df: pd.DataFrame, key: str) -> int:
        """Write a DataFrame as one Parquet object; returns bytes written."""
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        payload = buffer.getvalue()
        self.put_bytes(key, payload)
        return len(payload)

    def read_parquet(self, prefix_or_key: str) -> pd.DataFrame:
        """Read one Parquet object, or concatenate every ``.parquet`` under a prefix."""
        keys = [k for k in self.list_keys(prefix_or_key) if k.endswith(".parquet")]
        if not keys:
            raise FileNotFoundError(f"no parquet data under {self.uri}/{prefix_or_key}")
        frames = [pd.read_parquet(io.BytesIO(self.get_bytes(k))) for k in keys]
        frames = [f for f in frames if not f.empty] or frames[:1]
        return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    def replace_parquet(self, df: pd.DataFrame, prefix: str, filename: str = "part-000.parquet") -> int:
        """Idempotent overwrite: clear the prefix, then write one file into it."""
        self.delete_prefix(prefix)
        return self.write_parquet(df, f"{prefix.rstrip('/')}/{filename}")

    def write_json(self, obj: Any, key: str) -> None:
        """Write a JSON document."""
        self.put_bytes(key, json.dumps(obj, indent=2, default=str).encode("utf-8"))

    def read_json(self, key: str) -> Any:
        """Read a JSON document."""
        return json.loads(self.get_bytes(key).decode("utf-8"))


class LocalStorage(Storage):
    """Storage rooted at a local folder (unit tests, offline runs, local source files)."""

    def __init__(self, root: str):
        self.root = Path(root).expanduser().resolve()
        self.uri = str(self.root)

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root not in path.parents and path != self.root:
            raise ValueError(f"key escapes storage root: {key}")
        return path

    def put_bytes(self, key: str, data: bytes) -> None:  # noqa: D102
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:  # noqa: D102
        return self._path(key).read_bytes()

    def list_keys(self, prefix: str = "") -> list[str]:  # noqa: D102
        base = self._path(prefix) if prefix else self.root
        if base.is_file():
            return [prefix]
        if not base.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix: str) -> int:  # noqa: D102
        target = self._path(prefix)
        if target.is_file():
            target.unlink()
            return 1
        if not target.exists():
            return 0
        count = sum(1 for p in target.rglob("*") if p.is_file())
        shutil.rmtree(target)
        return count

    def size(self, key: str) -> int:  # noqa: D102
        path = self._path(key)
        if not path.is_file():
            raise FileNotFoundError(key)
        return path.stat().st_size


class S3Storage(Storage):
    """Storage in an S3 bucket (optionally under a key prefix)."""

    def __init__(self, uri: str, settings: Settings):
        import boto3
        from botocore.config import Config

        without_scheme = uri[len("s3://"):]
        self.bucket, _, base = without_scheme.partition("/")
        self.base = base.strip("/")
        self.uri = uri.rstrip("/")
        self._retries = settings.max_retries
        self._delay = settings.retry_base_delay
        client_kwargs: dict[str, Any] = {
            "region_name": settings.aws_region,
            "config": Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3, "mode": "standard"}),
        }
        if settings.s3_endpoint_url:
            client_kwargs["endpoint_url"] = settings.s3_endpoint_url
            # LocalStack accepts any credentials; only default them when nothing is configured.
            session = boto3.session.Session()
            if session.get_credentials() is None:
                client_kwargs["aws_access_key_id"] = "test"
                # LocalStack dummy credential, not a secret.
                client_kwargs["aws_secret_access_key"] = "test"  # nosec B105
        self.client = boto3.client("s3", **client_kwargs)

    def _key(self, key: str) -> str:
        return f"{self.base}/{key}" if self.base else key

    def _strip(self, full_key: str) -> str:
        return full_key[len(self.base) + 1:] if self.base else full_key

    def ensure_bucket(self) -> None:
        """Create the bucket if it does not exist (LocalStack convenience)."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            log.info("creating bucket", extra={"bucket": self.bucket})
            # S3 rejects an explicit us-east-1 constraint but requires one for every other region.
            region = self.client.meta.region_name
            if region and region != "us-east-1":
                self.client.create_bucket(
                    Bucket=self.bucket, CreateBucketConfiguration={"LocationConstraint": region}
                )
            else:
                self.client.create_bucket(Bucket=self.bucket)

    def put_bytes(self, key: str, data: bytes) -> None:  # noqa: D102
        with_retry("s3.put_object", self._retries, self._delay)(self.client.put_object)(
            Bucket=self.bucket, Key=self._key(key), Body=data
        )

    def get_bytes(self, key: str) -> bytes:  # noqa: D102
        def _get() -> bytes:
            return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

        return with_retry("s3.get_object", self._retries, self._delay)(_get)()

    def list_keys(self, prefix: str = "") -> list[str]:  # noqa: D102
        def _list() -> list[str]:
            keys: list[str] = []
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
                keys.extend(self._strip(obj["Key"]) for obj in page.get("Contents", []))
            return sorted(keys)

        return with_retry("s3.list_objects", self._retries, self._delay)(_list)()

    def delete_prefix(self, prefix: str) -> int:  # noqa: D102
        keys = self.list_keys(prefix)
        for start in range(0, len(keys), 1000):
            chunk = [{"Key": self._key(k)} for k in keys[start:start + 1000]]
            with_retry("s3.delete_objects", self._retries, self._delay)(self.client.delete_objects)(
                Bucket=self.bucket, Delete={"Objects": chunk, "Quiet": True}
            )
        return len(keys)

    def size(self, key: str) -> int:  # noqa: D102
        from botocore.exceptions import ClientError

        try:
            return int(self.client.head_object(Bucket=self.bucket, Key=self._key(key))["ContentLength"])
        except ClientError as exc:
            raise FileNotFoundError(key) from exc


def open_storage(uri: str, settings: Settings) -> Storage:
    """Open S3 storage for ``s3://`` URIs, otherwise a local folder."""
    if uri.startswith("s3://"):
        return S3Storage(uri, settings)
    return LocalStorage(uri)


def open_lake(settings: Settings) -> Storage:
    """Open the data lake, creating the bucket on LocalStack if needed."""
    lake = open_storage(settings.lake_uri, settings)
    if isinstance(lake, S3Storage) and settings.is_local:
        lake.ensure_bucket()
    return lake
