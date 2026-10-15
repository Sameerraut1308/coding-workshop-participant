"""Runtime configuration, read entirely from environment variables.

Nothing in the pipeline is hard-coded to local or cloud: the same code runs on
the LocalStack VDI (``IS_LOCAL=true``) and on AWS (``IS_LOCAL=false``).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from . import __version__

# The seven PostgreSQL source tables, in foreign-key load order.
SOURCE_TABLES: tuple[str, ...] = (
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)

DEFAULT_SOURCE_DIR = str(Path.home() / "data-engineering-performance-talent-management")

# Raw learning events live under this prefix of the lake bucket in the cloud (uploaded by scripts/upload_source.py).
LANDING_LEARNING_PREFIX = "landing/learning_activity"
# Source CSVs + schema.sql used to seed an empty cloud database (uploaded by scripts/upload_source.py).
LANDING_SOURCE_PREFIX = "landing/source"


def default_lake_uri(participant: str, is_local: bool) -> str:
    """Default lake bucket.

    In AWS the workshop IAM policy only allows buckets matching ``<app_name>*<app_id>*``, so the cloud name
    repeats the participant id after the ``coding-workshop-<id>`` prefix to match either reading of ``app_name``.
    """
    participant = participant.lower()
    if is_local:
        return f"s3://talent-lake-{participant}"
    return f"s3://coding-workshop-{participant}-talent-lake-{participant}"


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable ("true"/"1"/"yes" are truthy)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _env_str(name: str, default: str = "") -> str:
    """Return a stripped environment variable, falling back when unset or empty."""
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


@dataclass(frozen=True)
class Settings:
    """All runtime settings for one pipeline run."""

    is_local: bool
    run_date: str
    run_id: str
    pipeline_version: str

    # Data lake (Bronze / Silver / Gold / quarantine). "s3://bucket" or a local folder.
    lake_uri: str
    aws_region: str
    s3_endpoint_url: str | None

    # Where source data comes from.
    source_mode: str  # "postgres" (default) or "csv" (offline / tests)
    source_dir: str  # folder holding the CSVs (csv mode)
    learning_activity_uri: str  # folder or s3://bucket/prefix with year=/month=/day= files
    seed_source_uri: str  # folder or s3://... with <table>.csv + schema.sql; "" = never seed PostgreSQL

    # PostgreSQL connection.
    pg_host: str
    pg_port: int
    pg_name: str
    pg_user: str
    pg_pass: str = field(repr=False)

    # Behaviour switches.
    write_gold_to_postgres: bool = True
    gold_schema: str = "gold"
    fail_on_empty: bool = True
    max_retries: int = 4
    retry_base_delay: float = 1.0

    @property
    def pg_sslmode(self) -> str:
        """Aurora in the cloud requires SSL; the local PostgreSQL does not."""
        return "disable" if self.is_local else "require"

    @property
    def lake_is_s3(self) -> bool:
        """True when the data lake lives in S3 (LocalStack or AWS)."""
        return self.lake_uri.startswith("s3://")

    def masked(self) -> dict[str, object]:
        """Settings safe to log (no password)."""
        return {
            "is_local": self.is_local,
            "run_date": self.run_date,
            "run_id": self.run_id,
            "pipeline_version": self.pipeline_version,
            "lake_uri": self.lake_uri,
            "s3_endpoint_url": self.s3_endpoint_url,
            "source_mode": self.source_mode,
            "learning_activity_uri": self.learning_activity_uri,
            "seed_source_uri": self.seed_source_uri,
            "pg_host": self.pg_host,
            "pg_port": self.pg_port,
            "pg_name": self.pg_name,
            "pg_sslmode": self.pg_sslmode,
            "write_gold_to_postgres": self.write_gold_to_postgres,
        }


def load_settings(run_date: str | None = None) -> Settings:
    """Build :class:`Settings` from the environment.

    Key variables (all optional except the PostgreSQL ones in postgres mode):

    * ``IS_LOCAL`` - ``true`` on the VDI / LocalStack, ``false`` in AWS.
    * ``DATA_LAKE_URI`` - default ``s3://talent-lake-<id>`` locally, ``s3://coding-workshop-<id>-talent-lake-<id>``
      in the cloud (see :func:`default_lake_uri`); a local path also works.
    * ``AWS_ENDPOINT_URL`` - LocalStack endpoint, used only when ``IS_LOCAL=true``.
    * ``SOURCE_MODE`` - ``postgres`` (default) or ``csv``.
    * ``SOURCE_DIR`` - dataset folder (default ``~/data-engineering-performance-talent-management``).
    * ``LEARNING_ACTIVITY_URI`` - default ``$SOURCE_DIR/learning_activity`` locally and
      ``<lake>/landing/learning_activity`` in the cloud (the pod has no copy of the dataset folder).
    * ``SEED_SOURCE_URI`` - where the seed step finds the source CSVs to load into an empty database:
      off locally (``load.sh`` does it), ``<lake>/landing/source`` in the cloud (Aurora is unreachable from the VDI).
    * ``PARTICIPANT_ID`` - names the default lake; falls back to ``APP_ID`` (what the workshop gives the EKS job).
    * ``AWS_REGION`` - falls back to ``AWS_DEFAULT_REGION``, then ``APP_REGION`` (workshop-injected), then us-east-1.
    * ``POSTGRES_HOST/PORT/NAME/USER/PASS`` - injected by the workshop environment.
    * ``RUN_DATE`` - logical run date (``YYYY-MM-DD``), default today (UTC).
    """
    is_local = _env_bool("IS_LOCAL", True)
    participant = _env_str("PARTICIPANT_ID") or _env_str("APP_ID", "local")
    resolved_run_date = run_date or _env_str("RUN_DATE", date.today().isoformat())
    datetime.strptime(resolved_run_date, "%Y-%m-%d")  # fail fast on a bad date

    source_dir = _env_str("SOURCE_DIR", DEFAULT_SOURCE_DIR)
    endpoint = None
    if is_local:
        endpoint = _env_str("AWS_ENDPOINT_URL_S3") or _env_str("AWS_ENDPOINT_URL") or "http://localhost:4566"

    lake_uri = _env_str("DATA_LAKE_URI", default_lake_uri(participant, is_local))
    if is_local:
        default_learning = str(Path(source_dir) / "learning_activity")
        default_seed = ""
    else:
        default_learning = f"{lake_uri.rstrip('/')}/{LANDING_LEARNING_PREFIX}"
        default_seed = f"{lake_uri.rstrip('/')}/{LANDING_SOURCE_PREFIX}"

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Settings(
        is_local=is_local,
        run_date=resolved_run_date,
        run_id=_env_str("RUN_ID", f"{run_stamp}-{uuid.uuid4().hex[:8]}"),
        pipeline_version=__version__,
        lake_uri=lake_uri,
        aws_region=_env_str("AWS_REGION") or _env_str("AWS_DEFAULT_REGION") or _env_str("APP_REGION", "us-east-1"),
        s3_endpoint_url=endpoint,
        source_mode=_env_str("SOURCE_MODE", "postgres").lower(),
        source_dir=source_dir,
        learning_activity_uri=_env_str("LEARNING_ACTIVITY_URI", default_learning),
        seed_source_uri=_env_str("SEED_SOURCE_URI", default_seed),
        pg_host=_env_str("POSTGRES_HOST", "localhost"),
        pg_port=int(_env_str("POSTGRES_PORT", "5432")),
        pg_name=_env_str("POSTGRES_NAME", "postgres"),
        pg_user=_env_str("POSTGRES_USER", "postgres"),
        pg_pass=os.getenv("POSTGRES_PASS", ""),
        write_gold_to_postgres=_env_bool("WRITE_GOLD_TO_POSTGRES", True),
        gold_schema=_env_str("GOLD_SCHEMA", "gold"),
        fail_on_empty=_env_bool("FAIL_ON_EMPTY", True),
        max_retries=int(_env_str("MAX_RETRIES", "4")),
        retry_base_delay=float(_env_str("RETRY_BASE_DELAY", "1.0")),
    )
