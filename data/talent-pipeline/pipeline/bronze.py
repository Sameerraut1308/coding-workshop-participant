"""Bronze layer: land source data in the lake unchanged, with lineage.

* PostgreSQL tables -> ``bronze/<table>/load_date=<run_date>/part-000.parquet``
  (all values kept as text, plus lineage columns). Re-running the same
  ``run_date`` overwrites that folder, so reruns never create duplicates.
* ``learning_activity`` JSONL files -> copied byte-for-byte to
  ``bronze/learning_activity/year=/month=/day=/``. Raw files are immutable:
  an existing file with identical content is skipped (incremental), and a
  changed file is logged as a warning before being replaced.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import SOURCE_TABLES, Settings
from .observability import RunTracker, get_logger
from .storage import Storage, open_storage

log = get_logger(__name__)

BRONZE = "bronze"
LEARNING = "learning_activity"


def bronze_table_prefix(table: str, run_date: str) -> str:
    """Lake prefix for one Bronze snapshot."""
    return f"{BRONZE}/{table}/load_date={run_date}"


def add_lineage(df: pd.DataFrame, source: str, settings: Settings) -> pd.DataFrame:
    """Stamp lineage columns onto every row."""
    out = df.copy()
    out["_source"] = source
    out["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    out["_run_id"] = settings.run_id
    out["_pipeline_version"] = settings.pipeline_version
    return out


def _read_source_table(table: str, settings: Settings, engine: object | None) -> tuple[pd.DataFrame, str]:
    """Read one source table from PostgreSQL (default) or CSV (offline/tests)."""
    if settings.source_mode == "csv":
        path = Path(settings.source_dir) / f"{table}.csv"
        df = pd.read_csv(path, dtype="string", keep_default_na=False, na_values=[""])
        return df, f"csv:{path.name}"
    from .db import read_table

    return read_table(engine, table, settings), f"postgres:{settings.pg_name}.public.{table}"  # type: ignore[arg-type]


def ingest_tables(settings: Settings, lake: Storage, tracker: RunTracker) -> None:
    """Snapshot every source table into Bronze."""
    engine = None
    if settings.source_mode == "postgres":
        from .db import check_connection, make_engine

        engine = make_engine(settings)
        check_connection(engine, settings)

    for table in SOURCE_TABLES:
        with tracker.step(f"bronze.{table}") as m:
            df, source = _read_source_table(table, settings, engine)
            m["source"] = source
            m["rows_read"] = int(len(df))
            if df.empty:
                msg = f"source table {table} returned zero rows"
                if settings.fail_on_empty:
                    raise ValueError(msg)
                log.warning(msg, extra={"table": table})
            prefix = bronze_table_prefix(table, settings.run_date)
            m["bytes_written"] = lake.replace_parquet(add_lineage(df, source, settings), prefix)
            m["rows_written"] = int(len(df))
            m["path"] = f"{lake.uri}/{prefix}/"
    if engine is not None:
        engine.dispose()


def ingest_learning_activity(settings: Settings, lake: Storage, tracker: RunTracker) -> None:
    """Copy raw learning-activity JSONL files into Bronze, preserving partitions."""
    with tracker.step(f"bronze.{LEARNING}") as m:
        source = open_storage(settings.learning_activity_uri, settings)
        files = [k for k in source.list_keys("") if k.endswith(".jsonl")]
        if not files:
            raise FileNotFoundError(f"no .jsonl files found under {settings.learning_activity_uri}")

        copied = skipped = changed = empty = bad_partition = 0
        source_bytes = 0
        for key in files:
            parts = dict(p.split("=", 1) for p in key.split("/") if "=" in p)
            if not {"year", "month", "day"} <= parts.keys():
                bad_partition += 1
                log.warning("file outside year=/month=/day= layout skipped", extra={"file": key})
                continue
            data = source.get_bytes(key)
            source_bytes += len(data)
            if not data.strip():
                empty += 1
                log.warning("empty source file", extra={"file": key})
            target = f"{BRONZE}/{LEARNING}/{key}"
            if lake.exists(target):
                if lake.get_bytes(target) == data:
                    skipped += 1
                    continue
                changed += 1
                log.warning("raw file changed at source; replacing", extra={"file": key})
            lake.put_bytes(target, data)
            copied += 1

        m.update(
            {
                "source": settings.learning_activity_uri,
                "files_found": len(files),
                "files_copied": copied,
                "files_unchanged_skipped": skipped,
                "files_changed": changed,
                "files_empty": empty,
                "files_bad_partition": bad_partition,
                "source_bytes": source_bytes,
            }
        )


def run_bronze(settings: Settings, lake: Storage, tracker: RunTracker) -> None:
    """Run the whole Bronze layer."""
    ingest_tables(settings, lake, tracker)
    ingest_learning_activity(settings, lake, tracker)
