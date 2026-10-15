"""Upload the source data to S3, where the cloud job reads it.

Usage::

    source scripts/env.sh
    IS_LOCAL=false python scripts/upload_source.py      # both uploads below, to the cloud lake
    python scripts/upload_source.py --dest s3://bucket/prefix --skip-tables

Two uploads, both safe to re-run (files already uploaded with identical bytes are skipped):

* ``learning_activity/**/*.jsonl`` -> ``<lake>/landing/learning_activity`` (``--dest``), keeping the
  ``year=/month=/day=`` layout. The cloud pod has no copy of the dataset folder, so in cloud mode the
  job defaults ``LEARNING_ACTIVITY_URI`` to this location.
* the seven source ``<table>.csv`` files and ``schema.sql`` -> ``<lake>/landing/source`` (``--tables-dest``).
  The job's seed step loads them into Aurora when its source tables are empty, because the VDI
  cannot reach Aurora directly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.config import LANDING_LEARNING_PREFIX, LANDING_SOURCE_PREFIX, SOURCE_TABLES, load_settings  # noqa: E402
from pipeline.seed import SCHEMA_FILE  # noqa: E402
from pipeline.storage import S3Storage, Storage, open_storage  # noqa: E402


def upload_files(files: list[tuple[str, Path]], dest: Storage) -> dict[str, int]:
    """Copy each ``(key, path)`` to ``dest``, skipping objects that already hold identical bytes."""
    uploaded = unchanged = 0
    for key, path in files:
        data = path.read_bytes()
        if dest.exists(key) and dest.md5(key) == hashlib.md5(data, usedforsecurity=False).hexdigest():
            unchanged += 1
            continue
        dest.put_bytes(key, data)
        uploaded += 1
    return {"files_uploaded": uploaded, "files_unchanged_skipped": unchanged}


def upload_folder(source: Path, dest: Storage) -> dict[str, int]:
    """Copy every ``*.jsonl`` under ``source`` to ``dest`` (same relative paths); returns counts."""
    return upload_files([(p.relative_to(source).as_posix(), p) for p in sorted(source.rglob("*.jsonl"))], dest)


def source_table_files(source_dir: Path) -> list[tuple[str, Path]]:
    """The seven source CSVs plus ``schema.sql``; raises if any is missing."""
    names = [f"{t}.csv" for t in SOURCE_TABLES] + [SCHEMA_FILE]
    missing = [n for n in names if not (source_dir / n).is_file()]
    if missing:
        raise FileNotFoundError(f"missing in {source_dir}: {missing}")
    return [(n, source_dir / n) for n in names]


def _open_dest(uri: str, settings) -> Storage:  # type: ignore[no-untyped-def]
    dest = open_storage(uri, settings)
    if isinstance(dest, S3Storage):
        dest.ensure_bucket()
    return dest


def main(argv: list[str] | None = None) -> int:
    """Upload the learning files and the source tables; prints a JSON summary."""
    settings = load_settings()
    lake = settings.lake_uri.rstrip("/")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=str(Path(settings.source_dir) / "learning_activity"))
    parser.add_argument("--dest", default=os.getenv("LEARNING_ACTIVITY_URI") or f"{lake}/{LANDING_LEARNING_PREFIX}")
    parser.add_argument("--tables-source", default=settings.source_dir, help="folder with the CSVs and schema.sql")
    parser.add_argument("--tables-dest", default=os.getenv("SEED_SOURCE_URI") or f"{lake}/{LANDING_SOURCE_PREFIX}")
    parser.add_argument("--skip-tables", action="store_true", help="upload only the learning files")
    args = parser.parse_args(argv)

    source = Path(args.source).expanduser()
    if not any(source.rglob("*.jsonl")):
        parser.error(f"no .jsonl files under {source}")
    tables = [] if args.skip_tables else source_table_files(Path(args.tables_source).expanduser())

    summary: dict[str, object] = {"source": str(source), "dest": args.dest,
                                  **upload_folder(source, _open_dest(args.dest, settings))}
    if tables:
        summary["source_tables"] = {"dest": args.tables_dest,
                                    **upload_files(tables, _open_dest(args.tables_dest, settings))}
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
