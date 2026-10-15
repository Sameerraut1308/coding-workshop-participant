"""Upload the raw learning-activity files to S3, where the cloud job reads them.

Usage::

    source scripts/env.sh
    python scripts/upload_source.py                     # -> <lake>/landing/learning_activity
    python scripts/upload_source.py --dest s3://bucket/prefix

The cloud pod has no copy of the dataset folder, so in cloud mode the job defaults
``LEARNING_ACTIVITY_URI`` to ``<lake>/landing/learning_activity``. The
``year=/month=/day=`` layout is kept, and files already uploaded with identical
bytes are skipped, so re-running is safe.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.config import LANDING_LEARNING_PREFIX, load_settings  # noqa: E402
from pipeline.storage import S3Storage, Storage, open_storage  # noqa: E402


def upload_folder(source: Path, dest: Storage) -> dict[str, int]:
    """Copy every ``*.jsonl`` under ``source`` to ``dest`` (same relative paths); returns counts."""
    uploaded = unchanged = 0
    for path in sorted(source.rglob("*.jsonl")):
        key = path.relative_to(source).as_posix()
        data = path.read_bytes()
        if dest.exists(key) and dest.md5(key) == hashlib.md5(data, usedforsecurity=False).hexdigest():
            unchanged += 1
            continue
        dest.put_bytes(key, data)
        uploaded += 1
    return {"files_uploaded": uploaded, "files_unchanged_skipped": unchanged}


def main(argv: list[str] | None = None) -> int:
    """Upload the dataset's learning_activity folder; prints a JSON summary."""
    settings = load_settings()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=str(Path(settings.source_dir) / "learning_activity"))
    parser.add_argument("--dest", default=os.getenv("LEARNING_ACTIVITY_URI")
                        or f"{settings.lake_uri.rstrip('/')}/{LANDING_LEARNING_PREFIX}")
    args = parser.parse_args(argv)

    source = Path(args.source).expanduser()
    if not any(source.rglob("*.jsonl")):
        parser.error(f"no .jsonl files under {source}")
    dest = open_storage(args.dest, settings)
    if isinstance(dest, S3Storage):
        dest.ensure_bucket()
    print(json.dumps({"source": str(source), "dest": args.dest, **upload_folder(source, dest)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
