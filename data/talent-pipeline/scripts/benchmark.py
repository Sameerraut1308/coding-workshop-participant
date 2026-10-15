"""Scale test: run the full pipeline offline at 1x, 5x and 10x the learning-activity volume.

Usage::

    python scripts/benchmark.py --source-dir ~/data-engineering-performance-talent-management --scales 1,5,10

For each scale it copies the dataset into a temp folder, multiplies the
learning-activity events (new unique event_ids), runs the whole job against a
local-folder lake, and reports duration, throughput and peak memory. Each scale
runs in a fresh process so peak-memory numbers are not mixed up.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
# subprocess only re-runs this script with the current interpreter (fixed argv, no shell).
import subprocess  # nosec B404
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_scaled_dataset(source_dir: Path, target: Path, scale: int) -> int:
    """Copy CSVs and write ``scale`` copies of every learning event; returns event lines written."""
    target.mkdir(parents=True, exist_ok=True)
    for csv in source_dir.glob("*.csv"):
        shutil.copy(csv, target / csv.name)
    lines_written = 0
    for src in (source_dir / "learning_activity").rglob("*.jsonl"):
        rel = src.relative_to(source_dir / "learning_activity")
        dst = target / "learning_activity" / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        original = src.read_text().splitlines()
        out: list[str] = []
        for copy in range(scale):
            for line in original:
                out.append(line if copy == 0 else line.replace('"event_id": "EV', f'"event_id": "EV{copy:02d}', 1))
        dst.write_text("\n".join(out) + ("\n" if out else ""))
        lines_written += len(out)
    return lines_written


def run_once(source_dir: Path, scale: int) -> dict[str, object]:
    """Run one scale in a child process and collect its metrics."""
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "data"
        lines = build_scaled_dataset(source_dir, data, scale)
        env = {**os.environ, "SOURCE_MODE": "csv", "SOURCE_DIR": str(data),
               "LEARNING_ACTIVITY_URI": str(data / "learning_activity"), "DATA_LAKE_URI": str(Path(tmp) / "lake"),
               "WRITE_GOLD_TO_POSTGRES": "false", "RUN_DATE": "2026-10-15", "LOG_LEVEL": "WARNING"}
        start = time.perf_counter()
        child_cmd = [sys.executable, str(ROOT / "scripts" / "benchmark.py"), "--child"]
        proc = subprocess.run(child_cmd, env=env, capture_output=True, text=True, check=False)  # nosec B603
        elapsed = time.perf_counter() - start
        if proc.returncode != 0:
            raise RuntimeError(proc.stdout[-2000:] + proc.stderr[-2000:])
        child = json.loads(proc.stdout.strip().splitlines()[-1])
        manifest = json.loads(next((Path(tmp) / "lake" / "_runs").rglob("*.json")).read_text())
        silver_la = next(s for s in manifest["steps"] if s["name"] == "silver.learning_activity")
        return {
            "scale": f"{scale}x",
            "learning_lines": lines,
            "job_seconds": round(elapsed, 1),
            "silver_learning_seconds": silver_la["duration_s"],
            "learning_rows_per_minute": int(lines / silver_la["duration_s"] * 60),
            "end_to_end_rows_per_minute": int(lines / elapsed * 60),
            "peak_memory_mb": child["peak_memory_mb"],
            "reconciliation": f'{sum(c["passed"] for c in manifest["reconciliation"])}'
                              f'/{len(manifest["reconciliation"])}',
        }


def child() -> None:
    """Run the job in this process and print peak memory as JSON."""
    sys.path.insert(0, str(ROOT))
    import job

    code = job.main([])
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"exit": code, "peak_memory_mb": round(peak_kb / 1024, 1)}))
    sys.exit(code)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-dir", default=str(Path.home() / "data-engineering-performance-talent-management"))
    parser.add_argument("--scales", default="1,5,10")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        child()
        return
    results = [run_once(Path(args.source_dir).expanduser(), int(s)) for s in args.scales.split(",")]
    header = list(results[0])
    print(" | ".join(header))
    print(" | ".join("---" for _ in header))
    for r in results:
        print(" | ".join(str(r[h]) for h in header))


if __name__ == "__main__":
    main()
