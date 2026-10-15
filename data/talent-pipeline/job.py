"""Entry point: run the ACME Performance & Talent pipeline end to end.

Usage::

    python job.py                         # seed -> bronze -> silver -> gold -> reconcile
    python job.py --steps silver,gold     # run selected steps only
    python job.py --run-date 2026-10-15   # reprocess a specific logical date

Exit code is 0 on success and 1 on any failure; a run manifest (metrics for
every step plus reconciliation results) is always written to
``<lake>/_runs/run_date=<date>/<run_id>.json``.

The ``seed`` step only does something when ``SEED_SOURCE_URI`` is set (the cloud default): it loads
the source CSVs into PostgreSQL if the source tables are empty (see ``pipeline/seed.py``).
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.bronze import run_bronze  # noqa: E402
from pipeline.config import load_settings  # noqa: E402
from pipeline.gold import run_gold  # noqa: E402
from pipeline.observability import RunTracker, get_logger, setup_logging  # noqa: E402
from pipeline.reconcile import run_reconcile  # noqa: E402
from pipeline.seed import run_seed  # noqa: E402
from pipeline.silver import run_silver  # noqa: E402
from pipeline.storage import open_lake  # noqa: E402

ALL_STEPS = ("seed", "bronze", "silver", "gold", "reconcile")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--steps", default=",".join(ALL_STEPS), help=f"comma-separated subset of {ALL_STEPS}")
    parser.add_argument("--run-date", default=None, help="logical run date YYYY-MM-DD (default: $RUN_DATE or today)")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)
    args.steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = set(args.steps) - set(ALL_STEPS)
    if unknown:
        parser.error(f"unknown steps: {sorted(unknown)}")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the requested steps; returns a process exit code."""
    args = parse_args(argv)
    setup_logging(args.log_level)
    log = get_logger("job")
    settings = load_settings(args.run_date)
    tracker = RunTracker(settings.run_id, settings.run_date, settings.pipeline_version, settings.masked())
    log.info("job started", extra={"steps": args.steps, **settings.masked()})

    lake = None
    exit_code = 0
    try:
        lake = open_lake(settings)
        if "seed" in args.steps:
            run_seed(settings, tracker)
        if "bronze" in args.steps:
            run_bronze(settings, lake, tracker)
        if "silver" in args.steps:
            run_silver(settings, lake, tracker)
        if "gold" in args.steps:
            run_gold(settings, lake, tracker)
        if "reconcile" in args.steps:
            run_reconcile(settings, lake, tracker)
        tracker.status = "success"
    except Exception as exc:  # noqa: BLE001 - top-level guard: log, record, exit non-zero
        tracker.status = "failed"
        exit_code = 1
        log.error("job failed", extra={"error": repr(exc)}, exc_info=True)
    finally:
        manifest = tracker.to_manifest()
        if lake is not None:
            try:
                key = f"_runs/run_date={settings.run_date}/{settings.run_id}.json"
                lake.write_json(manifest, key)
                log.info("run manifest written", extra={"path": f"{lake.uri}/{key}"})
            except Exception as exc:  # noqa: BLE001
                log.error("could not write run manifest", extra={"error": repr(exc)})
        log.info(
            "job finished",
            extra={"status": tracker.status, "duration_s": manifest["duration_s"], "steps_run": len(tracker.steps),
                   "reconciliation_passed": sum(c["passed"] for c in tracker.checks),
                   "reconciliation_total": len(tracker.checks)},
        )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
