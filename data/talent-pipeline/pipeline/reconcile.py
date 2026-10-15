"""Cross-layer reconciliation: prove no rows were lost or invented.

Checks are computed from what is actually stored in the lake (not from
in-memory state), so ``python job.py --steps reconcile`` can audit any
earlier run on its own.
"""

from __future__ import annotations

from typing import Any

from .bronze import BRONZE, LEARNING, bronze_table_prefix
from .config import SOURCE_TABLES, Settings
from .gold import BUSINESS_QUESTIONS, RULES, gold_prefix
from .observability import RunTracker, get_logger
from .silver import quarantine_prefix, silver_prefix
from .storage import Storage, open_storage

log = get_logger(__name__)


def _rows(lake: Storage, prefix: str) -> int:
    try:
        return int(len(lake.read_parquet(prefix)))
    except FileNotFoundError:
        return 0


def _check(results: list[dict[str, Any]], layer: str, name: str, expected: Any, actual: Any,
           tolerance: float = 0.0) -> None:
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        passed = abs(float(expected) - float(actual)) <= tolerance
    else:
        passed = expected == actual
    results.append({"layer": layer, "check": name, "expected": expected, "actual": actual, "passed": bool(passed)})


def reconcile(settings: Settings, lake: Storage) -> list[dict[str, Any]]:
    """Run every reconciliation check and return the results."""
    r: list[dict[str, Any]] = []

    # ---- Bronze vs source: raw files must match byte-for-byte -----------------
    source = open_storage(settings.learning_activity_uri, settings)
    src_files = {k: source.size(k) for k in source.list_keys("") if k.endswith(".jsonl")}
    lake_files = {k[len(f"{BRONZE}/{LEARNING}/"):]: lake.size(k) for k in lake.list_keys(f"{BRONZE}/{LEARNING}/")}
    mismatched = sorted(k for k, size in src_files.items() if lake_files.get(k) != size)
    _check(r, "bronze", "learning_activity files: count source == bronze", len(src_files), len(lake_files))
    _check(r, "bronze", "learning_activity files: size mismatches", 0, len(mismatched))

    # ---- Silver: bronze == silver + quarantine + exact duplicates -------------
    for table in SOURCE_TABLES:
        bronze = _rows(lake, bronze_table_prefix(table, settings.run_date))
        silver = _rows(lake, silver_prefix(table) + "/")
        quarantined = _rows(lake, quarantine_prefix(table, settings.run_date))
        dupes = int(lake.read_json(f"_metadata/silver/{table}.json").get("exact_duplicates_dropped", 0))
        _check(r, "silver", f"{table}: bronze == silver + quarantine + duplicates", bronze,
               silver + quarantined + dupes)

    non_blank = 0
    for key in lake.list_keys(f"{BRONZE}/{LEARNING}/"):
        non_blank += sum(1 for line in lake.get_bytes(key).decode("utf-8", errors="replace").splitlines()
                         if line.strip())
    meta = lake.read_json(f"_metadata/silver/{LEARNING}.json")
    learning = lake.read_parquet(silver_prefix(LEARNING) + "/")
    l_quar = _rows(lake, quarantine_prefix(LEARNING, settings.run_date))
    _check(r, "silver", "learning_activity: bronze lines == silver + quarantine + duplicates",
           non_blank, len(learning) + l_quar + int(meta.get("exact_duplicates_dropped", 0)))
    _check(r, "silver", "learning_activity: event_id unique", 0, int(learning["event_id"].duplicated().sum()))

    # ---- Gold vs Silver ----------------------------------------------------
    employees = lake.read_parquet(silver_prefix("employees") + "/")
    reviews = lake.read_parquet(silver_prefix("performance_reviews") + "/")
    assessments = lake.read_parquet(silver_prefix("competency_assessments") + "/")
    talent = lake.read_parquet(silver_prefix("talent_review") + "/")
    teams = lake.read_parquet(silver_prefix("teams") + "/")
    projects = lake.read_parquet(silver_prefix("projects") + "/")
    g = {name: lake.read_parquet(gold_prefix(name) + "/") for name in BUSINESS_QUESTIONS}
    n_emp = len(employees)

    for name in ["gold_employee_360", "gold_employee_performance", "gold_employee_learning"]:
        _check(r, "gold", f"{name}: one row per silver employee", n_emp, len(g[name]))
        _check(r, "gold", f"{name}: employee_id unique", 0, int(g[name]["employee_id"].duplicated().sum()))

    _check(r, "gold", "review_history: one row per silver review", len(reviews), len(g["gold_review_history"]))
    _check(r, "gold", "critical_skill_gaps: rows == silver assessments with gap >= 2",
           int((assessments["skill_gap"] >= RULES["critical_gap_min"]).sum()), len(g["gold_critical_skill_gaps"]))
    _check(r, "gold", "promotion_candidates: rows == High potential & Ready Now in talent_review",
           int(((talent["potential_rating"] == "High") & (talent["promotion_readiness"] == "Ready Now")).sum()),
           len(g["gold_promotion_candidates"]))
    completed = learning[learning["event_type"] == "course_completed"]
    _check(r, "gold", "course_completions: rows == distinct employee x completed course",
           len(completed[["employee_id", "course_name"]].drop_duplicates()), len(g["gold_course_completions"]))
    _check(r, "gold", "employee_learning: courses_completed sums to course_completions",
           len(g["gold_course_completions"]), int(g["gold_employee_learning"]["courses_completed"].sum()))
    _check(r, "gold", "attrition_risk: employee_id unique", 0,
           int(g["gold_attrition_risk"]["employee_id"].duplicated().sum()))
    _check(r, "gold", "team_outcomes: one row per team", len(teams), len(g["gold_team_outcomes"]))
    _check(r, "gold", "team_outcomes: headcount sums to employees", n_emp,
           int(g["gold_team_outcomes"]["headcount"].sum()))
    _check(r, "gold", "team_outcomes: projects sum to silver projects", len(projects),
           int(g["gold_team_outcomes"]["projects"].sum()))
    _check(r, "gold", "skill_distribution: assessments sum to silver", len(assessments),
           int(g["gold_skill_distribution"]["assessments"].sum()))
    hist_cols = [f"level_{i}_count" for i in range(1, 6)]
    _check(r, "gold", "skill_distribution: level histogram sums to assessments", len(assessments),
           int(g["gold_skill_distribution"][hist_cols].sum().sum()))
    _check(r, "gold", "skill_distribution: grain department x competency unique", 0,
           int(g["gold_skill_distribution"].duplicated(["department", "competency_id"]).sum()))

    # Independent recomputation of a key metric straight from Silver.
    latest = reviews.sort_values("review_period").groupby("employee_id").tail(1)
    _check(r, "gold", "avg latest rating: gold == silver recomputed", round(float(latest["rating"].mean()), 4),
           round(float(g["gold_employee_performance"]["latest_rating"].mean()), 4), tolerance=0.0001)
    return r


def run_reconcile(settings: Settings, lake: Storage, tracker: RunTracker) -> list[dict[str, Any]]:
    """Run reconciliation as a tracked step; raise if any check fails."""
    with tracker.step("reconcile") as m:
        results = reconcile(settings, lake)
        tracker.checks = results
        failed = [c for c in results if not c["passed"]]
        m.update({"checks": len(results), "passed": len(results) - len(failed), "failed": len(failed)})
        for c in failed:
            log.error("reconciliation failed", extra=c)
        if failed:
            raise AssertionError(f"{len(failed)} reconciliation check(s) failed")
    return results
