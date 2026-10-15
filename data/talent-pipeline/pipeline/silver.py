"""Silver layer: cleaned, typed, de-duplicated, conformed data.

For each table: read Bronze -> apply the contract (types, nulls, domains,
foreign keys) -> write valid rows to ``silver/<table>/`` and rejected rows to
``quarantine/<table>/run_date=<run_date>/``. Both are full overwrites, so a
rerun produces exactly the same output.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .bronze import BRONZE, LEARNING, bronze_table_prefix
from .config import SOURCE_TABLES, Settings
from .contracts import CONTRACTS
from .observability import RunTracker, get_logger
from .quality import QualityResult, apply_contract
from .storage import Storage

log = get_logger(__name__)

SILVER = "silver"
QUARANTINE = "quarantine"


def silver_prefix(table: str) -> str:
    """Lake prefix for a Silver table."""
    return f"{SILVER}/{table}"


def quarantine_prefix(table: str, run_date: str) -> str:
    """Lake prefix for a table's rejected rows for one run date."""
    return f"{QUARANTINE}/{table}/run_date={run_date}"


# ---------------------------------------------------------------------------
# Derived columns (pure functions -> unit tested)
# ---------------------------------------------------------------------------
def enrich_employees(df: pd.DataFrame, as_of: str) -> pd.DataFrame:
    """Add ``full_name`` and ``tenure_years`` (as of the run date)."""
    out = df.copy()
    out["full_name"] = out["first_name"] + " " + out["last_name"]
    days = (pd.Timestamp(as_of) - out["hire_date"]).dt.days
    out["tenure_years"] = (days / 365.25).round(2).astype("Float64")
    return out


def enrich_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Split ``review_period`` (``2025-H1``) and flag high performers (rating >= 4)."""
    out = df.copy()
    out["review_year"] = out["review_period"].str[:4].astype("Int64")
    out["review_half"] = out["review_period"].str[-2:]
    out["is_high_performer"] = (out["rating"] >= 4).astype("boolean")
    return out


def enrich_assessments(df: pd.DataFrame) -> pd.DataFrame:
    """``skill_gap = required_level - proficiency_level`` (positive means below requirement)."""
    out = df.copy()
    out["skill_gap"] = (out["required_level"] - out["proficiency_level"]).astype("Int64")
    out["meets_requirement"] = (out["skill_gap"] <= 0).astype("boolean")
    return out


def enrich_learning(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar keys used for partitioning and monthly aggregation."""
    out = df.copy()
    out["event_year"] = out["event_date"].dt.year.astype("Int64")
    out["event_month"] = out["event_date"].dt.strftime("%Y-%m")
    return out


ENRICHERS = {
    "performance_reviews": enrich_reviews,
    "competency_assessments": enrich_assessments,
}


# ---------------------------------------------------------------------------
# Learning activity parsing
# ---------------------------------------------------------------------------
def parse_learning_lines(lines: list[tuple[str, int, str]]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Parse raw JSONL lines into a DataFrame.

    Args:
        lines: ``(source_file, line_number, raw_line)`` tuples.

    Returns:
        ``(records, malformed, metrics)``. Truncated or invalid JSON goes to
        ``malformed`` with the raw text kept for troubleshooting. Records that
        use the legacy ``notes`` field are mapped onto ``text`` (schema drift).
    """
    expected = set(CONTRACTS[LEARNING].columns)
    records: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    blank = drift = 0
    unknown_fields: dict[str, int] = {}
    for source_file, line_no, raw in lines:
        if not raw.strip():
            blank += 1
            continue
        try:
            obj = json.loads(raw)
            if not isinstance(obj, dict):
                raise ValueError("JSON value is not an object")
        except (json.JSONDecodeError, ValueError) as exc:
            malformed.append(
                {"_source_file": source_file, "_line_number": line_no, "raw_line": raw[:2000],
                 "_dq_reasons": f"malformed_json:{exc.__class__.__name__}"}
            )
            continue
        if "text" not in obj and "notes" in obj:
            obj["text"] = obj.pop("notes")
            drift += 1
        for key in obj.keys() - expected:
            unknown_fields[key] = unknown_fields.get(key, 0) + 1
        record = {col: (None if obj.get(col) is None else str(obj.get(col))) for col in expected}
        record["_source_file"] = source_file
        record["_line_number"] = line_no
        records.append(record)

    cols = list(CONTRACTS[LEARNING].columns) + ["_source_file", "_line_number"]
    records_df = pd.DataFrame(records, columns=cols)
    records_df[list(CONTRACTS[LEARNING].columns)] = records_df[list(CONTRACTS[LEARNING].columns)].astype("string")
    records_df["_line_number"] = records_df["_line_number"].astype("Int64")
    malformed_df = pd.DataFrame(malformed, columns=["_source_file", "_line_number", "raw_line", "_dq_reasons"])
    metrics = {
        "lines_total": len(lines),
        "lines_blank": blank,
        "lines_malformed": len(malformed),
        "records_parsed": len(records),
        "schema_drift_notes_to_text": drift,
        "unknown_fields": unknown_fields,
    }
    if drift:
        log.warning("schema drift: 'notes' field mapped to 'text'", extra={"records": drift})
    return records_df, malformed_df, metrics


def _write_outputs(lake: Storage, table: str, result: QualityResult, settings: Settings,
                   extra_rejected: pd.DataFrame | None = None) -> dict[str, Any]:
    """Write valid rows to Silver and rejected rows to quarantine (both idempotent)."""
    valid = result.valid.copy()
    valid["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
    valid["_silver_run_id"] = settings.run_id

    lake.delete_prefix(silver_prefix(table))
    if table == LEARNING:
        # Partition by event year: queries for one period read only that folder.
        for year, part in valid.groupby("event_year", dropna=False):
            lake.write_parquet(part, f"{silver_prefix(table)}/event_year={year}/part-000.parquet")
    else:
        lake.write_parquet(valid, f"{silver_prefix(table)}/part-000.parquet")

    rejected = result.rejected
    if extra_rejected is not None and len(extra_rejected):
        rejected = pd.concat([extra_rejected, rejected], ignore_index=True)
    q_prefix = quarantine_prefix(table, settings.run_date)
    lake.delete_prefix(q_prefix)
    if len(rejected):
        lake.write_parquet(rejected.astype("string"), f"{q_prefix}/rejected.parquet")
    return {"rows_written": int(len(valid)), "quarantine_rows": int(len(rejected)),
            "path": f"{lake.uri}/{silver_prefix(table)}/"}


def transform_tables(settings: Settings, lake: Storage, tracker: RunTracker) -> dict[str, pd.DataFrame]:
    """Bronze -> Silver for the seven relational tables; returns the valid frames."""
    references: dict[str, pd.DataFrame] = {}
    for table in SOURCE_TABLES:
        with tracker.step(f"silver.{table}") as m:
            bronze = lake.read_parquet(bronze_table_prefix(table, settings.run_date))
            result = apply_contract(bronze, CONTRACTS[table], references)
            if table == "employees":
                result.valid = enrich_employees(result.valid, settings.run_date)
            elif table in ENRICHERS:
                result.valid = ENRICHERS[table](result.valid)
            m["rows_read"] = int(len(bronze))
            m.update(result.metrics)
            m.update(_write_outputs(lake, table, result, settings))
            lake.write_json(m, f"_metadata/silver/{table}.json")
            if result.valid.empty and settings.fail_on_empty:
                raise ValueError(f"silver.{table} has zero valid rows")
            references[table] = result.valid
    return references


def transform_learning(settings: Settings, lake: Storage, tracker: RunTracker,
                       references: dict[str, pd.DataFrame]) -> None:
    """Bronze -> Silver for the semi-structured learning events."""
    with tracker.step(f"silver.{LEARNING}") as m:
        lines: list[tuple[str, int, str]] = []
        for key in lake.list_keys(f"{BRONZE}/{LEARNING}/"):
            if not key.endswith(".jsonl"):
                continue
            text = lake.get_bytes(key).decode("utf-8", errors="replace")
            rel = key[len(f"{BRONZE}/{LEARNING}/"):]
            lines.extend((rel, i + 1, line) for i, line in enumerate(text.splitlines()))
        records, malformed, parse_metrics = parse_learning_lines(lines)
        m.update(parse_metrics)

        result = apply_contract(records, CONTRACTS[LEARNING], {"employees": references["employees"]})
        result.valid = enrich_learning(result.valid)
        part_date = (result.valid["_source_file"].str.extract(r"year=(\d{4})/month=(\d{2})/day=(\d{2})")
                     .fillna("").agg("-".join, axis=1))
        m["event_date_partition_mismatch"] = int(
            (result.valid["event_date"].dt.strftime("%Y-%m-%d") != part_date).sum())
        m.update(result.metrics)
        m.update(_write_outputs(lake, LEARNING, result, settings, extra_rejected=malformed))
        lake.write_json(m, f"_metadata/silver/{LEARNING}.json")


def run_silver(settings: Settings, lake: Storage, tracker: RunTracker) -> dict[str, pd.DataFrame]:
    """Run the whole Silver layer."""
    references = transform_tables(settings, lake, tracker)
    transform_learning(settings, lake, tracker, references)
    return references
