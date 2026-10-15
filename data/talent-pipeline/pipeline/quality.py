"""Apply a :class:`TableContract` to a DataFrame.

Bad rows are never silently dropped and never crash the pipeline: they are
returned in ``rejected`` with a ``_dq_reasons`` column explaining every rule
they broke, and the caller writes them to the quarantine area of the lake.

Invariant (checked by reconciliation)::

    rows_in == rows_valid + rows_rejected + exact_duplicates_dropped
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from .contracts import TableContract
from .observability import get_logger

log = get_logger(__name__)


class SchemaContractError(Exception):
    """Raised when required columns are missing entirely (schema drift)."""


@dataclass
class QualityResult:
    """Output of :func:`apply_contract`."""

    valid: pd.DataFrame
    rejected: pd.DataFrame
    metrics: dict[str, Any] = field(default_factory=dict)


def normalise_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and turn empty strings into nulls for every text column."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object or pd.api.types.is_string_dtype(out[col]):
            s = out[col].astype("string").str.strip()
            out[col] = s.mask(s == "")
    return out


def cast_column(series: pd.Series, kind: str) -> pd.Series:
    """Cast to the contract type; values that cannot be cast become null."""
    if kind == "int":
        numeric = pd.to_numeric(series, errors="coerce")
        integral = numeric.isna() | (numeric % 1 == 0)
        return numeric.where(integral).astype("Int64")
    if kind == "float":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    if kind == "date":
        return pd.to_datetime(series, format="%Y-%m-%d", errors="coerce").astype("datetime64[ns]")
    if kind == "datetime":
        return pd.to_datetime(series, format="ISO8601", errors="coerce").astype("datetime64[ns]")
    if kind == "str":
        return series.astype("string")
    raise ValueError(f"unknown contract type: {kind}")


def apply_contract(
    df: pd.DataFrame,
    contract: TableContract,
    references: dict[str, pd.DataFrame] | None = None,
) -> QualityResult:
    """Validate, cast, and de-duplicate ``df`` against ``contract``.

    Args:
        df: Raw rows (any types; usually strings from Bronze).
        contract: The rules to enforce.
        references: Already-validated parent tables, keyed by table name, used
            for foreign-key checks.

    Returns:
        A :class:`QualityResult` with typed valid rows, rejected rows (raw values
        plus ``_dq_reasons``), and metrics.
    """
    references = references or {}
    missing = [c for c in contract.columns if c not in df.columns]
    if missing:
        raise SchemaContractError(f"{contract.name}: missing columns {missing}")

    contract_cols = list(contract.columns)
    lineage_cols = [c for c in df.columns if c.startswith("_")]
    unexpected = [c for c in df.columns if c not in contract.columns and not c.startswith("_")]
    if unexpected:
        log.warning("unexpected columns dropped", extra={"table": contract.name, "columns": unexpected})

    raw = normalise_strings(df[contract_cols + lineage_cols]).reset_index(drop=True)
    typed = raw.copy()
    reasons: list[list[str]] = [[] for _ in range(len(raw))]

    def flag(mask: pd.Series, reason: str) -> None:
        for idx in mask[mask.fillna(False).astype(bool)].index:
            reasons[idx].append(reason)

    for col, kind in contract.columns.items():
        typed[col] = cast_column(raw[col], kind)
        flag(raw[col].notna() & typed[col].isna(), f"invalid_type:{col}")

    for col in contract.required:
        flag(raw[col].isna(), f"missing_required:{col}")

    for rule in contract.rules:
        flag(~rule.check(typed).fillna(False).astype(bool), f"rule:{rule.name}")

    for fk in contract.foreign_keys:
        parent = references.get(fk.ref_table)
        if parent is None:
            log.warning("fk check skipped: parent not provided", extra={"table": contract.name, "fk": fk.column})
            continue
        known = set(parent[fk.ref_column].dropna().astype("int64").tolist())
        flag(typed[fk.column].notna() & ~typed[fk.column].isin(known), f"fk:{fk.column}->{fk.ref_table}")

    bad = pd.Series([bool(r) for r in reasons], index=raw.index)
    rejected = raw[bad].copy()
    rejected["_dq_reasons"] = [";".join(r) for r, b in zip(reasons, bad) if b]
    valid = typed[~bad].copy()

    # Exact duplicates (same business content) are dropped; conflicting rows that
    # share a primary key are quarantined so nothing is lost silently.
    before = len(valid)
    valid = valid.drop_duplicates(subset=contract_cols, keep="first")
    exact_dupes = before - len(valid)
    pk_dupe_mask = valid.duplicated(subset=list(contract.primary_key), keep="first")
    if pk_dupe_mask.any():
        conflicts = raw.loc[valid[pk_dupe_mask].index].copy()
        conflicts["_dq_reasons"] = "duplicate_primary_key"
        rejected = pd.concat([rejected, conflicts], ignore_index=True)
        valid = valid[~pk_dupe_mask]

    reason_counts = Counter(r for row in rejected["_dq_reasons"] for r in str(row).split(";") if r)
    metrics = {
        "rows_in": int(len(df)),
        "rows_valid": int(len(valid)),
        "rows_rejected": int(len(rejected)),
        "exact_duplicates_dropped": int(exact_dupes),
        "reject_reasons": dict(sorted(reason_counts.items())),
    }
    if len(rejected):
        log.warning("rows quarantined", extra={"table": contract.name, **metrics})
    return QualityResult(valid=valid.reset_index(drop=True), rejected=rejected.reset_index(drop=True), metrics=metrics)
