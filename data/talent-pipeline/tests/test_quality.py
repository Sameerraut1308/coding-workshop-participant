"""Unit tests for schema contracts and the data-quality engine."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.contracts import CONTRACTS
from pipeline.quality import SchemaContractError, apply_contract, cast_column, normalise_strings


def _employees(rows: list[dict]) -> pd.DataFrame:
    cols = list(CONTRACTS["employees"].columns)
    return pd.DataFrame(rows, columns=cols).astype("string")


TEAMS = pd.DataFrame({"team_id": pd.array([1, 2], dtype="Int64")})
GOOD = {"employee_id": "1", "first_name": "Ann", "last_name": "Lee", "email": "a@x.io", "team_id": "1",
        "job_level": "IC1", "hire_date": "2020-01-01"}


def test_valid_rows_are_cast_to_contract_types() -> None:
    res = apply_contract(_employees([GOOD]), CONTRACTS["employees"], {"teams": TEAMS})
    assert len(res.valid) == 1 and res.rejected.empty
    assert str(res.valid["employee_id"].dtype) == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(res.valid["hire_date"])


@pytest.mark.parametrize(
    "override, reason",
    [
        ({"employee_id": "abc"}, "invalid_type:employee_id"),
        ({"employee_id": "1.5"}, "invalid_type:employee_id"),
        ({"hire_date": "2024-02-30"}, "invalid_type:hire_date"),
        ({"hire_date": "01/02/2024"}, "invalid_type:hire_date"),
        ({"first_name": "   "}, "missing_required:first_name"),
        ({"email": None}, "missing_required:email"),
        ({"email": "not-an-email"}, "rule:email_format"),
        ({"job_level": "Wizard"}, "rule:job_level_in_allowed_values"),
        ({"team_id": "99"}, "fk:team_id->teams"),
    ],
)
def test_bad_rows_are_quarantined_with_reason(override: dict, reason: str) -> None:
    res = apply_contract(_employees([GOOD, {**GOOD, "employee_id": "2", **override}]), CONTRACTS["employees"],
                         {"teams": TEAMS})
    assert len(res.valid) == 1
    assert len(res.rejected) == 1
    assert reason in res.rejected.loc[0, "_dq_reasons"]


def test_multiple_reasons_are_all_recorded() -> None:
    bad = {**GOOD, "email": "nope", "hire_date": "yesterday", "team_id": "99"}
    res = apply_contract(_employees([bad]), CONTRACTS["employees"], {"teams": TEAMS})
    reasons = set(res.rejected.loc[0, "_dq_reasons"].split(";"))
    assert reasons == {"rule:email_format", "invalid_type:hire_date", "fk:team_id->teams"}
    assert res.metrics["reject_reasons"]["fk:team_id->teams"] == 1


def test_exact_duplicates_dropped_and_pk_conflicts_quarantined() -> None:
    rows = [GOOD, GOOD, {**GOOD, "first_name": "Different"}]
    res = apply_contract(_employees(rows), CONTRACTS["employees"], {"teams": TEAMS})
    assert len(res.valid) == 1
    assert res.metrics["exact_duplicates_dropped"] == 1
    assert res.rejected["_dq_reasons"].tolist() == ["duplicate_primary_key"]


def test_row_count_invariant_holds() -> None:
    rows = [GOOD, GOOD, {**GOOD, "employee_id": "2", "email": "bad"}, {**GOOD, "employee_id": "3"}]
    m = apply_contract(_employees(rows), CONTRACTS["employees"], {"teams": TEAMS}).metrics
    assert m["rows_in"] == m["rows_valid"] + m["rows_rejected"] + m["exact_duplicates_dropped"]


def test_missing_column_raises_schema_contract_error() -> None:
    df = _employees([GOOD]).drop(columns=["email"])
    with pytest.raises(SchemaContractError, match="email"):
        apply_contract(df, CONTRACTS["employees"], {"teams": TEAMS})


def test_unexpected_columns_dropped_lineage_columns_kept() -> None:
    df = _employees([GOOD])
    df["surprise"] = "x"
    df["_source"] = "csv"
    res = apply_contract(df, CONTRACTS["employees"], {"teams": TEAMS})
    assert "surprise" not in res.valid.columns and "_source" in res.valid.columns


def test_fk_check_skipped_when_parent_missing() -> None:
    res = apply_contract(_employees([{**GOOD, "team_id": "99"}]), CONTRACTS["employees"], {})
    assert len(res.valid) == 1


def test_domain_ranges_for_reviews() -> None:
    reviews = pd.DataFrame(
        {"review_id": ["1", "2", "3"], "employee_id": ["1", "1", "1"],
         "review_period": ["2025-H1", "2025-H3", "2025-H1"], "rating": ["5", "3", "0"],
         "manager_comment": [None, "x", "y"], "submitted_date": ["2025-07-01"] * 3}
    ).astype("string")
    res = apply_contract(reviews, CONTRACTS["performance_reviews"])
    assert res.valid["review_id"].tolist() == [1]
    assert sorted(res.rejected["_dq_reasons"]) == ["rule:rating_between_1_5", "rule:review_period_format_YYYY-Hn"]


def test_cast_column_helpers() -> None:
    assert cast_column(pd.Series(["1", "x", None]), "int").tolist()[0] == 1
    assert cast_column(pd.Series(["2.5"]), "float").iloc[0] == 2.5
    assert pd.isna(cast_column(pd.Series(["2024-13-01"]), "date").iloc[0])
    assert cast_column(pd.Series(["2024-01-02T10:00:00"]), "datetime").iloc[0] == pd.Timestamp("2024-01-02 10:00")
    with pytest.raises(ValueError):
        cast_column(pd.Series(["1"]), "complex")


def test_normalise_strings_trims_and_nulls_blanks() -> None:
    out = normalise_strings(pd.DataFrame({"a": [" x ", "", None]}))
    assert out["a"].iloc[0] == "x" and out["a"].isna().sum() == 2
