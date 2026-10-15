"""Unit tests for Silver parsing and derived columns."""

from __future__ import annotations

import json

import pandas as pd

from pipeline.silver import enrich_assessments, enrich_employees, enrich_learning, enrich_reviews, parse_learning_lines


def test_parse_learning_handles_malformed_blank_and_schema_drift() -> None:
    good = json.dumps({"event_id": "EV1", "employee_id": "1", "event_date": "2025-01-01T00:00:00",
                       "event_type": "enrollment", "course_name": "SQL", "text": "hi", "score": None})
    drift = json.dumps({"event_id": "EV2", "employee_id": "2", "event_date": "2025-01-01T00:00:00",
                        "event_type": "enrollment", "course_name": "SQL", "notes": "from notes", "score": 50,
                        "extra": 1})
    lines = [("f", 1, good), ("f", 2, ""), ("f", 3, '{"event_id": "EV3", "emp'), ("f", 4, drift), ("f", 5, "[1, 2]")]
    records, malformed, m = parse_learning_lines(lines)
    assert m == {"lines_total": 5, "lines_blank": 1, "lines_malformed": 2, "records_parsed": 2,
                 "schema_drift_notes_to_text": 1, "unknown_fields": {"extra": 1}}
    assert records.loc[records.event_id == "EV2", "text"].iloc[0] == "from notes"
    assert records.loc[records.event_id == "EV2", "score"].iloc[0] == "50"
    assert set(malformed["_line_number"]) == {3, 5}
    assert malformed["raw_line"].iloc[0].startswith('{"event_id": "EV3"')


def test_skill_gap_is_required_minus_proficiency() -> None:
    df = pd.DataFrame({"proficiency_level": pd.array([2, 4, 3], dtype="Int64"),
                       "required_level": pd.array([4, 3, 3], dtype="Int64")})
    out = enrich_assessments(df)
    assert out["skill_gap"].tolist() == [2, -1, 0]
    assert out["meets_requirement"].tolist() == [False, True, True]


def test_review_period_split_and_high_performer_flag() -> None:
    out = enrich_reviews(pd.DataFrame({"review_period": pd.array(["2024-H2", "2025-H1"], dtype="string"),
                                       "rating": pd.array([3, 4], dtype="Int64")}))
    assert out["review_year"].tolist() == [2024, 2025]
    assert out["review_half"].tolist() == ["H2", "H1"]
    assert out["is_high_performer"].tolist() == [False, True]


def test_employee_tenure_and_full_name() -> None:
    df = pd.DataFrame({"first_name": ["Ann"], "last_name": ["Lee"], "hire_date": pd.to_datetime(["2020-01-01"])})
    out = enrich_employees(df, "2025-01-01")
    assert out["full_name"].iloc[0] == "Ann Lee"
    assert abs(out["tenure_years"].iloc[0] - 5.0) < 0.01


def test_learning_calendar_keys() -> None:
    out = enrich_learning(pd.DataFrame({"event_date": pd.to_datetime(["2024-03-05T10:00:00"])}))
    assert out["event_year"].iloc[0] == 2024 and out["event_month"].iloc[0] == "2024-03"
