"""Unit tests for the Gold business metrics (one block per business question).

The fixture is small enough that every expected value below was worked out by hand.
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.gold import attrition_signals, build_gold, rating_history, trend_label


def _i(*v: object) -> pd.arrays.IntegerArray:
    return pd.array(list(v), dtype="Int64")


def _silver() -> dict[str, pd.DataFrame]:
    teams = pd.DataFrame({"team_id": _i(1, 2, 3), "name": ["A", "B", "C"], "department": ["Eng", "Sales", "Sales"]})
    employees = pd.DataFrame({
        "employee_id": _i(1, 2, 3, 4), "first_name": ["a", "b", "c", "d"], "last_name": ["x"] * 4,
        "email": ["e"] * 4, "team_id": _i(1, 1, 2, 2), "job_level": ["IC1", "Manager", "IC2", "Director"],
        "hire_date": pd.to_datetime(["2020-01-01"] * 4), "full_name": ["a x", "b x", "c x", "d x"],
        "tenure_years": [5.0] * 4,
    })
    reviews = pd.DataFrame({
        "review_id": _i(1, 2, 3, 4, 5, 6, 7, 8), "employee_id": _i(1, 1, 2, 2, 3, 3, 4, 4),
        "review_period": ["2024-H2", "2025-H1"] * 4, "rating": _i(3, 5, 4, 4, 3, 1, 4, 5),
        "manager_comment": ["ok", "great", "good", "good", "fine", "poor", "good", "top"],
        "submitted_date": pd.to_datetime(["2025-01-10", "2025-07-10"] * 4),
    })
    competencies = pd.DataFrame({"competency_id": _i(1, 2), "name": ["Python", "Negotiation"],
                                 "category": ["Technical", "Domain"]})
    assessments = pd.DataFrame({
        "assessment_id": _i(1, 2, 3, 4, 5), "employee_id": _i(1, 2, 3, 4, 2), "competency_id": _i(1, 1, 2, 2, 2),
        "proficiency_level": _i(2, 4, 1, 3, 1), "required_level": _i(4, 3, 3, 3, 3), "skill_gap": _i(2, -1, 2, 0, 2),
        "assessed_date": pd.to_datetime(["2025-05-01"] * 5),
    })
    talent = pd.DataFrame({
        "employee_id": _i(1, 2, 3, 4), "review_cycle": ["2025-H1"] * 4,
        "potential_rating": ["High", "High", "Low", "High"],
        "promotion_readiness": ["Ready Now", "Ready Now", "Not Ready", "Ready Now"],
        "attrition_risk": ["High", "Medium", "High", "Low"],
    })
    projects = pd.DataFrame({"project_id": _i(1, 2, 3), "team_id": _i(1, 1, 2),
                             "outcome_status": ["on-time", "delayed", "on-time"], "outcome_score": [80.0, 50.0, 90.0]})
    learning = pd.DataFrame({
        "event_id": ["E1", "E2", "E3", "E4", "E5", "E6"], "employee_id": _i(1, 1, 2, 4, 1, 4),
        "event_date": pd.to_datetime(["2025-01-10", "2025-01-10", "2025-01-10", "2025-02-03", "2025-02-03",
                                      "2025-02-01"]),
        "event_type": ["enrollment", "course_completed", "quiz_passed", "certification_earned", "course_completed",
                       "course_completed"],
        "course_name": ["SQL", "SQL", "SQL", "SQL", "Python", "SQL"], "score": [None, None, 90.0, None, None, None],
    })
    return {"teams": teams, "employees": employees, "performance_reviews": reviews, "competencies": competencies,
            "competency_assessments": assessments, "talent_review": talent, "projects": projects,
            "learning_activity": learning}


@pytest.fixture(scope="module")
def gold() -> dict[str, pd.DataFrame]:
    return build_gold(_silver(), "2025-12-31")


# ---- shared building blocks ----------------------------------------------
def test_rating_history_latest_previous_and_trend() -> None:
    h = rating_history(_silver()["performance_reviews"])
    assert (h.loc[1, "latest_rating"], h.loc[1, "previous_rating"], h.loc[1, "rating_change"]) == (5, 3, 2)
    assert h.loc[1, "avg_rating"] == 4.0 and h.loc[1, "latest_manager_comment"] == "great"
    assert h.loc[3, "performance_trend"] == "Declining" and h.loc[2, "performance_trend"] == "Stable"


def test_single_review_has_no_previous() -> None:
    reviews = pd.DataFrame({"employee_id": _i(9), "review_period": ["2025-H1"], "rating": _i(4),
                            "manager_comment": ["x"]})
    h = rating_history(reviews)
    assert h.loc[9, "latest_rating"] == 4 and pd.isna(h.loc[9, "previous_rating"])
    assert h.loc[9, "performance_trend"] == "New (one review)"


def test_trend_label() -> None:
    assert trend_label(pd.Series([1, 0, -1, None], dtype="Int64")).tolist() == [
        "Improving", "Stable", "Declining", "New (one review)"]


# ---- Q1 -------------------------------------------------------------------
def test_q1_employee_performance_wide(gold: dict[str, pd.DataFrame]) -> None:
    p = gold["gold_employee_performance"].set_index("employee_id")
    assert len(p) == 4
    assert (p.loc[1, "rating_2024_h2"], p.loc[1, "rating_2025_h1"]) == (3, 5)
    assert p.loc[3, "rating_change"] == -2 and p.loc[1, "reviews_count"] == 2


def test_q1_review_history_long(gold: dict[str, pd.DataFrame]) -> None:
    h = gold["gold_review_history"]
    assert len(h) == 8
    emp3 = h[h["employee_id"] == 3]
    assert emp3["review_sequence"].tolist() == [1, 2]
    assert pd.isna(emp3["change_vs_previous"].iloc[0]) and emp3["change_vs_previous"].iloc[1] == -2


# ---- Q2 -------------------------------------------------------------------
def test_q2_critical_skill_gaps(gold: dict[str, pd.DataFrame]) -> None:
    c = gold["gold_critical_skill_gaps"]
    assert sorted(c["employee_id"].tolist()) == [1, 2, 3]  # gap >= 2 only
    row = c.set_index("employee_id").loc[1]
    assert row["severity"] == "Critical - key competency" and bool(row["is_key_competency"])
    assert c.set_index("employee_id").loc[3, "severity"] == "Critical"
    assert c.iloc[0]["employee_id"] == 1  # key competency (required 4) sorted first


# ---- Q3 -------------------------------------------------------------------
def test_q3_promotion_candidates(gold: dict[str, pd.DataFrame]) -> None:
    p = gold["gold_promotion_candidates"].set_index("employee_id")
    assert sorted(p.index) == [1, 2, 4]  # High potential AND Ready Now
    assert p.loc[4, "recommendation"] == "Promote now"  # rating 5, no critical gaps
    assert p.loc[1, "recommendation"] == "Promote after closing skill gap"
    assert p.loc[2, "recommendation"] == "Promote after closing skill gap"
    assert bool(p.loc[1, "flight_risk"]) and not bool(p.loc[4, "flight_risk"])
    assert gold["gold_promotion_candidates"]["employee_id"].iloc[0] == 4


# ---- Q4 -------------------------------------------------------------------
def test_q4_employee_learning(gold: dict[str, pd.DataFrame]) -> None:
    lr = gold["gold_employee_learning"].set_index("employee_id")
    assert len(lr) == 4
    assert lr.loc[1, "courses_completed"] == 2 and lr.loc[1, "completed_courses_list"] == "Python; SQL"
    assert lr.loc[3, "learning_events"] == 0 and lr.loc[3, "completed_courses_list"] == ""
    assert lr.loc[4, "certifications_list"] == "SQL"
    assert lr.loc[2, "quiz_pass_rate_pct"] == 100.0


def test_q4_course_completions(gold: dict[str, pd.DataFrame]) -> None:
    c = gold["gold_course_completions"]
    assert len(c) == 3
    emp4 = c[c["employee_id"] == 4].iloc[0]
    assert bool(emp4["certified"]) and emp4["certified_on"] == pd.Timestamp("2025-02-03")
    assert not c[c["employee_id"] == 1]["certified"].any()


# ---- Q5 -------------------------------------------------------------------
def test_q5_attrition_signals_scoring() -> None:
    e360 = pd.DataFrame({
        "rating_change": _i(-2, 0, -1, None), "latest_rating": _i(1, 4, 3, 5),
        "attrition_risk": ["High", "Low", "Low", "High"], "learning_events_last_12m": _i(0, 3, 3, 3),
    })
    s = attrition_signals(e360)
    assert s["risk_score"].tolist() == [5, 0, 1, 1]
    assert s["computed_risk_level"].tolist() == ["High", "Low", "Low", "Low"]
    assert s["risk_reasons"].iloc[2] == "Rating declined"
    assert s["risk_reasons"].iloc[1] == ""


def test_q5_attrition_risk_table(gold: dict[str, pd.DataFrame]) -> None:
    a = gold["gold_attrition_risk"].set_index("employee_id")
    assert list(a.index) == [3]  # declined 3 -> 1, low rating, flagged High
    assert a.loc[3, "risk_score"] == 5  # + no learning in last 12 months and a.loc[3, "computed_risk_level"] == "High"
    assert not bool(a.loc[3, "regretted_loss"])


# ---- Q6 -------------------------------------------------------------------
def test_q6_team_outcomes(gold: dict[str, pd.DataFrame]) -> None:
    t = gold["gold_team_outcomes"].set_index("team_id")
    assert len(t) == 3
    assert t.loc[1, "on_time_rate_pct"] == 50.0 and t.loc[1, "avg_project_outcome_score"] == 65.0
    assert t.loc[1, "avg_latest_rating"] == 4.5 and t.loc[1, "delivery_status"] == "Some delayed"
    assert t.loc[2, "delivery_status"] == "All on time" and t.loc[2, "avg_latest_rating"] == 3.0
    assert t.loc[3, "headcount"] == 0 and t.loc[3, "delivery_status"] == "No projects"


# ---- Q7 -------------------------------------------------------------------
def test_q7_skill_distribution(gold: dict[str, pd.DataFrame]) -> None:
    s = gold["gold_skill_distribution"].set_index(["department", "competency_name"])
    eng = s.loc[("Eng", "Python")]
    assert eng["assessments"] == 2 and eng["level_2_count"] == 1 and eng["level_4_count"] == 1
    assert eng["pct_meeting_requirement"] == 50.0 and eng["pct_expert_4_5"] == 50.0 and eng["critical_gaps"] == 1
    sales = s.loc[("Sales", "Negotiation")]
    assert sales["assessments"] == 2 and sales["critical_gaps"] == 1
    assert s.filter(like="level_").sum().sum() == 5
