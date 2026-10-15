"""Gold layer: business-ready tables, one (or two) per business question.

Business questions (from the workshop brief):

* Q1 What are the performance ratings and review history for each employee?
* Q2 Which employees have critical skill gaps in key competencies?
* Q3 Who are the high-potential employees ready for promotion?
* Q4 What training and development activities have been completed by each employee?
* Q5 Which employees are at risk of attrition based on performance trends?
* Q6 How does employee performance correlate with team and project outcomes?
* Q7 What is the distribution of skills across the organization?

Every per-employee metric (latest rating, trend, gap counts, learning counts)
is computed once in ``gold_employee_360`` and reused, so definitions never
drift between tables. Business thresholds live in :data:`RULES`.
"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from .config import Settings
from .observability import RunTracker, get_logger
from .silver import LEARNING, silver_prefix
from .storage import Storage

log = get_logger(__name__)

GOLD = "gold"

# Tunable business definitions (documented in the README).
RULES = {
    "high_performer_min_rating": 4,      # rating >= 4
    "low_performer_max_rating": 2,       # rating <= 2
    "critical_gap_min": 2,               # required - proficiency >= 2 levels
    "key_competency_min_required": 4,    # required level >= 4 means the role depends on it
    "learning_inactive_months": 12,      # no learning in the last N months
}

BUSINESS_QUESTIONS: dict[str, str] = {
    "gold_employee_performance": "Q1. Performance ratings and review history - one row per employee",
    "gold_review_history": "Q1. Review history - one row per employee per review period",
    "gold_critical_skill_gaps": "Q2. Critical skill gaps - one row per employee x competency with a critical gap",
    "gold_promotion_candidates": "Q3. High-potential employees ready for promotion",
    "gold_employee_learning": "Q4. Training and development completed - one row per employee",
    "gold_course_completions": "Q4. Courses completed - one row per employee x course",
    "gold_attrition_risk": "Q5. Employees at risk of attrition based on performance trends",
    "gold_team_outcomes": "Q6. Team performance vs project outcomes - one row per team",
    "gold_skill_distribution": "Q7. Distribution of skills - one row per department x competency",
    "gold_employee_360": "Conformed employee-level view used by every question above",
}


def gold_prefix(table: str) -> str:
    """Lake prefix for a Gold table."""
    return f"{GOLD}/{table}"


def load_silver(lake: Storage) -> dict[str, pd.DataFrame]:
    """Read every Silver table needed by Gold."""
    names = ["teams", "competencies", "employees", "projects", "performance_reviews",
             "competency_assessments", "talent_review", LEARNING]
    return {n: lake.read_parquet(silver_prefix(n) + "/") for n in names}


def _pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (100.0 * numerator / denominator.where(denominator != 0)).round(1)


def _count(series: pd.Series, condition: Callable[[pd.Series], pd.Series]) -> int:
    return int(condition(series).fillna(False).sum())


def trend_label(change: pd.Series) -> pd.Series:
    """Improving / Stable / Declining / New (only one review)."""
    out = pd.Series("New (one review)", index=change.index, dtype="string")
    out[change > 0] = "Improving"
    out[change == 0] = "Stable"
    out[change < 0] = "Declining"
    return out


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------
def rating_history(reviews: pd.DataFrame) -> pd.DataFrame:
    """Per employee: latest / previous rating, change, average, count, latest comment."""
    ordered = reviews.sort_values(["employee_id", "review_period"])
    grouped = ordered.groupby("employee_id")
    latest = grouped.tail(1).set_index("employee_id")
    previous = grouped.nth(-2).set_index("employee_id") if len(ordered) else latest.iloc[0:0]
    out = pd.DataFrame(index=latest.index)
    out["reviews_count"] = grouped.size()
    out["first_review_period"] = grouped["review_period"].first()
    out["latest_review_period"] = latest["review_period"]
    out["latest_rating"] = latest["rating"].astype("Int64")
    out["previous_review_period"] = previous["review_period"].reindex(out.index)
    out["previous_rating"] = previous["rating"].reindex(out.index).astype("Int64")
    out["rating_change"] = (out["latest_rating"] - out["previous_rating"]).astype("Int64")
    out["avg_rating"] = grouped["rating"].mean().astype("Float64").round(2)
    out["latest_manager_comment"] = latest["manager_comment"]
    out["performance_trend"] = trend_label(out["rating_change"])
    return out


def skill_profile(assessments: pd.DataFrame) -> pd.DataFrame:
    """Per employee: assessments, average gap, gaps below requirement, critical gaps."""
    a = assessments.assign(
        below=assessments["skill_gap"] > 0,
        critical=assessments["skill_gap"] >= RULES["critical_gap_min"],
    )
    g = a.groupby("employee_id")
    return pd.DataFrame({
        "assessments_count": g.size(),
        "avg_proficiency": g["proficiency_level"].mean().astype("Float64").round(2),
        "avg_skill_gap": g["skill_gap"].mean().astype("Float64").round(2),
        "competencies_below_required": g["below"].sum(),
        "critical_skill_gaps": g["critical"].sum(),
    })


def learning_profile(events: pd.DataFrame, as_of: str) -> pd.DataFrame:
    """Per employee learning engagement metrics."""
    e = events
    counts = pd.crosstab(e["employee_id"], e["event_type"])
    for col in ["enrollment", "module_completed", "course_completed", "certification_earned",
                "quiz_attempted", "quiz_passed"]:
        if col not in counts:
            counts[col] = 0
    quiz = e[e["event_type"].isin(["quiz_attempted", "quiz_passed"])].groupby("employee_id")["score"].mean()
    cutoff = pd.Timestamp(as_of) - pd.DateOffset(months=RULES["learning_inactive_months"])
    g = e.groupby("employee_id")
    completed = e[e["event_type"] == "course_completed"].groupby("employee_id")["course_name"]
    certified = e[e["event_type"] == "certification_earned"].groupby("employee_id")["course_name"]
    out = pd.DataFrame({
        "learning_events": g.size(),
        "courses_engaged": g["course_name"].nunique(),
        "enrollments": counts["enrollment"],
        "modules_completed": counts["module_completed"],
        "courses_completed": completed.nunique(),
        "certifications_earned": certified.nunique(),
        "quiz_attempts": counts["quiz_attempted"] + counts["quiz_passed"],
        "quizzes_passed": counts["quiz_passed"],
        "avg_quiz_score": quiz.round(1),
        "first_learning_date": g["event_date"].min(),
        "last_learning_date": g["event_date"].max(),
        "learning_events_last_12m": e[e["event_date"] >= cutoff].groupby("employee_id").size(),
        "completed_courses_list": completed.agg(lambda s: "; ".join(sorted(s.unique()))),
        "certifications_list": certified.agg(lambda s: "; ".join(sorted(s.unique()))),
    })
    out["quiz_pass_rate_pct"] = _pct(out["quizzes_passed"], out["quiz_attempts"])
    return out


def build_employee_360(s: dict[str, pd.DataFrame], as_of: str) -> pd.DataFrame:
    """One row per employee joining every Silver source (the conformed view)."""
    emp = s["employees"].merge(s["teams"].rename(columns={"name": "team_name"}), on="team_id", how="left")
    emp = emp.set_index("employee_id")
    talent = s["talent_review"].set_index("employee_id")[
        ["review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"]]
    out = (
        emp[["full_name", "email", "team_id", "team_name", "department", "job_level", "hire_date", "tenure_years"]]
        .join(rating_history(s["performance_reviews"]))
        .join(skill_profile(s["competency_assessments"]))
        .join(talent)
        .join(learning_profile(s[LEARNING], as_of))
    )
    zero_fill = ["learning_events", "courses_engaged", "enrollments", "modules_completed", "courses_completed",
                 "certifications_earned", "quiz_attempts", "quizzes_passed", "learning_events_last_12m",
                 "assessments_count", "competencies_below_required", "critical_skill_gaps", "reviews_count"]
    for col in zero_fill:
        out[col] = out[col].fillna(0).astype("Int64")
    out["is_high_performer"] = (out["latest_rating"] >= RULES["high_performer_min_rating"]).astype("boolean")
    out["is_low_performer"] = (out["latest_rating"] <= RULES["low_performer_max_rating"]).astype("boolean")
    return out.reset_index()


# ---------------------------------------------------------------------------
# Q1 - performance ratings and review history
# ---------------------------------------------------------------------------
def employee_performance(e360: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    """Q1 - one row per employee, every review period as its own column."""
    wide = reviews.pivot_table(index="employee_id", columns="review_period", values="rating", aggfunc="first")
    wide.columns = [f"rating_{c.replace('-', '_').lower()}" for c in wide.columns]
    cols = ["employee_id", "full_name", "department", "team_name", "job_level", "reviews_count",
            "first_review_period", "latest_review_period", "previous_rating", "latest_rating", "rating_change",
            "avg_rating", "performance_trend", "latest_manager_comment"]
    out = e360[cols].set_index("employee_id").join(wide.astype("Int64")).reset_index()
    return out.sort_values("employee_id", ignore_index=True)


def review_history(reviews: pd.DataFrame, e360: pd.DataFrame) -> pd.DataFrame:
    """Q1 - long format: one row per employee per review, with change vs the previous review."""
    r = reviews.sort_values(["employee_id", "review_period"]).copy()
    r["review_sequence"] = r.groupby("employee_id").cumcount() + 1
    r["change_vs_previous"] = r.groupby("employee_id")["rating"].diff().astype("Int64")
    r = r.merge(e360[["employee_id", "full_name", "department", "team_name"]], on="employee_id", how="left")
    cols = ["review_id", "employee_id", "full_name", "department", "team_name", "review_period", "review_sequence",
            "rating", "change_vs_previous", "manager_comment", "submitted_date"]
    return r[cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Q2 - critical skill gaps
# ---------------------------------------------------------------------------
def critical_skill_gaps(s: dict[str, pd.DataFrame], e360: pd.DataFrame) -> pd.DataFrame:
    """Q2 - every assessment where the employee is >= 2 levels below the required level."""
    a = s["competency_assessments"]
    a = a[a["skill_gap"] >= RULES["critical_gap_min"]]
    a = a.merge(s["competencies"].rename(columns={"name": "competency_name"}), on="competency_id")
    a = a.merge(e360[["employee_id", "full_name", "department", "team_name", "job_level", "latest_rating",
                      "potential_rating", "critical_skill_gaps"]], on="employee_id", how="left")
    a["is_key_competency"] = (a["required_level"] >= RULES["key_competency_min_required"]).astype("boolean")
    a["severity"] = pd.Series("Critical", index=a.index, dtype="string")
    a.loc[a["is_key_competency"], "severity"] = "Critical - key competency"
    cols = ["employee_id", "full_name", "department", "team_name", "job_level", "competency_id", "competency_name",
            "category", "required_level", "proficiency_level", "skill_gap", "is_key_competency", "severity",
            "assessed_date", "latest_rating", "potential_rating", "critical_skill_gaps"]
    return a[cols].sort_values(["skill_gap", "required_level", "employee_id"], ascending=[False, False, True],
                               ignore_index=True)


# ---------------------------------------------------------------------------
# Q3 - promotion candidates
# ---------------------------------------------------------------------------
def promotion_candidates(e360: pd.DataFrame) -> pd.DataFrame:
    """Q3 - High potential AND 'Ready Now', with a recommendation based on performance and skills."""
    c = e360[(e360["potential_rating"] == "High") & (e360["promotion_readiness"] == "Ready Now")].copy()
    strong = c["latest_rating"] >= RULES["high_performer_min_rating"]
    no_gaps = c["critical_skill_gaps"] == 0
    c["recommendation"] = pd.Series("Review - rating below 4", index=c.index, dtype="string")
    c.loc[strong & ~no_gaps, "recommendation"] = "Promote after closing skill gap"
    c.loc[strong & no_gaps, "recommendation"] = "Promote now"
    c["flight_risk"] = c["attrition_risk"].isin(["High"]).astype("boolean")
    order = {"Promote now": 0, "Promote after closing skill gap": 1, "Review - rating below 4": 2}
    c["_o"] = c["recommendation"].map(order)
    cols = ["employee_id", "full_name", "department", "team_name", "job_level", "tenure_years", "latest_rating",
            "previous_rating", "performance_trend", "avg_proficiency", "critical_skill_gaps", "courses_completed",
            "certifications_earned", "attrition_risk", "flight_risk", "recommendation"]
    return (c.sort_values(["_o", "latest_rating", "flight_risk", "employee_id"], ascending=[True, False, False, True])
            [cols].reset_index(drop=True))


# ---------------------------------------------------------------------------
# Q4 - training and development completed
# ---------------------------------------------------------------------------
def employee_learning(e360: pd.DataFrame) -> pd.DataFrame:
    """Q4 - every employee (including those with no learning at all)."""
    cols = ["employee_id", "full_name", "department", "team_name", "job_level", "learning_events", "courses_engaged",
            "enrollments", "modules_completed", "courses_completed", "certifications_earned", "quiz_attempts",
            "quizzes_passed", "quiz_pass_rate_pct", "avg_quiz_score", "first_learning_date", "last_learning_date",
            "learning_events_last_12m", "completed_courses_list", "certifications_list"]
    out = e360[cols].copy()
    out["completed_courses_list"] = out["completed_courses_list"].fillna("")
    out["certifications_list"] = out["certifications_list"].fillna("")
    return out.sort_values("employee_id", ignore_index=True)


def course_completions(events: pd.DataFrame, e360: pd.DataFrame) -> pd.DataFrame:
    """Q4 - one row per employee x course completed, with whether it was certified."""
    done = (events[events["event_type"] == "course_completed"].groupby(["employee_id", "course_name"])
            ["event_date"].min().rename("completed_on").reset_index())
    cert = (events[events["event_type"] == "certification_earned"].groupby(["employee_id", "course_name"])
            ["event_date"].min().rename("certified_on").reset_index())
    out = done.merge(cert, on=["employee_id", "course_name"], how="left")
    out["certified"] = out["certified_on"].notna().astype("boolean")
    out = out.merge(e360[["employee_id", "full_name", "department"]], on="employee_id", how="left")
    return out[["employee_id", "full_name", "department", "course_name", "completed_on", "certified",
                "certified_on"]].sort_values(["employee_id", "completed_on"], ignore_index=True)


# ---------------------------------------------------------------------------
# Q5 - attrition risk from performance trends
# ---------------------------------------------------------------------------
def attrition_signals(e360: pd.DataFrame) -> pd.DataFrame:
    """Score each employee on observable warning signs (0-5) and explain why."""
    d = e360.copy()
    signals = {
        "Rating declined": d["rating_change"] < 0,
        "Rating dropped 2+ points": d["rating_change"] <= -2,
        "Low latest rating (1-2)": d["latest_rating"] <= RULES["low_performer_max_rating"],
        "Manager flagged High attrition risk": d["attrition_risk"] == "High",
        f"No learning in last {RULES['learning_inactive_months']} months": d["learning_events_last_12m"] == 0,
    }
    reasons = pd.Series("", index=d.index, dtype="string")
    score = pd.Series(0, index=d.index, dtype="Int64")
    for label, mask in signals.items():
        hit = mask.fillna(False).astype(bool)
        score += hit.astype("Int64")
        reasons = reasons.where(~hit, reasons.str.cat(pd.Series(label, index=d.index), sep="; "))
    d["risk_score"] = score
    d["risk_reasons"] = reasons.str.lstrip("; ")
    d["computed_risk_level"] = pd.Series("Low", index=d.index, dtype="string")
    d.loc[d["risk_score"] >= 2, "computed_risk_level"] = "Medium"
    d.loc[d["risk_score"] >= 3, "computed_risk_level"] = "High"
    return d


def attrition_risk(e360: pd.DataFrame) -> pd.DataFrame:
    """Q5 - employees with a declining trend or a Medium/High computed risk."""
    d = attrition_signals(e360)
    at_risk = d[(d["computed_risk_level"] != "Low") | (d["performance_trend"] == "Declining")].copy()
    at_risk["regretted_loss"] = ((at_risk["potential_rating"] == "High")
                                 | (at_risk["previous_rating"] >= RULES["high_performer_min_rating"])).astype("boolean")
    cols = ["employee_id", "full_name", "department", "team_name", "job_level", "tenure_years", "previous_rating",
            "latest_rating", "rating_change", "performance_trend", "attrition_risk", "potential_rating",
            "learning_events_last_12m", "risk_score", "computed_risk_level", "risk_reasons", "regretted_loss"]
    return at_risk.sort_values(["risk_score", "rating_change", "employee_id"], ascending=[False, True, True])[cols] \
        .reset_index(drop=True)


# ---------------------------------------------------------------------------
# Q6 - performance vs team and project outcomes
# ---------------------------------------------------------------------------
def team_outcomes(e360: pd.DataFrame, s: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q6 - one row per team (all teams, even those without projects)."""
    g = e360.groupby("team_id")
    people = pd.DataFrame({
        "headcount": g.size(),
        "avg_latest_rating": g["latest_rating"].mean().round(2),
        "avg_rating_change": g["rating_change"].mean().round(2),
        "pct_high_performers": _pct(g["is_high_performer"].sum(), g["latest_rating"].count()),
        "pct_low_performers": _pct(g["is_low_performer"].sum(), g["latest_rating"].count()),
        "avg_skill_gap": g["avg_skill_gap"].mean().round(2),
        "avg_courses_completed": g["courses_completed"].mean().round(2),
    })
    p = s["projects"].groupby("team_id")
    projects = pd.DataFrame({
        "projects": p.size(),
        "on_time_projects": p["outcome_status"].apply(lambda x: _count(x, lambda v: v == "on-time")),
        "delayed_projects": p["outcome_status"].apply(lambda x: _count(x, lambda v: v == "delayed")),
        "avg_project_outcome_score": p["outcome_score"].mean().round(2),
    })
    teams = s["teams"].rename(columns={"name": "team_name"}).set_index("team_id")[["team_name", "department"]]
    out = teams.join(people).join(projects)
    for col in ["projects", "on_time_projects", "delayed_projects", "headcount"]:
        out[col] = out[col].fillna(0).astype("Int64")
    out["on_time_rate_pct"] = _pct(out["on_time_projects"], out["projects"])
    out["delivery_status"] = pd.Series("No projects", index=out.index, dtype="string")
    has = out["projects"] > 0
    out.loc[has & (out["on_time_rate_pct"] >= 100), "delivery_status"] = "All on time"
    out.loc[has & (out["on_time_rate_pct"] < 100), "delivery_status"] = "Some delayed"
    return out.reset_index().sort_values("team_id", ignore_index=True)


# ---------------------------------------------------------------------------
# Q7 - skill distribution
# ---------------------------------------------------------------------------
def skill_distribution(s: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Q7 - one row per department x competency with the proficiency-level histogram."""
    a = (s["competency_assessments"]
         .merge(s["employees"][["employee_id", "team_id"]], on="employee_id")
         .merge(s["teams"][["team_id", "department"]], on="team_id")
         .merge(s["competencies"].rename(columns={"name": "competency_name"}), on="competency_id"))
    keys = ["department", "competency_id", "competency_name", "category"]
    g = a.groupby(keys)
    out = pd.DataFrame({
        "assessments": g.size(),
        "employees_assessed": g["employee_id"].nunique(),
        "avg_proficiency": g["proficiency_level"].mean().round(2),
        "avg_required": g["required_level"].mean().round(2),
        "avg_skill_gap": g["skill_gap"].mean().round(2),
        "meets_requirement": g["skill_gap"].apply(lambda x: _count(x, lambda v: v <= 0)),
        "critical_gaps": g["skill_gap"].apply(lambda x: _count(x, lambda v: v >= RULES["critical_gap_min"])),
    })
    hist = pd.crosstab([a[k] for k in keys], a["proficiency_level"]).reindex(columns=[1, 2, 3, 4, 5], fill_value=0)
    hist.columns = [f"level_{c}_count" for c in hist.columns]
    out = out.join(hist)
    out["pct_meeting_requirement"] = _pct(out["meets_requirement"], out["assessments"])
    out["pct_expert_4_5"] = _pct(out["level_4_count"] + out["level_5_count"], out["assessments"])
    return out.reset_index().sort_values(["department", "category", "competency_name"], ignore_index=True)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def build_gold(s: dict[str, pd.DataFrame], as_of: str) -> dict[str, pd.DataFrame]:
    """Build every Gold table from Silver frames."""
    e360 = build_employee_360(s, as_of)
    builders: dict[str, Callable[[], pd.DataFrame]] = {
        "gold_employee_performance": lambda: employee_performance(e360, s["performance_reviews"]),
        "gold_review_history": lambda: review_history(s["performance_reviews"], e360),
        "gold_critical_skill_gaps": lambda: critical_skill_gaps(s, e360),
        "gold_promotion_candidates": lambda: promotion_candidates(e360),
        "gold_employee_learning": lambda: employee_learning(e360),
        "gold_course_completions": lambda: course_completions(s[LEARNING], e360),
        "gold_attrition_risk": lambda: attrition_risk(e360),
        "gold_team_outcomes": lambda: team_outcomes(e360, s),
        "gold_skill_distribution": lambda: skill_distribution(s),
        "gold_employee_360": lambda: e360,
    }
    return {name: build() for name, build in builders.items()}


def _postgres_ready(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas nullable/extension dtypes to plain types for ``to_sql``."""
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        if pd.api.types.is_bool_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            out[col] = out[col].astype(object).where(out[col].notna(), None)
        elif pd.api.types.is_integer_dtype(dtype):
            out[col] = out[col].astype("float64") if out[col].isna().any() else out[col].astype("int64")
        elif pd.api.types.is_float_dtype(dtype):
            out[col] = out[col].astype("float64")
    return out


def run_gold(settings: Settings, lake: Storage, tracker: RunTracker) -> dict[str, pd.DataFrame]:
    """Build Gold tables and publish them to the lake (and PostgreSQL if enabled)."""
    with tracker.step("gold.load_silver") as m:
        silver = load_silver(lake)
        m.update({f"rows_{k}": int(len(v)) for k, v in silver.items()})

    with tracker.step("gold.build") as m:
        tables = build_gold(silver, settings.run_date)
        m.update({k: int(len(v)) for k, v in tables.items()})

    engine = None
    if settings.write_gold_to_postgres:
        from .db import make_engine

        engine = make_engine(settings)

    for name, df in tables.items():
        with tracker.step(f"gold.{name}") as m:
            if df.empty:
                log.warning("gold table is empty", extra={"table": name})
            m["rows_written"] = int(len(df))
            m["bytes_written"] = lake.replace_parquet(df, gold_prefix(name))
            m["path"] = f"{lake.uri}/{gold_prefix(name)}/"
            if engine is not None:
                from .db import write_table

                write_table(engine, _postgres_ready(df), name, settings.gold_schema, settings)
                m["postgres_table"] = f"{settings.gold_schema}.{name}"
    if engine is not None:
        engine.dispose()
    return tables
