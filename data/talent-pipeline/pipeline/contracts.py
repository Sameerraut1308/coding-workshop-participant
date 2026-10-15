"""Schema contracts: the explicit rules every Silver table must satisfy.

Each contract lists the expected columns and types, which are required
(non-null), the primary key, domain rules, and foreign keys. The rules mirror
``schema.sql`` from the source system, plus extra rules for the semi-structured
``learning_activity`` events, which have no database constraints at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

# A domain rule returns True for rows that PASS.
Rule = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class DomainRule:
    """A named row-level check (e.g. ``rating between 1 and 5``)."""

    name: str
    check: Rule


@dataclass(frozen=True)
class ForeignKey:
    """``column`` must exist in ``ref_table.ref_column``."""

    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class TableContract:
    """Contract for one table."""

    name: str
    columns: dict[str, str]  # column -> int | float | str | date | datetime
    primary_key: tuple[str, ...]
    required: tuple[str, ...]
    rules: tuple[DomainRule, ...] = field(default_factory=tuple)
    foreign_keys: tuple[ForeignKey, ...] = field(default_factory=tuple)


def _between(col: str, low: float, high: float) -> DomainRule:
    return DomainRule(f"{col}_between_{low:g}_{high:g}", lambda df: df[col].isna() | df[col].between(low, high))


def _in(col: str, allowed: set[str]) -> DomainRule:
    return DomainRule(f"{col}_in_allowed_values", lambda df: df[col].isna() | df[col].isin(sorted(allowed)))


def _matches(col: str, pattern: str, label: str) -> DomainRule:
    return DomainRule(label, lambda df: df[col].isna() | df[col].astype("string").str.fullmatch(pattern).fillna(False))


JOB_LEVELS = {"IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director"}
EVENT_TYPES = {
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
}

CONTRACTS: dict[str, TableContract] = {
    "teams": TableContract(
        name="teams",
        columns={"team_id": "int", "name": "str", "department": "str"},
        primary_key=("team_id",),
        required=("team_id", "name", "department"),
    ),
    "competencies": TableContract(
        name="competencies",
        columns={"competency_id": "int", "name": "str", "category": "str"},
        primary_key=("competency_id",),
        required=("competency_id", "name", "category"),
        rules=(_in("category", {"Technical", "Leadership", "Domain"}),),
    ),
    "employees": TableContract(
        name="employees",
        columns={
            "employee_id": "int",
            "first_name": "str",
            "last_name": "str",
            "email": "str",
            "team_id": "int",
            "job_level": "str",
            "hire_date": "date",
        },
        primary_key=("employee_id",),
        required=("employee_id", "first_name", "last_name", "email", "team_id", "job_level", "hire_date"),
        rules=(
            _matches("email", r"[^@\s]+@[^@\s]+\.[^@\s]+", "email_format"),
            _in("job_level", JOB_LEVELS),
        ),
        foreign_keys=(ForeignKey("team_id", "teams", "team_id"),),
    ),
    "projects": TableContract(
        name="projects",
        columns={"project_id": "int", "name": "str", "team_id": "int", "outcome_status": "str",
                 "outcome_score": "float"},
        primary_key=("project_id",),
        required=("project_id", "name", "team_id", "outcome_status", "outcome_score"),
        rules=(
            _in("outcome_status", {"on-time", "delayed", "at-risk", "cancelled"}),
            _between("outcome_score", 0, 100),
        ),
        foreign_keys=(ForeignKey("team_id", "teams", "team_id"),),
    ),
    "performance_reviews": TableContract(
        name="performance_reviews",
        columns={
            "review_id": "int",
            "employee_id": "int",
            "review_period": "str",
            "rating": "int",
            "manager_comment": "str",
            "submitted_date": "date",
        },
        primary_key=("review_id",),
        required=("review_id", "employee_id", "review_period", "rating", "submitted_date"),
        rules=(
            _between("rating", 1, 5),
            _matches("review_period", r"\d{4}-H[12]", "review_period_format_YYYY-Hn"),
        ),
        foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    ),
    "competency_assessments": TableContract(
        name="competency_assessments",
        columns={
            "assessment_id": "int",
            "employee_id": "int",
            "competency_id": "int",
            "proficiency_level": "int",
            "required_level": "int",
            "assessed_date": "date",
        },
        primary_key=("assessment_id",),
        required=("assessment_id", "employee_id", "competency_id", "proficiency_level", "required_level",
                  "assessed_date"),
        rules=(_between("proficiency_level", 1, 5), _between("required_level", 1, 5)),
        foreign_keys=(
            ForeignKey("employee_id", "employees", "employee_id"),
            ForeignKey("competency_id", "competencies", "competency_id"),
        ),
    ),
    "talent_review": TableContract(
        name="talent_review",
        columns={
            "employee_id": "int",
            "review_cycle": "str",
            "potential_rating": "str",
            "promotion_readiness": "str",
            "attrition_risk": "str",
        },
        primary_key=("employee_id",),
        required=("employee_id", "review_cycle", "potential_rating", "promotion_readiness", "attrition_risk"),
        rules=(
            _in("potential_rating", {"Low", "Medium", "High"}),
            _in("promotion_readiness", {"Ready Now", "1-2 Years", "Not Ready"}),
            _in("attrition_risk", {"Low", "Medium", "High"}),
        ),
        foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    ),
    "learning_activity": TableContract(
        name="learning_activity",
        columns={
            "event_id": "str",
            "employee_id": "int",
            "event_date": "datetime",
            "event_type": "str",
            "course_name": "str",
            "text": "str",
            "score": "float",
        },
        primary_key=("event_id",),
        required=("event_id", "employee_id", "event_date", "event_type", "course_name"),
        rules=(
            _in("event_type", EVENT_TYPES),
            _between("score", 0, 100),
            _matches("event_id", r"EV\d+", "event_id_format"),
        ),
        foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    ),
}
