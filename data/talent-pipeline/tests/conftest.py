"""Shared fixtures: a tiny dataset with known values and known data-quality traps."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TEAMS = "team_id,name,department\n1,Team A,Engineering\n2,Team B,Sales\n3,Team C,Sales\n"
COMPETENCIES = "competency_id,name,category\n1,Python,Technical\n2,Negotiation,Domain\n"
EMPLOYEES = (
    "employee_id,first_name,last_name,email,team_id,job_level,hire_date\n"
    "1,Ann,Lee,ann@acme.example,1,IC1,2020-01-01\n"
    "2,Bob,Ray,bob@acme.example,1,Manager,2021-06-15\n"
    "3,Cat,Kim,cat@acme.example,2,IC2,2022-03-01\n"
    "4,Dan,Fox,dan@acme.example,2,Director,2019-09-09\n"
    "5,Eve,Doe,eve@acme.example,99,IC3,2023-01-01\n"  # FK violation: team 99 does not exist
    "6,Fay,Ng,not-an-email,1,IC1,2024-02-30\n"  # bad email + impossible date
)
PROJECTS = (
    "project_id,name,team_id,outcome_status,outcome_score\n"
    "1,Alpha,1,on-time,80.00\n2,Beta,1,delayed,50.00\n3,Gamma,2,on-time,90.00\n"
)
REVIEWS = (
    "review_id,employee_id,review_period,rating,manager_comment,submitted_date\n"
    "1,1,2024-H2,3,ok,2025-01-10\n2,1,2025-H1,5,great,2025-07-10\n"
    "3,2,2024-H2,4,good,2025-01-10\n4,2,2025-H1,4,good,2025-07-10\n"
    "5,3,2024-H2,2,weak,2025-01-10\n6,3,2025-H1,1,poor,2025-07-10\n"
    "7,4,2024-H2,4,good,2025-01-10\n8,4,2025-H1,5,top,2025-07-10\n"
    "9,4,2025-H1,7,typo,2025-07-11\n"  # rating out of range
)
ASSESSMENTS = (
    "assessment_id,employee_id,competency_id,proficiency_level,required_level,assessed_date\n"
    "1,1,1,2,4,2025-05-01\n2,2,1,4,3,2025-05-01\n3,3,2,1,3,2025-05-01\n4,4,2,3,3,2025-05-01\n"
)
TALENT = (
    "employee_id,review_cycle,potential_rating,promotion_readiness,attrition_risk\n"
    "1,2025-H1,High,Ready Now,High\n2,2025-H1,High,Ready Now,Medium\n"
    "3,2025-H1,Low,Not Ready,High\n4,2025-H1,Medium,Ready Now,Low\n"
)


def _event(eid: int, emp: str | None, date: str, etype: str, course: str = "SQL", score: int | None = None,
           **extra: object) -> str:
    rec: dict[str, object] = {"event_id": f"EV{eid:010d}", "employee_id": emp, "event_date": date,
                              "event_type": etype, "course_name": course, "text": "t", "score": score}
    rec.update(extra)
    return json.dumps(rec)


LEARNING_FILES = {
    "year=2025/month=01/day=10/events-000.jsonl": [
        _event(1, "1", "2025-01-10T09:00:00", "enrollment"),
        _event(2, "1", "2025-01-10T10:00:00", "course_completed"),
        _event(2, "1", "2025-01-10T10:00:00", "course_completed"),  # exact duplicate
        _event(3, "2", "2025-01-10T11:00:00", "quiz_passed", score=90),
        '{"event_id": "EV0000000004", "employee_id": "2", "event_da',  # truncated JSON
    ],
    "year=2025/month=02/day=03/events-000.jsonl": [
        _event(5, None, "2025-02-03T09:00:00", "enrollment"),  # missing employee
        _event(6, "3", "N/A", "enrollment"),  # bad date
        json.dumps({"event_id": "EV0000000007", "employee_id": "4", "event_date": "2025-02-03T12:00:00",
                    "event_type": "certification_earned", "course_name": "SQL", "notes": "renamed field",
                    "score": None}),
        _event(8, "1", "2025-02-03T13:00:00", "course_completed", course="Python"),
        _event(9, "1", "2025-02-03T14:00:00", "quiz_attempted", score=150),  # score out of range
    ],
    "year=2025/month=02/day=04/events-000.jsonl": [],  # empty file
}


@pytest.fixture()
def dataset_dir(tmp_path: Path) -> Path:
    """Write the synthetic dataset in the same layout as the real one."""
    root = tmp_path / "dataset"
    root.mkdir()
    for name, body in {
        "teams": TEAMS, "competencies": COMPETENCIES, "employees": EMPLOYEES, "projects": PROJECTS,
        "performance_reviews": REVIEWS, "competency_assessments": ASSESSMENTS, "talent_review": TALENT,
    }.items():
        (root / f"{name}.csv").write_text(body)
    for rel, lines in LEARNING_FILES.items():
        path = root / "learning_activity" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + ("\n" if lines else ""))
    return root


@pytest.fixture()
def offline_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, dataset_dir: Path) -> Path:
    """Environment for an offline run: CSV source, local-folder lake, no PostgreSQL."""
    lake = tmp_path / "lake"
    monkeypatch.setenv("IS_LOCAL", "true")
    monkeypatch.setenv("SOURCE_MODE", "csv")
    monkeypatch.setenv("SOURCE_DIR", str(dataset_dir))
    monkeypatch.setenv("LEARNING_ACTIVITY_URI", str(dataset_dir / "learning_activity"))
    monkeypatch.setenv("DATA_LAKE_URI", str(lake))
    monkeypatch.setenv("WRITE_GOLD_TO_POSTGRES", "false")
    monkeypatch.setenv("RUN_DATE", "2025-12-31")
    monkeypatch.setenv("RETRY_BASE_DELAY", "0")
    monkeypatch.delenv("RUN_ID", raising=False)
    return lake
