# Data lineage — source-to-target mapping

Every Bronze and Silver row carries lineage columns: `_source`, `_ingested_at`, `_run_id`, `_pipeline_version`
(Bronze) and `_silver_processed_at`, `_silver_run_id` (Silver). Learning events also keep `_source_file` and
`_line_number`, so any Gold number can be traced back to the exact raw line.

## Bronze → Silver

| Silver table | Source | Renamed | Cast | Derived columns | Rules enforced (else quarantined) |
|---|---|---|---|---|---|
| `teams` | `public.teams` | – | `team_id`→int | – | PK unique, all fields required |
| `competencies` | `public.competencies` | – | `competency_id`→int | – | `category` ∈ {Technical, Leadership, Domain} |
| `employees` | `public.employees` | – | ids→int, `hire_date`→date | `full_name` = first + last; `tenure_years` = (run_date − hire_date)/365.25 | email format, `job_level` in allowed set, FK `team_id`→teams |
| `projects` | `public.projects` | – | `outcome_score`→float | – | `outcome_status` allowed set, score 0–100, FK team |
| `performance_reviews` | `public.performance_reviews` | – | `rating`→int, `submitted_date`→date | `review_year`, `review_half` (from `review_period`), `is_high_performer` (rating ≥ 4) | rating 1–5, period format `YYYY-Hn`, FK employee |
| `competency_assessments` | `public.competency_assessments` | – | levels→int, `assessed_date`→date | **`skill_gap` = required_level − proficiency_level**, `meets_requirement` (gap ≤ 0) | levels 1–5, FK employee + competency |
| `talent_review` | `public.talent_review` | – | `employee_id`→int | – | potential / readiness / attrition in allowed sets, FK employee |
| `learning_activity` | `learning_activity/year=/month=/day=/*.jsonl` | **`notes` → `text`** (legacy schema drift) | `employee_id` "123"→int, `event_date`→timestamp, `score`→float | `event_year`, `event_month` | valid JSON, required fields, event_type allowed set, score 0–100, `event_id` format, FK employee; exact duplicates dropped |

## Silver → Gold

| Gold column(s) | Derived from | Logic |
|---|---|---|
| `latest_rating`, `latest_review_period`, `latest_manager_comment` | performance_reviews | rating in the employee's most recent `review_period` |
| `previous_rating`, `rating_change` | performance_reviews | second-most-recent rating; latest − previous |
| `avg_rating`, `reviews_count` | performance_reviews | mean / count per employee |
| `performance_trend` | rating_change | > 0 Improving, = 0 Stable, < 0 Declining |
| `rating_2024_h2`, `rating_2025_h1` | performance_reviews | pivot: one column per review period |
| `change_vs_previous`, `review_sequence` (`gold_review_history`) | performance_reviews | diff / order within employee |
| `avg_proficiency`, `avg_skill_gap`, `competencies_below_required`, `critical_skill_gaps` | competency_assessments | per-employee mean; count gap > 0; count gap ≥ 2 |
| `is_key_competency`, `severity` (`gold_critical_skill_gaps`) | competency_assessments | required_level ≥ 4 |
| `courses_completed`, `certifications_earned` | learning_activity | distinct courses with a `course_completed` / `certification_earned` event |
| `modules_completed`, `enrollments`, `quiz_attempts`, `quizzes_passed`, `quiz_pass_rate_pct`, `avg_quiz_score` | learning_activity | counts by `event_type`; passed / attempts; mean quiz score |
| `learning_events_last_12m`, `first/last_learning_date` | learning_activity | events with `event_date` ≥ run_date − 12 months; min / max date |
| `completed_courses_list`, `certifications_list` | learning_activity | "; "-joined sorted course names |
| `recommendation`, `flight_risk` (`gold_promotion_candidates`) | talent_review + employee_360 | see README → Business definitions |
| `risk_score`, `computed_risk_level`, `risk_reasons`, `regretted_loss` (`gold_attrition_risk`) | employee_360 | 5 rule-based signals, one point each |
| team people metrics (`gold_team_outcomes`) | employee_360 grouped by `team_id` | mean rating, % high/low performers, mean gap, mean courses |
| `projects`, `on_time_rate_pct`, `avg_project_outcome_score`, `delivery_status` | projects | count, % on-time, mean score, All on time / Some delayed / No projects |
| `level_1_count` … `level_5_count`, `pct_meeting_requirement`, `pct_expert_4_5`, `critical_gaps` (`gold_skill_distribution`) | competency_assessments + employees + teams + competencies | histogram of `proficiency_level` per department × competency |

## Gold table grain

| Table | Grain (unique key) | Rows (current data) |
|---|---|---|
| `gold_employee_360` | employee_id | 2,000 |
| `gold_employee_performance` | employee_id | 2,000 |
| `gold_review_history` | review_id | 4,000 |
| `gold_critical_skill_gaps` | employee_id × competency_id (assessment) | 325 |
| `gold_promotion_candidates` | employee_id | 357 |
| `gold_employee_learning` | employee_id | 2,000 |
| `gold_course_completions` | employee_id × course_name | 3,421 |
| `gold_attrition_risk` | employee_id | 260 |
| `gold_team_outcomes` | team_id | 100 |
| `gold_skill_distribution` | department × competency_id | 400 |
