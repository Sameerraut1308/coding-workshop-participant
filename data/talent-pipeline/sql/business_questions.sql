-- ACME Performance & Talent: the 7 business questions answered from the Gold layer.
-- The pipeline publishes every Gold table to the PostgreSQL schema "gold".
-- Run:  psql "host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER" -f sql/business_questions.sql

\echo '== Q1. Performance ratings and review history for each employee (first 20) =='
SELECT employee_id, full_name, department, rating_2024_h2, rating_2025_h1,
       rating_change, avg_rating, performance_trend
FROM gold.gold_employee_performance
ORDER BY employee_id
LIMIT 20;

\echo '-- Q1 drill-down: full review history for one employee (change the id) --'
SELECT review_period, rating, change_vs_previous, manager_comment, submitted_date
FROM gold.gold_review_history
WHERE employee_id = 1
ORDER BY review_sequence;

\echo '== Q2. Employees with critical skill gaps in key competencies (required level 4+) =='
SELECT full_name, department, job_level, competency_name, category,
       required_level, proficiency_level, skill_gap
FROM gold.gold_critical_skill_gaps
WHERE is_key_competency
ORDER BY skill_gap DESC, department, full_name
LIMIT 25;

\echo '-- Q2 summary: employees with critical gaps per department --'
SELECT department, COUNT(DISTINCT employee_id) AS employees_with_critical_gaps,
       COUNT(*) AS critical_gaps,
       SUM(CASE WHEN is_key_competency THEN 1 ELSE 0 END) AS in_key_competencies
FROM gold.gold_critical_skill_gaps
GROUP BY department
ORDER BY employees_with_critical_gaps DESC;

\echo '== Q3. High-potential employees ready for promotion =='
SELECT recommendation, COUNT(*) AS employees,
       SUM(CASE WHEN flight_risk THEN 1 ELSE 0 END) AS also_high_attrition_risk
FROM gold.gold_promotion_candidates
GROUP BY recommendation
ORDER BY employees DESC;

SELECT full_name, department, job_level, latest_rating, performance_trend,
       certifications_earned, attrition_risk
FROM gold.gold_promotion_candidates
WHERE recommendation = 'Promote now'
ORDER BY flight_risk DESC, latest_rating DESC, full_name
LIMIT 25;

\echo '== Q4. Training and development completed by each employee (first 20) =='
SELECT employee_id, full_name, department, courses_completed, certifications_earned,
       quiz_pass_rate_pct, last_learning_date, completed_courses_list
FROM gold.gold_employee_learning
ORDER BY employee_id
LIMIT 20;

\echo '-- Q4: employees who have never completed a course, per department --'
SELECT department, COUNT(*) AS employees_without_completed_course
FROM gold.gold_employee_learning
WHERE courses_completed = 0
GROUP BY department
ORDER BY 2 DESC;

\echo '== Q5. Employees at risk of attrition based on performance trends =='
SELECT computed_risk_level, COUNT(*) AS employees,
       SUM(CASE WHEN regretted_loss THEN 1 ELSE 0 END) AS regretted_losses
FROM gold.gold_attrition_risk
GROUP BY computed_risk_level
ORDER BY employees DESC;

SELECT full_name, department, previous_rating, latest_rating, attrition_risk,
       potential_rating, risk_score, risk_reasons
FROM gold.gold_attrition_risk
WHERE regretted_loss
ORDER BY risk_score DESC, rating_change
LIMIT 25;

\echo '== Q6. Team performance vs project outcomes =='
SELECT delivery_status, COUNT(*) AS teams,
       ROUND(AVG(avg_latest_rating)::numeric, 2)        AS avg_team_rating,
       ROUND(AVG(pct_high_performers)::numeric, 1)      AS avg_pct_high_performers,
       ROUND(AVG(avg_project_outcome_score)::numeric, 2) AS avg_outcome_score
FROM gold.gold_team_outcomes
GROUP BY delivery_status
ORDER BY teams DESC;

SELECT ROUND(CORR(avg_latest_rating, on_time_rate_pct)::numeric, 2)          AS corr_rating_vs_on_time,
       ROUND(CORR(avg_latest_rating, avg_project_outcome_score)::numeric, 2) AS corr_rating_vs_outcome,
       ROUND(CORR(avg_skill_gap, on_time_rate_pct)::numeric, 2)              AS corr_skill_gap_vs_on_time
FROM gold.gold_team_outcomes
WHERE projects > 0;

\echo '== Q7. Distribution of skills across the organization =='
SELECT category, competency_name,
       SUM(level_1_count) AS l1, SUM(level_2_count) AS l2, SUM(level_3_count) AS l3,
       SUM(level_4_count) AS l4, SUM(level_5_count) AS l5,
       ROUND(100.0 * SUM(meets_requirement) / SUM(assessments), 1) AS pct_meeting_requirement
FROM gold.gold_skill_distribution
GROUP BY category, competency_name
ORDER BY pct_meeting_requirement;
