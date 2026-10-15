# Test evidence

**The pipeline runs locally and in AWS EKS.** Sections 1–3 and 5 are local runs. Section 4 is the successful cloud run:
the EKS job against Aurora PostgreSQL and the real S3 lake bucket, `status: success`, 30/30 reconciliation checks, the
same row counts as locally.

The local results below were produced on 2026-10-15 against the real dataset, a local PostgreSQL 18.6 (loaded with the
dataset's own `load.sh`) and LocalStack Pro S3 on port 4566, with `AWS_REGION=ap-south-1`, Python 3.13 and pandas 3.0.6.
The S3 emulator is LocalStack Pro (Docker container `localstack-main`, version 2026.11.0.dev62), **not** moto.
Re-run them on the VDI with the commands shown, after `source scripts/env.sh` (`run_all.sh` does this for you).

## 1. Automated tests — 81 passed, 0 failed, 0 skipped, 98% coverage

```bash
RUN_INTEGRATION=1 python -m pytest -v --cov=pipeline --cov=job
```

```
tests/test_db.py .....                                                   [  6%]
tests/test_end_to_end.py .........                                       [ 17%]
tests/test_gold.py .............                                         [ 33%]
tests/test_integration.py ....                                           [ 38%]
tests/test_observability.py ........                                     [ 48%]
tests/test_quality.py ...................                                [ 71%]
tests/test_requirements.py ...                                           [ 75%]
tests/test_seed.py .........                                             [ 86%]
tests/test_silver.py .....                                               [ 92%]
tests/test_storage.py ......                                             [100%]
Name                        Stmts   Miss  Cover
-----------------------------------------------
job.py                         63      3    95%
pipeline/__init__.py            1      0   100%
pipeline/bronze.py             80      6    92%
pipeline/config.py             72      0   100%
pipeline/contracts.py          31      0   100%
pipeline/db.py                 71      3    96%
pipeline/gold.py              207      1    99%
pipeline/observability.py      97      0   100%
pipeline/quality.py            83      0   100%
pipeline/reconcile.py          80      1    99%
pipeline/seed.py               69      1    99%
pipeline/silver.py            133      2    98%
pipeline/storage.py           156      5    97%
-----------------------------------------------
TOTAL                        1143     22    98%
81 passed, 1 warning in 80.81s (0:01:20)
```

The one warning is a `SyntaxWarning` raised inside the system `pytz` package (`/usr/lib/python3/dist-packages`), not in
this project's code.

Default run, as `bash scripts/run_all.sh` does it (no `RUN_INTEGRATION`; the 10 PostgreSQL/LocalStack tests are skipped).
In the same run the pipeline finished with `status: success` and 30/30 reconciliation checks, and the notebook executed:

```
TOTAL                        1143    109    90%
71 passed, 10 skipped, 1 warning in 12.54s
```

| Guide requirement | Covered by | Result |
|---|---|---|
| Unit tests: core transformations | `test_silver.py`, `test_gold.py` (one test block per business question, hand-computed expected values) | ✅ |
| Unit tests: schema validation rules | `test_quality.py` (types, nulls, domains, FKs, duplicates, schema drift) | ✅ 100% coverage of `quality.py` / `contracts.py` |
| Integration: PostgreSQL + S3 read/write | `test_integration.py` (reads all 7 source tables, S3 round-trip, full run publishes every Gold table to S3 **and** PostgreSQL with equal row counts) | ✅ |
| Source ingestion validation | `test_bronze_matches_source_bytes`, Bronze row counts in manifest | ✅ |
| S3 as a raw source (cloud layout) | `test_pipeline_reads_raw_learning_events_from_s3` — `scripts/upload_source.py` uploads all 783 files to `landing/learning_activity`, Bronze reads them from S3, 30/30 reconciliation | ✅ |
| Cloud settings | `test_settings_local_vs_cloud` (IAM-compatible lake bucket, S3 raw-event default), `test_settings_region_fallbacks` | ✅ (and used for real in the AWS EKS run — section 4) |
| Cloud settings: lake name from `APP_ID` | `test_settings_seed_location_and_app_id_fallback` — the EKS job gets `APP_ID`, not `PARTICIPANT_ID` | ✅ |
| Seeding an empty database (cloud) | `test_seed.py` — creates and loads all 7 tables in a throwaway schema with row counts equal to the CSVs; re-run does nothing; empty tables get loaded; partly loaded tables are refused; a bad CSV rolls back everything; against the loaded local database the step records "skip"; off unless `SEED_SOURCE_URI` is set | ✅ |
| PostgreSQL access independent of pandas/SQLAlchemy versions | `test_db.py` — with pandas' SQLAlchemy support switched off (as in the EKS image: pandas 3.0.6 with SQLAlchemy 2.0.22, where `pd.read_sql` raised `TypeError`), `read_table` still returns text values; `write_table` creates the same column types `to_sql` did (bigint, double precision, boolean, text, timestamp), replaces instead of appending, and a failing insert leaves the previous table in place | ✅ |
| Job dependencies work in the job's container image | In `jupyter/all-spark-notebook` (Python 3.11.6, run locally with Docker): `pip install -r requirements.txt` replaces only pandas (→ 2.2.3) and pyarrow (→ 17.0.0) and keeps numpy 1.24.4; pyarrow imports and writes Parquet; `RUN_INTEGRATION=1 python -m pytest` 78 passed (before `test_requirements.py` existed) and `python job.py` succeeded with 30/30 reconciliation; Gold tables identical row for row to the local pandas 3.0.6 run. `test_requirements.py` keeps `requirements.txt` pinned, runtime-only and covering every import of the job | ✅ |
| Uploading the cloud inputs | `test_upload_source_uploads_tables_and_learning_files_once` — CSVs, `schema.sql` and learning files uploaded once; re-run uploads nothing; a missing CSV is reported | ✅ |
| Data quality validation (quarantine) | `test_quarantine_contains_reasons` | ✅ |
| Idempotency (run twice, no duplicates) | `test_rerun_is_idempotent` — every output identical; 2nd run copies 0 raw files (the test picks the newest run manifest by file time, since two runs can share a one-second `run_id` stamp) | ✅ |
| Reconciliation catches lost rows | `test_reconciliation_detects_lost_rows` — deleting one Silver row fails the run | ✅ |
| Retry / backoff for transient failures | `test_observability.py` — recovers after 2 failures with 0.5s → 1.0s backoff; gives up and re-raises after max retries | ✅ |
| Zero rows written | `test_empty_source_table_fails_fast` | ✅ |
| Lake bucket creation in any region | `test_s3_ensure_bucket_in_any_region` (`us-east-1` and `ap-south-1`) — `CreateBucket` sends a `LocationConstraint` outside `us-east-1` | ✅ |
| End-to-end smoke test | `test_full_run_succeeds_and_reconciles` | ✅ |

## 2. Reconciliation — 30/30 checks pass on every run

Run on its own at any time: `python job.py --steps reconcile`. Results are stored in each run manifest
(`_runs/run_date=…/<run_id>.json → reconciliation`).

| Layer | Check |
|---|---|
| Bronze | learning file count source == Bronze (783); 0 file-size mismatches |
| Silver | for each of 8 tables: Bronze rows == Silver + quarantine + exact duplicates; event_id unique |
| Gold | one row per employee in employee_360 / employee_performance / employee_learning (+ unique keys); review_history == silver reviews (4,000); critical gaps == assessments with gap ≥ 2 (325); promotion candidates == High & Ready Now (357); course_completions == distinct employee × completed course (3,421) and equals the sum of `courses_completed`; team_outcomes one row per team, headcount sums to 2,000, projects sum to 33; skill_distribution assessments and level histogram both sum to 4,000; grain uniqueness; average latest rating recomputed independently from Silver matches Gold |

## 3. Row counts per layer (real dataset)

| Table | Bronze | Silver | Quarantined | Duplicates dropped |
|---|---|---|---|---|
| teams | 100 | 100 | 0 | 0 |
| competencies | 40 | 40 | 0 | 0 |
| employees | 2,000 | 2,000 | 0 | 0 |
| projects | 33 | 33 | 0 | 0 |
| performance_reviews | 4,000 | 4,000 | 0 | 0 |
| competency_assessments | 4,000 | 4,000 | 0 | 0 |
| talent_review | 2,000 | 2,000 | 0 | 0 |
| learning_activity | 29,868 lines in 783 files | 29,574 | 143 (13 malformed + 130 rule failures) | 151 |

## 4. Cloud run in AWS EKS — success, 30/30 reconciliation

The EKS job `coding-workshop-talent-pipeline` (region `ap-south-1`) ran the full pipeline against Aurora PostgreSQL and
the real lake bucket `s3://coding-workshop-<id>-talent-lake-<id>`. The numbers below are from its run manifest
`_runs/run_date=2026-10-15/20261015T152846Z-d2be7505.json`. A read-only check afterwards found all 10 Gold folders under
`gold/`, and the manifest in S3 was byte for byte the same as the saved copy.

| | |
|---|---|
| Run | `20261015T152846Z-d2be7505`, pipeline version 1.0.0 |
| Status | `success` (all 30 steps `success`) |
| Reconciliation | **30/30 passed**, 0 failed (2 Bronze, 9 Silver, 19 Gold) |
| Duration | **173.7 s** (2 min 54 s), 15:28:47 → 15:31:40 UTC |
| By step | seed 13.6 s · Bronze 75.6 s · Silver 32.0 s · Gold 5.1 s · reconcile 46.7 s |
| Settings | `is_local: false`, no S3 endpoint override (real AWS S3), Aurora `sslmode=require`, source tables from Aurora, raw events from `<lake>/landing/learning_activity`, Gold also written to PostgreSQL |
| Seed | `skip` (the second cloud run had already loaded the 7 source tables) |
| Bronze learning files | 783 found, 0 copied, 783 unchanged and skipped (copied by an earlier attempt), 1 empty, 7,302,325 bytes |

Row counts per layer (each the same as the local runs in section 3):

| Table | Bronze | Silver | Quarantined | Duplicates dropped |
|---|---|---|---|---|
| teams | 100 | 100 | 0 | 0 |
| competencies | 40 | 40 | 0 | 0 |
| employees | 2,000 | 2,000 | 0 | 0 |
| projects | 33 | 33 | 0 | 0 |
| performance_reviews | 4,000 | 4,000 | 0 | 0 |
| competency_assessments | 4,000 | 4,000 | 0 | 0 |
| talent_review | 2,000 | 2,000 | 0 | 0 |
| learning_activity | 29,868 lines in 783 files | 29,574 | 143 (13 malformed + 130 rule failures) | 151 |

| Gold table (written to S3 `gold/` and to Aurora `gold.`) | Rows | Local run |
|---|---|---|
| gold_employee_performance | 2,000 | 2,000 |
| gold_review_history | 4,000 | 4,000 |
| gold_critical_skill_gaps | 325 | 325 |
| gold_promotion_candidates | 357 | 357 |
| gold_employee_learning | 2,000 | 2,000 |
| gold_course_completions | 3,421 | 3,421 |
| gold_attrition_risk | 260 | 260 |
| gold_team_outcomes | 100 | 100 |
| gold_skill_distribution | 400 | 400 |
| gold_employee_360 | 2,000 | 2,000 |

The first three cloud runs failed: no network path to Aurora and no AWS credentials, then pandas 3 with the image's
older SQLAlchemy, then pyarrow 26 with numpy 1.26. The README's *Cloud deployment status* tells how each was found and
fixed.

## 5. Performance — linear scaling

```bash
python scripts/benchmark.py --scales 1,5,10
```

| Scale | Learning lines | Job seconds | Learning rows / min | End-to-end rows / min | Peak memory | Reconciliation |
|---|---|---|---|---|---|---|
| 1× | 29,868 | 5.1 | 626,601 | 352,623 | 277 MB | 30/30 |
| 5× | 149,340 | 16.4 | 644,911 | 547,838 | 626 MB | 30/30 |
| 10× | 298,680 | 31.7 | 628,910 | 564,538 | 1,097 MB | 30/30 |

**Conclusion:** throughput stays flat (~630k rows/min) as volume grows 10×, so there is no non-linear slowdown;
memory grows roughly linearly (~110 MB per 30k events). *(Benchmark not yet re-run on pandas 3.0.)* Against PostgreSQL + S3 the full run takes ~36–50 s, dominated by
network writes of the Gold tables — well inside a daily batch window.

## 6. Conclusions
* The pipeline runs locally and in AWS EKS, with 30/30 reconciliation and the same row counts in both.
* Every layer is wired end to end and reruns are idempotent.
* All source rows are accounted for; the 1% of bad learning events are isolated with reasons, not lost.
* Business metrics are verified against hand-computed values (unit tests) and independently recomputed from Silver (reconciliation).
