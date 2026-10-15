# talent-pipeline — ACME Performance & Talent Management

An end-to-end **Bronze → Silver → Gold** data pipeline (Python / pandas) that turns ACME's scattered HR data
(PostgreSQL tables + raw learning-activity event files) into trusted, queryable Gold tables, and a Jupyter
notebook that answers the seven business questions with charts and plain-English findings.

| | |
|---|---|
| **Run everything** | `bash scripts/run_all.sh` |
| **Pipeline only** | `python job.py` |
| **Insights** | `notebooks/talent_insights.ipynb` (pre-rendered copy: `notebooks/talent_insights.html`) |
| **SQL answers** | `sql/business_questions.sql` (queries the `gold` schema in PostgreSQL) |
| **Test evidence** | `docs/TEST_EVIDENCE.md` — 64 tests, 98% coverage, 30/30 reconciliation checks |
| **Column lineage** | `docs/LINEAGE.md` — source-to-target mapping for every Gold column |

---

## 1. Business questions and where they are answered

| # | Question | Gold table(s) | Headline finding |
|---|---|---|---|
| Q1 | What are the performance ratings and review history for each employee? | `gold_employee_performance` (1 row / employee), `gold_review_history` (1 row / review) | Average rating rose 3.02 → 3.41; 230 employees (12%) declined |
| Q2 | Which employees have critical skill gaps in key competencies? | `gold_critical_skill_gaps` | 317 employees have 325 critical gaps; 177 are in key competencies |
| Q3 | Who are the high-potential employees ready for promotion? | `gold_promotion_candidates` | 357 High-potential & Ready-Now; **132 "promote now"**, 34 after closing a gap, 191 need review (rating < 4) |
| Q4 | What training and development activities have been completed by each employee? | `gold_employee_learning` (1 row / employee), `gold_course_completions` (1 row / employee × course) | 1.7 courses per person on average; 339 (17%) never completed a course |
| Q5 | Which employees are at risk of attrition based on performance trends? | `gold_attrition_risk` | Declining rating ⇒ 62% flagged High risk (vs ~10% otherwise); 153 High computed risk; 144 regretted-loss risks |
| Q6 | How does employee performance correlate with team and project outcomes? | `gold_team_outcomes` | Team average rating correlates **+0.54** with on-time delivery |
| Q7 | What is the distribution of skills across the organization? | `gold_skill_distribution` | Even spread; Stakeholder Management weakest (65% meet requirement); Leadership weakest in HR & Finance |

All Gold tables are built from one conformed table, `gold_employee_360` (one row per employee), so a metric such as
"latest rating" or "critical gap" is defined exactly once.

### Business definitions (one place to change them: `pipeline/gold.py → RULES`)

| Term | Definition |
|---|---|
| Skill gap | `required_level − proficiency_level` (positive = below requirement) |
| Critical gap | skill gap ≥ 2 levels |
| Key competency | required level ≥ 4 |
| High / low performer | latest rating ≥ 4 / ≤ 2 |
| Performance trend | latest vs previous review: Improving / Stable / Declining |
| Promotion candidate | potential = High **and** readiness = Ready Now. *Promote now* = rating ≥ 4 and no critical gaps |
| Attrition risk score (0–5) | +1 each: rating declined; dropped 2+ points; latest rating ≤ 2; talent review flagged High; no learning in 12 months. High = 3+, Medium = 2 |
| Regretted loss | High potential, or previously rated ≥ 4 |

---

## 2. Architecture

```
 SOURCES                      BRONZE (raw, immutable)            SILVER (clean, typed)              GOLD (business-ready)
 ─────────                    ────────────────────────           ─────────────────────              ─────────────────────
 PostgreSQL (7 tables) ──►  bronze/<table>/load_date=D/  ──►  silver/<table>/               ──►  gold/<table>/   (S3, Parquet)
                             (all values as text + lineage)   (contract-validated, de-duped)      gold.<table>     (PostgreSQL)
 learning_activity/     ──►  bronze/learning_activity/    ──►  silver/learning_activity/
 year=/month=/day=/*.jsonl    year=/month=/day=/  (byte copy)    event_year=YYYY/
                                                          └──►  quarantine/<table>/run_date=D/   (rejected rows + reason)
                                                                _metadata/silver/<table>.json    (row-count metrics)
                                                                _runs/run_date=D/<run_id>.json    (run manifest)
```

| Layer | Responsibility | Module |
|---|---|---|
| Ingestion | Copy sources unchanged, stamp lineage (`_source`, `_ingested_at`, `_run_id`, `_pipeline_version`) | `pipeline/bronze.py` |
| Transformation | Enforce schema contracts, cast types, route bad rows to quarantine, de-duplicate, derive columns | `pipeline/silver.py`, `contracts.py`, `quality.py` |
| Serving | Build the Gold tables, publish to S3 and PostgreSQL | `pipeline/gold.py` |
| Verification | Cross-layer row-count and metric reconciliation | `pipeline/reconcile.py` |
| Shared | Env config, storage (S3/local), DB, logging, retry, run tracking | `config.py`, `storage.py`, `db.py`, `observability.py` |

### Key design choices
* **Idempotent by design.** Every write replaces its folder (Bronze snapshot per `load_date`, Silver/Gold full overwrite,
  Gold tables in PostgreSQL drop-and-recreate in one transaction). Re-running produces identical output — tested.
* **Incremental raw ingestion.** Learning files already in Bronze with identical bytes are skipped; changed files are logged and replaced.
* **Quarantine, don't crash.** Invalid rows go to `quarantine/` with every rule they broke (`_dq_reasons`); valid rows continue.
  Missing required *columns* (schema drift) fail fast with a clear error. A known legacy field (`notes`) is mapped to `text`.
* **Partitioning.** Raw events keep the source `year=/month=/day=` layout; Silver events are partitioned by `event_year`,
  so time-bounded reads touch only the needed folders. Small dimension tables are single Parquet files (no small-file overhead).
* **Local and cloud from the same code.** `IS_LOCAL=true` → LocalStack endpoint, no SSL; `IS_LOCAL=false` → real AWS S3,
  `sslmode=require` for Aurora, and cloud defaults for the lake bucket and raw-event location. Everything else comes from
  environment variables. (The workshop's EKS job deploy still needs two changes before it can run this job — see *Cloud mode*.) The lake bucket is created on first use in
  whatever `AWS_REGION` is set (with a `LocationConstraint` outside `us-east-1`, as S3 requires — the VDI uses `ap-south-1`).
* **Resilience.** S3 and PostgreSQL calls retry with exponential backoff (1s, 2s, 4s, 8s); zero-row sources halt the run
  (`FAIL_ON_EMPTY=true`); a run manifest is written even when the run fails.

---

## 3. Setup and run (workshop VDI)

**Prerequisites:** local PostgreSQL is running, and LocalStack is up (`localstack start -d`, the step `bin/start-dev.sh` uses).
LocalStack now requires `LOCALSTACK_AUTH_TOKEN` to start — export it in `~/.bashrc` as `docs/validation.md` describes.

**Environment variables.** On the VDI, `~/.bashrc` exports `PARTICIPANT_ID` and `AWS_REGION` but **not** the `POSTGRES_*`
variables, and it returns early in non-interactive shells, so `bash scripts/run_all.sh` would never see its exports.
`scripts/env.sh` handles both: it reads the `export` lines from `~/.bashrc`, then fills in the workshop's local defaults
(from `bin/setup-environment.sh` / `infra/locals.tf`) for anything still unset. Values you export yourself always win.

| Variable | Local default set by `scripts/env.sh` |
|---|---|
| `IS_LOCAL` | `true` |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` |
| `POSTGRES_NAME` / `POSTGRES_USER` / `POSTGRES_PASS` | `postgres` / `postgres` / `postgres123` |
| `AWS_ENDPOINT_URL` | `http://localhost:4566` |

`run_all.sh` sources it automatically; for the manual steps below, `source scripts/env.sh` first.

```bash
# 0. Put this folder at  coding-workshop-participant/data/talent-pipeline
cd ~/coding-workshop-participant/data/talent-pipeline
source scripts/env.sh

# 1. Install dependencies into the same interpreter that runs the scripts
python -m pip install --user -r requirements.txt

# 2. Load the source data into PostgreSQL (once)
bash ~/data-engineering-performance-talent-management/load.sh

# 3. Run the pipeline
python job.py 2>&1 | tee /tmp/talent-pipeline.log

# 4. Tests (offline unit + end-to-end), then integration against real PostgreSQL + S3
python -m pytest --cov=pipeline --cov=job
RUN_INTEGRATION=1 python -m pytest

# 5. Insights
python -m notebook notebooks/talent_insights.ipynb    # or open notebooks/talent_insights.html
psql "host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER" -f sql/business_questions.sql
```

Or do steps 3–5 in one go: `bash scripts/run_all.sh`. It sources `scripts/env.sh`, then runs everything with the same
`python` — including the notebook (`python -m nbconvert`), because the `jupyter` launchers in `~/.local/bin` can belong
to a different Python version than the one the requirements were installed into.

**pandas 3.** `pandas>=2.2` currently installs pandas 3.0.x. The pipeline, all 64 tests and the notebook were re-run on
pandas 3.0.6 and every business number is unchanged; pin `pandas<3` if you need the 2.x behaviour.

### Configuration (environment variables)

| Variable | Default | Purpose |
|---|---|---|
| `IS_LOCAL` | `true` | Local (LocalStack, no SSL) vs cloud (AWS, SSL) |
| `DATA_LAKE_URI` | local: `s3://talent-lake-<PARTICIPANT_ID>`; cloud: `s3://coding-workshop-<id>-talent-lake-<id>` | Lake location; a local folder path also works. The cloud name matches the workshop IAM bucket pattern `<app_name>*<app_id>*` |
| `AWS_ENDPOINT_URL` | `http://localhost:4566` (local only) | LocalStack endpoint |
| `POSTGRES_HOST/PORT/NAME/USER/PASS` | workshop-provided | Source DB and Gold publishing |
| `SOURCE_DIR` | `~/data-engineering-performance-talent-management` | Dataset folder |
| `LEARNING_ACTIVITY_URI` | local: `$SOURCE_DIR/learning_activity`; cloud: `<lake>/landing/learning_activity` | Raw events (folder or `s3://…`); upload them with `scripts/upload_source.py` |
| `AWS_REGION` | `AWS_DEFAULT_REGION`, then `APP_REGION`, then `us-east-1` | Region for S3 clients and bucket creation |
| `SOURCE_MODE` | `postgres` | `csv` reads the CSVs directly (offline runs, tests) |
| `WRITE_GOLD_TO_POSTGRES` | `true` | Also publish Gold to the `gold` schema |
| `RUN_DATE` | today | Logical run date; reprocess a date with `--run-date` |
| `FAIL_ON_EMPTY` | `true` | Halt when a source/table has zero rows |
| `MAX_RETRIES` / `RETRY_BASE_DELAY` | `4` / `1.0` | Backoff for S3/PostgreSQL |

Useful options: `python job.py --steps silver,gold`, `python job.py --steps reconcile` (audit the lake on its own).

### Cloud mode
**Status: not yet deployable with the workshop's unmodified EKS job chart.** The pipeline side is ready and tested; the
deploy side has two gaps in `infra/` (shared workshop files, so they are left unchanged pending a decision):

1. **Only `job.py` reaches the pod.** `infra/helm/templates/python-job.yaml` mounts just `job.py` and `requirements.txt`
   from a ConfigMap, so `from pipeline...` fails with `ModuleNotFoundError`. The `pipeline/` modules must be shipped too.
2. **No environment variables reach the pod.** `locals.env_vars` (`IS_LOCAL`, `POSTGRES_*`, `APP_REGION`) is injected
   into the Lambdas only; the job template has no `env`. Without them the job would look for PostgreSQL on `localhost`.

What is already in place for cloud runs:
* `IS_LOCAL=false` → no endpoint override (real S3), `sslmode=require` for Aurora.
* Lake bucket defaults to `s3://coding-workshop-<id>-talent-lake-<id>`, which the workshop IAM policy allows
  (`talent-lake-<id>` would be denied). It is created in `AWS_REGION` / `APP_REGION` with the right `LocationConstraint`.
* Raw events are read from S3 (`<lake>/landing/learning_activity`), because the pod has no dataset folder. Upload them once
  from the VDI: `IS_LOCAL=false python scripts/upload_source.py` (re-runs skip unchanged files). Reading from S3 is covered
  by `test_pipeline_reads_raw_learning_events_from_s3` against LocalStack.
* Aurora must be loaded once with `load.sh`, using the Aurora `POSTGRES_*` values and `IS_LOCAL=false` (SSL).

Once the deploy gaps are closed: `echo "export TF_VAR_aws_eks_enabled=true" >> ~/.bashrc`, `source ~/.bashrc`,
`../bin/deploy-backend.sh`, then `kubectl logs -f job/coding-workshop-talent-pipeline`.

### Troubleshooting
| Symptom | Fix |
|---|---|
| `Could not connect to the endpoint URL ... 4566` | LocalStack is not running — `localstack start -d` |
| LocalStack exits: `No credentials were found ... LOCALSTACK_AUTH_TOKEN` | Export your token in `~/.bashrc` (`docs/validation.md`), `source ~/.bashrc`, start again |
| `localstack` CLI crashes with `module 'lib' has no attribute 'GEN_EMAIL'` | Stale system pyOpenSSL: `python -m pip install --user -U pyopenssl` |
| `POSTGRES_* <unset>` / `fe_sendauth: no password supplied` | `source scripts/env.sh` (run_all.sh does this); override any `POSTGRES_*` value by exporting it |
| `IllegalLocationConstraintException` on `CreateBucket` | Fixed in `storage.py` (bucket is created in `AWS_REGION`); pull the latest code |
| Notebook fails with `ImportError` from `.../python3.14/...` | Kernel ran on another Python — use `python -m nbconvert` / `python -m notebook`, as `run_all.sh` does |
| `relation "teams" does not exist` | Run `load.sh` (step 2) |
| `relation already exists` when re-running `load.sh` | Data is already loaded — skip step 2 |
| Reconciliation check failed | Read the `reconciliation failed` log line; it names the table and the expected vs actual counts |

---

## 4. Data quality findings in the source data

The pipeline found and handled every one of these (details in each run manifest and in `quarantine/`):

| Issue (learning_activity) | Rows | Handling |
|---|---|---|
| Exact duplicate events | 151 | Dropped (counted in reconciliation) |
| Missing `employee_id` | 76 | Quarantined |
| Unparseable `event_date` (`N/A`, `yesterday`) | 39 | Quarantined |
| Blank `event_date` | 15 | Quarantined |
| Truncated / malformed JSON lines | 13 | Quarantined with the raw text |
| Legacy field `notes` instead of `text` | 39 | Schema drift handled: mapped to `text` |
| Empty file (`year=2025/month=09/day=03`) | 1 file | Logged as warning |

The seven PostgreSQL tables passed every contract (0 rejected rows). Overall 294 of 29,868 event lines (1.0%) were excluded.

---

## 5. Assumptions, trade-offs and known limitations

* **pandas instead of Spark.** The data is small (~45k rows); pandas is simpler and faster here. The benchmark shows linear
  scaling (~630k rows/min at 1×, 5× and 10× volume, peak memory 1.1 GB at 10×). Beyond a few million rows per run the same
  layer boundaries could move to PySpark.
* **Full-snapshot Bronze for tables** (no CDC): the source tables are small; each run snapshots them under `load_date=`.
* **Two review cycles only** (2024-H2, 2025-H1); trends compare these two. The 2024-H2 cycle only used ratings 2–4, so part
  of the apparent improvement may be a change in scale usage.
* **One talent-review cycle** (2025-H1): potential, readiness and attrition risk are point-in-time.
* **Attrition score is rule-based** and explainable (`risk_reasons`), not a trained model.
* **Small samples:** 27 teams have projects (33 projects); department × competency cells average ~10 assessments.
  Correlations are signals to investigate, not proof of cause.
* **Cloud deployment is not done yet.** The `IS_LOCAL=false` settings are unit-tested and reading raw events from S3 is
  integration-tested, but the workshop's EKS job chart ships neither the `pipeline/` modules nor the env vars (see *Cloud mode*).
  The pipeline was exercised end-to-end locally (PostgreSQL + LocalStack Pro S3, `AWS_REGION=ap-south-1`), not in a live AWS account.
* All names in the dataset are synthetic.

---

## 6. Project layout

```
talent-pipeline/
├── job.py                      # entry point (bronze → silver → gold → reconcile)
├── pipeline/
│   ├── config.py               # env-driven settings (local vs cloud)
│   ├── observability.py        # JSON logging, retry/backoff, run tracker
│   ├── storage.py              # S3 / local storage abstraction
│   ├── db.py                   # PostgreSQL read/write
│   ├── contracts.py            # schema contracts per table
│   ├── quality.py              # contract enforcement + quarantine
│   ├── bronze.py  silver.py  gold.py  reconcile.py
├── tests/                      # unit, end-to-end, integration (64 tests)
├── notebooks/talent_insights.ipynb  (+ .html)
├── sql/business_questions.sql
├── scripts/run_all.sh  scripts/env.sh  scripts/upload_source.py  scripts/benchmark.py
└── docs/TEST_EVIDENCE.md  docs/LINEAGE.md
```
