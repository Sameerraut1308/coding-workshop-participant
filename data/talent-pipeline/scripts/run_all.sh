#!/usr/bin/env bash
# One command to run everything on the workshop VDI:
#   1. check the environment   2. run the pipeline (seed -> bronze -> silver -> gold -> reconcile)
#   3. run the tests            4. execute the notebook (and export it to HTML)
#
# The seed step only loads PostgreSQL when SEED_SOURCE_URI is set (default: off locally,
# <lake>/landing/source with IS_LOCAL=false), so locally it does nothing; load.sh loads the database.
# Lake bucket (DATA_LAKE_URI) default: s3://talent-lake-<id> locally, s3://coding-workshop-<id>-talent-lake-<id>
# with IS_LOCAL=false, where <id> is PARTICIPANT_ID, else APP_ID, else "local".
#
# Usage (from data/talent-pipeline):   bash scripts/run_all.sh
# Options via env vars:  SKIP_TESTS=1  SKIP_NOTEBOOK=1  RUN_INTEGRATION=1
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/env.sh
source scripts/env.sh

LOG=/tmp/talent-pipeline.log
echo "== 1/4 Environment check =="
echo "  IS_LOCAL=${IS_LOCAL:-<unset, defaults to true>}"
echo "  POSTGRES_HOST=${POSTGRES_HOST:-<unset>} POSTGRES_PORT=${POSTGRES_PORT:-<unset>} POSTGRES_NAME=${POSTGRES_NAME:-<unset>}"
echo "  DATA_LAKE_URI=${DATA_LAKE_URI:-<unset, default lake bucket (see top of this script)>}"
SOURCE_DIR="${SOURCE_DIR:-$HOME/data-engineering-performance-talent-management}"
if [ ! -d "$SOURCE_DIR/learning_activity" ]; then
    echo "  ERROR: dataset not found at $SOURCE_DIR (set SOURCE_DIR)"; exit 1
fi
echo "  dataset: $SOURCE_DIR"

echo "== 2/4 Running pipeline (log: $LOG) =="
if python job.py 2>&1 | tee "$LOG" | grep -E '"level": "(WARNING|ERROR)"|job finished' ; then :; fi
if ! grep -q '"status": "success"' <(tail -1 "$LOG"); then
    echo "  Pipeline FAILED - see $LOG"; exit 1
fi
echo "  Pipeline succeeded."

if [ "${SKIP_TESTS:-0}" != "1" ]; then
    echo "== 3/4 Running tests =="
    python -m pytest --cov=pipeline --cov=job --cov-report=term | tee /tmp/talent-pipeline-tests.log | tail -25
fi

if [ "${SKIP_NOTEBOOK:-0}" != "1" ]; then
    echo "== 4/4 Executing notebook =="
    (cd notebooks && python -m nbconvert --to notebook --execute --inplace talent_insights.ipynb \
        && python -m nbconvert --to html talent_insights.ipynb --output talent_insights.html)
    echo "  Open notebooks/talent_insights.html in a browser, or run: jupyter notebook notebooks/talent_insights.ipynb"
fi
echo "Done."
