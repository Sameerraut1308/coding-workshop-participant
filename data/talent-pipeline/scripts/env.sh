# Load the workshop environment for non-interactive runs.  Usage:  source scripts/env.sh
#
# ~/.bashrc returns early in non-interactive shells (`case $- in *i*`), so `bash scripts/run_all.sh`
# never sees its exports. Pull the `export` lines out of it, then fall back to the workshop's local
# defaults (bin/setup-environment.sh, infra/locals.tf) for anything still unset. Values already set
# in the caller's environment always win.

if [ -f "$HOME/.bashrc" ]; then
    set +u
    eval "$(grep -E '^[[:space:]]*export[[:space:]]+[A-Za-z_][A-Za-z0-9_]*=' "$HOME/.bashrc")"
    set -u
fi

export IS_LOCAL="${IS_LOCAL:-true}"
if [ "$IS_LOCAL" = "true" ]; then
    export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    export POSTGRES_NAME="${POSTGRES_NAME:-postgres}"
    export POSTGRES_USER="${POSTGRES_USER:-postgres}"
    export POSTGRES_PASS="${POSTGRES_PASS:-postgres123}"
    export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
fi
