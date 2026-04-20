#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
START_DATE="${1:?start_date required}"
END_DATE="${2:?end_date required}"
PROFILE_ID="${3:-finance_bank}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"
RUN_RESET="${RUN_RESET:-false}"
RESET_MODE="${RESET_MODE:-ai_only}"
FORCE_FALLBACK="${FORCE_FALLBACK:-false}"
echo "[INFO] AI LAYER V2 START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"
if [[ "$RUN_RESET" == "true" ]]; then
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" bash "$PROJECT_ROOT/deploy/reset_ai_layer_tables_v2.sh" "$RESET_MODE"
fi
EXTRA_ARGS=()
if [[ "$FORCE_FALLBACK" == "true" ]]; then EXTRA_ARGS+=(--force-fallback); fi
"$PYTHON_BIN" "$PROJECT_ROOT/ai/ai_daily_summary_runner_v2.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" "${EXTRA_ARGS[@]}"
echo "[DONE] AI layer pipeline v2 completed"
