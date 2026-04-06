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

echo "[INFO] AI LAYER START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"

mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < "$PROJECT_ROOT/sql/ai_layer_tables.sql"

"$PYTHON_BIN" "$PROJECT_ROOT/ai/ai_daily_summary_runner.py" \
  --host "$DB_HOST" --port "$DB_PORT" \
  --user "$DB_USER" --password "$DB_PASSWORD" \
  --db "$DB_NAME" \
  --profile-id "$PROFILE_ID" \
  --dt-from "$START_DATE" --dt-to "$END_DATE"

echo "[DONE] AI layer pipeline completed"
