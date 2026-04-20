#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DT_FROM="${1:?dt_from required (e.g. 2026-04-01)}"
DT_TO="${2:?dt_to required (e.g. 2026-04-30)}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"

RUN_RESET_FIRST="${RUN_RESET_FIRST:-true}"
RUN_SOURCE_SIMULATION="${RUN_SOURCE_SIMULATION:-true}"
RUN_SOURCE_SAFE_PARSE_LOAD="${RUN_SOURCE_SAFE_PARSE_LOAD:-true}"
RUN_STREAM_ADAPTER="${RUN_STREAM_ADAPTER:-true}"
RUN_BATCH_ADAPTER="${RUN_BATCH_ADAPTER:-true}"
RUN_PRE_ML="${RUN_PRE_ML:-true}"
RUN_STREAMING_PIPELINE="${RUN_STREAMING_PIPELINE:-true}"
RUN_ML="${RUN_ML:-false}"
RUN_AI="${RUN_AI:-false}"
USE_SCENARIO_STREAM_INJECTION="${USE_SCENARIO_STREAM_INJECTION:-true}"
RUN_STREAM_POST_PROCESS="${RUN_STREAM_POST_PROCESS:-true}"

mysql_query() {
  mysql -N -B \
    -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
    -e "$1"
}

CURRENT="$DT_FROM"
FIRST_RUN=1

while [[ "$CURRENT" < "$DT_TO" || "$CURRENT" == "$DT_TO" ]]; do
  ROW=$(mysql_query "
    SELECT
      COALESCE(scenario_name, 'baseline') AS scenario_name,
      COALESCE(NULLIF(scenario_intensity,''), NULLIF(scenario_severity,''), 'medium') AS intensity
    FROM scenario_experiment_run
    WHERE profile_id='${PROFILE_ID}'
      AND dt_from='${CURRENT}'
      AND dt_to='${CURRENT}'
    ORDER BY scenario_run_id DESC
    LIMIT 1;
  ")

  if [[ -z "$ROW" ]]; then
    SCENARIO_NAME="baseline"
    INTENSITY="medium"
  else
    SCENARIO_NAME="$(echo "$ROW" | awk '{print $1}')"
    INTENSITY="$(echo "$ROW" | awk '{print $2}')"
  fi

  echo "=================================================="
  echo "[RUN] date=${CURRENT} scenario=${SCENARIO_NAME} intensity=${INTENSITY}"
  echo "=================================================="

  RUN_RESET="false"
  if [[ "$FIRST_RUN" -eq 1 && "$RUN_RESET_FIRST" == "true" ]]; then
    RUN_RESET="true"
  fi

  DB_HOST="$DB_HOST" \
  DB_PORT="$DB_PORT" \
  DB_USER="$DB_USER" \
  DB_PASSWORD="$DB_PASSWORD" \
  DB_NAME="$DB_NAME" \
  KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
  RUN_RESET="$RUN_RESET" \
  RUN_SOURCE_SIMULATION="$RUN_SOURCE_SIMULATION" \
  RUN_SOURCE_SAFE_PARSE_LOAD="$RUN_SOURCE_SAFE_PARSE_LOAD" \
  RUN_STREAM_ADAPTER="$RUN_STREAM_ADAPTER" \
  RUN_BATCH_ADAPTER="$RUN_BATCH_ADAPTER" \
  RUN_PRE_ML="$RUN_PRE_ML" \
  RUN_STREAMING_PIPELINE="$RUN_STREAMING_PIPELINE" \
  RUN_ML="$RUN_ML" \
  RUN_AI="$RUN_AI" \
  USE_SCENARIO_STREAM_INJECTION="$USE_SCENARIO_STREAM_INJECTION" \
  RUN_STREAM_POST_PROCESS="$RUN_STREAM_POST_PROCESS" \
  bash "$PROJECT_ROOT/deploy/run_unified_scenario_all_in_one_v2.sh" \
    "$SCENARIO_NAME" "$CURRENT" "$CURRENT" "$PROFILE_ID" "$INTENSITY"

  FIRST_RUN=0
  CURRENT=$(date -I -d "$CURRENT + 1 day")
done

echo "[DONE] scenario-driven monthly loop completed profile=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO}"
