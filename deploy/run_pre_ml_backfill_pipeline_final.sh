#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

START_DATE="${1:?start date required}"
END_DATE="${2:?end date required}"
PROFILE_ID="${3:?profile id required}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:?DB_USER required}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_NAME="${DB_NAME:?DB_NAME required}"

BASE_URL="${BASE_URL:-https://www.finance-bank.example.com}"
PROFILE_YAML="${PROFILE_YAML:-$PROJECT_ROOT/configs/profiles/${PROFILE_ID}.yaml}"

SOURCE_LOG_PATH="${SOURCE_LOG_PATH:-/mnt/d/etl_storage/log/logdata/finance/${PROFILE_ID}_base.log}"
STAGE_DIR="${STAGE_DIR:-/mnt/d/etl_storage/log/logdata/finance/staging}"
mkdir -p "$STAGE_DIR"

RANGE_TSV="${STAGE_DIR}/${PROFILE_ID}_${START_DATE}_${END_DATE}.tsv"

WRITE_LEGACY="${WRITE_LEGACY:-true}"
RUN_SIMULATION="${RUN_SIMULATION:-false}"

RUN_SOURCE_SAFE_PARSE_LOAD="${RUN_SOURCE_SAFE_PARSE_LOAD:-true}"
RUN_COLLECTOR="${RUN_COLLECTOR:-true}"
RUN_EVENT_LEDGER="${RUN_EVENT_LEDGER:-true}"
RUN_BATCH_LOAD="${RUN_BATCH_LOAD:-true}"
RUN_ANALYZER="${RUN_ANALYZER:-true}"
RUN_MAPPING_COVERAGE="${RUN_MAPPING_COVERAGE:-true}"
RUN_VALIDATION="${RUN_VALIDATION:-true}"
RUN_DRIFT="${RUN_DRIFT:-true}"
RUN_TIME_ANOMALY="${RUN_TIME_ANOMALY:-true}"
RUN_CORR_ANOMALY="${RUN_CORR_ANOMALY:-true}"
RUN_CREATE_CONTROL_TABLES="${RUN_CREATE_CONTROL_TABLES:-true}"
RUN_RISK_V4="${RUN_RISK_V4:-true}"
RUN_ROOT_CAUSE="${RUN_ROOT_CAUSE:-true}"
RUN_ACTION_ENGINE="${RUN_ACTION_ENGINE:-true}"

check_file_exists() {
  local file_path="$1"
  local label="$2"
  if [[ ! -f "$file_path" ]]; then
    echo "[ERROR] $label not found: $file_path"
    exit 1
  fi
}

run_mysql_file() {
  local sql_file="$1"
  if [[ -f "$sql_file" ]]; then
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" < "$sql_file"
  else
    echo "[WARN] SQL file not found, skip: $sql_file"
  fi
}

echo "[INFO] PRE-ML BACKFILL FINAL START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"
echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] RUN_SIMULATION=$RUN_SIMULATION"
echo "[INFO] RUN_SOURCE_SAFE_PARSE_LOAD=$RUN_SOURCE_SAFE_PARSE_LOAD"

if [[ "$RUN_SIMULATION" == "true" ]]; then
  echo "[STEP 1] weblog simulation"
  "$PYTHON_BIN" "$PROJECT_ROOT/simulator/weblog_sim/cli.py" \
    --profile "$PROFILE_YAML" \
    --start "${START_DATE}T00:00:00" \
    --end "${END_DATE}T23:59:59" \
    --avg-rps 1 \
    --seed 42 \
    --out "$SOURCE_LOG_PATH"
else
  echo "[STEP 1] weblog simulation skipped"
  check_file_exists "$SOURCE_LOG_PATH" "existing source log"
fi

if [[ "$RUN_SOURCE_SAFE_PARSE_LOAD" == "true" ]]; then
  echo "[STEP 2] source-safe parse"
  check_file_exists "$SOURCE_LOG_PATH" "source log"

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/ingest/parse_webserver_log_range_safe.py" \
    --base-url "$BASE_URL" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --dedup \
    "$SOURCE_LOG_PATH" "$RANGE_TSV"

  echo "[STEP 3] source-safe load"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/ingest/load_tsv_to_db_range_safe.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --table stg_webserver_log_hit \
    --tsv "$RANGE_TSV" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --delete-date-range
fi

if [[ "$RUN_COLLECTOR" == "true" ]]; then
  echo "[STEP 4] collector normalization"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/batch/collector_a_v3.py" \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    --db-user "$DB_USER" \
    --db-pass "$DB_PASSWORD" \
    --db-name "$DB_NAME" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate-target
fi

if [[ "$RUN_EVENT_LEDGER" == "true" ]]; then
  echo "[STEP 5] canonical event ledger"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/source_event/event_log_raw_builder.py" \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    --db-user "$DB_USER" \
    --db-pass "$DB_PASSWORD" \
    --db-name "$DB_NAME" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate-target
fi

if [[ "$RUN_BATCH_LOAD" == "true" ]]; then
  echo "[STEP 6] batch ingest load"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/batch/batch_loader_from_event_log_raw.py" \
    --db-host "$DB_HOST" \
    --db-port "$DB_PORT" \
    --db-user "$DB_USER" \
    --db-pass "$DB_PASSWORD" \
    --db-name "$DB_NAME" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate-target
fi

if [[ "$RUN_ANALYZER" == "true" ]]; then
  echo "[STEP 7] batch analyzer"
  ANALYZER_ARGS=(
    --db-host "$DB_HOST"
    --db-port "$DB_PORT"
    --db-user "$DB_USER"
    --db-pass "$DB_PASSWORD"
    --db-name "$DB_NAME"
    --profile-id "$PROFILE_ID"
    --dt-from "$START_DATE"
    --dt-to "$END_DATE"
    --identity-mode uid_pcid_ip
    --session-timeout-sec 1800
    --pv-mode view_only
    --truncate-target
  )
  if [[ "$WRITE_LEGACY" == "true" ]]; then
    ANALYZER_ARGS+=(--write-legacy)
  fi

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/batch/analyzer_b_v5.py" "${ANALYZER_ARGS[@]}"
fi

if [[ "$RUN_MAPPING_COVERAGE" == "true" ]]; then
  echo "[STEP 8] mapping coverage"
  if [[ -f "$PROJECT_ROOT/pipelines/mapping_coverage_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/mapping_coverage_runner.py" \
      --host "$DB_HOST" \
      --port "$DB_PORT" \
      --user "$DB_USER" \
      --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --dt-from "$START_DATE" \
      --dt-to "$END_DATE"
  else
    echo "[WARN] mapping_coverage_runner.py not found, skip"
  fi
fi

if [[ "$RUN_VALIDATION" == "true" ]]; then
  echo "[STEP 9] validation"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/validation_layer_runner_v2.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
fi

if [[ "$RUN_DRIFT" == "true" ]]; then
  echo "[STEP 10] drift"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    DB_BACKEND=mysql \
    DB_HOST="$DB_HOST" \
    DB_PORT="$DB_PORT" \
    DB_USER="$DB_USER" \
    DB_PASSWORD="$DB_PASSWORD" \
    DB_NAME="$DB_NAME" \
    Rscript "$PROJECT_ROOT/r/metric_drift_analysis_db_v8.R" \
      --date "$CURRENT" \
      --profile-id "$PROFILE_ID"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

if [[ "$RUN_TIME_ANOMALY" == "true" ]]; then
  echo "[STEP 11] time anomaly"
  if [[ -f "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" \
      --host "$DB_HOST" \
      --port "$DB_PORT" \
      --user "$DB_USER" \
      --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --dt-from "$START_DATE" \
      --dt-to "$END_DATE" \
      --truncate
  else
    echo "[WARN] time_pattern_anomaly_runner.py not found, skip"
  fi
fi

if [[ "$RUN_CORR_ANOMALY" == "true" ]]; then
  echo "[STEP 12] correlation anomaly"
  if [[ -f "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" \
      --host "$DB_HOST" \
      --port "$DB_PORT" \
      --user "$DB_USER" \
      --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --dt-from "$START_DATE" \
      --dt-to "$END_DATE" \
      --truncate
  else
    echo "[WARN] correlation_anomaly_runner.py not found, skip"
  fi
fi

if [[ "$RUN_CREATE_CONTROL_TABLES" == "true" ]]; then
  echo "[STEP 13] control tables"
  run_mysql_file "$PROJECT_ROOT/sql/07_reliability_control_tables.sql"
  run_mysql_file "$PROJECT_ROOT/sql/08_scenario_experiment_tables.sql"
fi

if [[ "$RUN_RISK_V4" == "true" ]]; then
  echo "[STEP 14] risk v4"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_day_v4_runner.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
fi

if [[ "$RUN_ROOT_CAUSE" == "true" ]]; then
  echo "[STEP 15] root cause"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/root_cause_and_contribution_runner.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
fi

if [[ "$RUN_ACTION_ENGINE" == "true" ]]; then
  echo "[STEP 16] action engine"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/action_engine_runner_v2.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
fi

echo "[DONE] pre-ML backfill final completed"
