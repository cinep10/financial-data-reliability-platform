#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

START_DATE="${1:-2026-02-23}"
END_DATE="${2:-2026-03-09}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

BASE_URL="${BASE_URL:-https://www.finance-bank.example.com}"
PROFILE_YAML="${PROFILE_YAML:-$PROJECT_ROOT/configs/profiles/finance_bank.yaml}"
LOG_OUT="${LOG_OUT:-/mnt/d/etl_storage/log/logdata/finance/finance_bank_base.log}"
TSV_OUT="${TSV_OUT:-/mnt/d/etl_storage/log/logdata/finance/finance_bank_base.tsv}"

WRITE_LEGACY="${WRITE_LEGACY:-true}"

# 운영 친화 기본값: 시뮬레이션은 기본 OFF
RUN_SIMULATION="${RUN_SIMULATION:-false}"
RUN_PARSE_LOAD="${RUN_PARSE_LOAD:-true}"
RUN_COLLECTOR="${RUN_COLLECTOR:-true}"
RUN_ANALYZER="${RUN_ANALYZER:-true}"
RUN_VALIDATION="${RUN_VALIDATION:-true}"
RUN_DRIFT="${RUN_DRIFT:-true}"
RUN_RISK_V2="${RUN_RISK_V2:-true}"

# reliability control
RUN_CREATE_CONTROL_TABLES="${RUN_CREATE_CONTROL_TABLES:-true}"
RUN_ROOT_CAUSE="${RUN_ROOT_CAUSE:-true}"
RUN_TIME_ANOMALY="${RUN_TIME_ANOMALY:-true}"
RUN_CORR_ANOMALY="${RUN_CORR_ANOMALY:-true}"

PYTHON_BIN="${PYTHON_BIN:-python3}"

export DB_BACKEND="${DB_BACKEND:-mysql}"
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

echo "[INFO] PRE-ML BACKFILL START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"
echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] RUN_SIMULATION=$RUN_SIMULATION"

check_file_exists() {
  local file_path="$1"
  local label="$2"
  if [[ ! -f "$file_path" ]]; then
    echo "[ERROR] $label not found: $file_path"
    exit 1
  fi
}

if [[ "$RUN_SIMULATION" == "true" ]]; then
  echo "[STEP 1] weblog simulation"
  "$PYTHON_BIN" "$PROJECT_ROOT/simulator/weblog_sim/cli.py" \
    --profile "$PROFILE_YAML" \
    --start "${START_DATE}T00:00:00" \
    --end "${END_DATE}T23:59:59" \
    --avg-rps 1 \
    --seed 42 \
    --out "$LOG_OUT"
else
  echo "[STEP 1] weblog simulation skipped (RUN_SIMULATION=false)"
  check_file_exists "$LOG_OUT" "existing log file"
fi

if [[ "$RUN_PARSE_LOAD" == "true" ]]; then
  echo "[STEP 2] parse + load raw log"
  check_file_exists "$LOG_OUT" "log file for parse"

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/parse_webserver_log.py" \
    --base-url "$BASE_URL" \
    "$LOG_OUT" \
    "$TSV_OUT"

  check_file_exists "$TSV_OUT" "generated TSV file"

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/load_tsv_to_db_v2.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --table stg_webserver_log_hit \
    --tsv "$TSV_OUT" \
    --columns "dt,ts,ip,method,url_raw,url_full,url_norm,host,path,query,status,bytes,latency_ms,ref,ref_host,ua,kv_raw,uid,pcid,sid,device_type,evt,accept_lang,cc,page_type" \
    --truncate-target
else
  echo "[STEP 2] parse + load skipped"
fi

if [[ "$RUN_COLLECTOR" == "true" ]]; then
  echo "[STEP 3] collector"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/collector_a_v2.py" \
    --db-host "$DB_HOST" --db-port "$DB_PORT" \
    --db-user "$DB_USER" --db-pass "$DB_PASSWORD" --db-name "$DB_NAME" \
    --dt-from "$START_DATE" --dt-to "$END_DATE" \
    --base-url "$BASE_URL" \
    --seed 42 \
    --truncate-target
else
  echo "[STEP 3] collector skipped"
fi

if [[ "$RUN_ANALYZER" == "true" ]]; then
  echo "[STEP 4] analyzer"
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

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/analyzer_b_v4.py" "${ANALYZER_ARGS[@]}"
else
  echo "[STEP 4] analyzer skipped"
fi

if [[ "$RUN_VALIDATION" == "true" ]]; then
  echo "[STEP 5] validation"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/validation_layer_runner_v2.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
else
  echo "[STEP 5] validation skipped"
fi

if [[ "$RUN_DRIFT" == "true" ]]; then
  echo "[STEP 6] drift v1/v2 backfill"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    echo "  -> drift date=$CURRENT"
    Rscript "$PROJECT_ROOT/r/metric_drift_analysis_db_v7.R" \
      --date "$CURRENT" \
      --profile-id "$PROFILE_ID"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
else
  echo "[STEP 6] drift skipped"
fi

if [[ "$RUN_RISK_V2" == "true" ]]; then
  echo "[STEP 7] risk score v2 backfill"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    echo "  -> risk v2 date=$CURRENT"
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_runner_v2.py" \
      --host "$DB_HOST" --port "$DB_PORT" \
      --user "$DB_USER" --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --date "$CURRENT"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
else
  echo "[STEP 7] risk v2 skipped"
fi

if [[ "$RUN_CREATE_CONTROL_TABLES" == "true" ]]; then
  echo "[STEP 8] create reliability control tables"
  mysql \
    -h "$DB_HOST" \
    -P "$DB_PORT" \
    -u "$DB_USER" \
    -p"$DB_PASSWORD" \
    "$DB_NAME" < "$PROJECT_ROOT/sql/07_reliability_control_tables.sql"
else
  echo "[STEP 8] create control tables skipped"
fi

if [[ "$RUN_ROOT_CAUSE" == "true" ]]; then
  echo "[STEP 9] root cause + contribution"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/root_cause_and_contribution_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
else
  echo "[STEP 9] root cause skipped"
fi

if [[ "$RUN_TIME_ANOMALY" == "true" ]]; then
  echo "[STEP 10] rolling time anomaly"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
else
  echo "[STEP 10] time anomaly skipped"
fi

if [[ "$RUN_CORR_ANOMALY" == "true" ]]; then
  echo "[STEP 11] correlation anomaly"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --truncate
else
  echo "[STEP 11] correlation anomaly skipped"
fi

echo "[DONE] pre-ML backfill pipeline completed"
