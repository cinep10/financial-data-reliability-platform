#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

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
RUN_SIMULATION="${RUN_SIMULATION:-false}"
RUN_PARSE_LOAD="${RUN_PARSE_LOAD:-true}"
RUN_COLLECTOR="${RUN_COLLECTOR:-true}"
RUN_ANALYZER="${RUN_ANALYZER:-true}"
RUN_MAPPING_COVERAGE="${RUN_MAPPING_COVERAGE:-true}"
RUN_VALIDATION="${RUN_VALIDATION:-true}"
RUN_DRIFT="${RUN_DRIFT:-true}"
RUN_TIME_ANOMALY="${RUN_TIME_ANOMALY:-true}"
RUN_CORR_ANOMALY="${RUN_CORR_ANOMALY:-true}"
RUN_RISK_V2="${RUN_RISK_V2:-false}"
RUN_RISK_V3="${RUN_RISK_V3:-false}"
RUN_RISK_V4="${RUN_RISK_V4:-true}"
RUN_CREATE_CONTROL_TABLES="${RUN_CREATE_CONTROL_TABLES:-true}"
RUN_ROOT_CAUSE="${RUN_ROOT_CAUSE:-true}"
RUN_ACTION_ENGINE="${RUN_ACTION_ENGINE:-true}"

export DB_BACKEND="${DB_BACKEND:-mysql}"
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

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

if [[ "$RUN_SIMULATION" == "true" ]]; then
  echo "[STEP 1] weblog simulation"
  "$PYTHON_BIN" "$PROJECT_ROOT/simulator/weblog_sim/cli.py" --profile "$PROFILE_YAML" --start "${START_DATE}T00:00:00" --end "${END_DATE}T23:59:59" --avg-rps 1 --seed 42 --out "$LOG_OUT"
else
  echo "[STEP 1] weblog simulation skipped"
  check_file_exists "$LOG_OUT" "existing log file"
fi

if [[ "$RUN_PARSE_LOAD" == "true" ]]; then
  echo "[STEP 2] parse + load"
  check_file_exists "$LOG_OUT" "log file"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/parse_webserver_log.py" --base-url "$BASE_URL" "$LOG_OUT" "$TSV_OUT"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/load_tsv_to_db_v2.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --table stg_webserver_log_hit --tsv "$TSV_OUT" --columns "dt,ts,ip,method,url_raw,url_full,url_norm,host,path,query,status,bytes,latency_ms,ref,ref_host,ua,kv_raw,uid,pcid,sid,device_type,evt,accept_lang,cc,page_type" --truncate-target
fi

if [[ "$RUN_COLLECTOR" == "true" ]]; then
  echo "[STEP 3] collector"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/collector_a_v2.py" --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASSWORD" --db-name "$DB_NAME" --dt-from "$START_DATE" --dt-to "$END_DATE" --base-url "$BASE_URL" --seed 42 --truncate-target
fi

if [[ "$RUN_ANALYZER" == "true" ]]; then
  echo "[STEP 4] analyzer"
  ANALYZER_ARGS=(--db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASSWORD" --db-name "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --identity-mode uid_pcid_ip --session-timeout-sec 1800 --pv-mode view_only --truncate-target)
  if [[ "$WRITE_LEGACY" == "true" ]]; then ANALYZER_ARGS+=(--write-legacy); fi
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/analyzer_b_v4.py" "${ANALYZER_ARGS[@]}"
fi

if [[ "$RUN_MAPPING_COVERAGE" == "true" ]]; then
  echo "[STEP 5] mapping coverage"
  if [[ -f "$PROJECT_ROOT/pipelines/mapping_coverage_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/mapping_coverage_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE"
  else
    echo "[WARN] mapping_coverage_runner.py not found, skip"
  fi
fi

if [[ "$RUN_VALIDATION" == "true" ]]; then
  echo "[STEP 6] validation"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/validation_layer_runner_v2.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_DRIFT" == "true" ]]; then
  echo "[STEP 7] drift"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    DB_BACKEND=mysql DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" Rscript "$PROJECT_ROOT/r/metric_drift_analysis_db_v8.R" --date "$CURRENT" --profile-id "$PROFILE_ID"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

if [[ "$RUN_TIME_ANOMALY" == "true" ]]; then
  echo "[STEP 8] time anomaly"
  if [[ -f "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  else
    echo "[WARN] time_pattern_anomaly_runner.py not found, skip"
  fi
fi

if [[ "$RUN_CORR_ANOMALY" == "true" ]]; then
  echo "[STEP 9] correlation anomaly"
  if [[ -f "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  else
    echo "[WARN] correlation_anomaly_runner.py not found, skip"
  fi
fi

if [[ "$RUN_RISK_V2" == "true" ]]; then
  echo "[STEP 10] risk v2"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    if [[ -f "$PROJECT_ROOT/pipelines/risk_score_runner_v2.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_runner_v2.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --date "$CURRENT"
    else
      echo "[WARN] risk_score_runner_v2.py not found, skip"
      break
    fi
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

if [[ "$RUN_CREATE_CONTROL_TABLES" == "true" ]]; then
  echo "[STEP 11] control tables"
  run_mysql_file "$PROJECT_ROOT/sql/07_reliability_control_tables.sql"
  run_mysql_file "$PROJECT_ROOT/sql/08_scenario_experiment_tables.sql"
fi

if [[ "$RUN_RISK_V3" == "true" ]]; then
  echo "[STEP 12] risk v3"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_day_v3_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_RISK_V4" == "true" ]]; then
  echo "[STEP 13] risk v4"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_day_v4_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_ROOT_CAUSE" == "true" ]]; then
  echo "[STEP 14] root cause"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/root_cause_and_contribution_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_ACTION_ENGINE" == "true" ]]; then
  echo "[STEP 15] action engine"
  if [[ -f "$PROJECT_ROOT/pipelines/action_engine_runner_v2.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/action_engine_runner_v2.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  elif [[ -f "$PROJECT_ROOT/pipelines/action_engine_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/action_engine_runner.py" --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  else
    echo "[WARN] action engine runner not found, skip"
  fi
fi

echo "[DONE] pre-ML backfill final completed"
