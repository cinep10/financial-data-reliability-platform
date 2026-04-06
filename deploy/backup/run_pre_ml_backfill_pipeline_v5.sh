#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

START_DATE="${1:-2026-02-23}"
END_DATE="${2:-2026-03-09}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

RUN_VALIDATION="${RUN_VALIDATION:-true}"
RUN_DRIFT="${RUN_DRIFT:-true}"
RUN_RISK_V3="${RUN_RISK_V3:-true}"
RUN_ROOT_CAUSE="${RUN_ROOT_CAUSE:-true}"
RUN_ACTION_ENGINE="${RUN_ACTION_ENGINE:-true}"
RUN_TIME_ANOMALY="${RUN_TIME_ANOMALY:-true}"
RUN_CORR_ANOMALY="${RUN_CORR_ANOMALY:-true}"
RUN_SCENARIO_SUMMARY="${RUN_SCENARIO_SUMMARY:-false}"

echo "[INFO] PRE-ML BACKFILL V5 START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"

if [[ "$RUN_VALIDATION" == "true" ]]; then
  echo "[STEP 1] validation"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/validation_layer_runner_v2.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_DRIFT" == "true" ]]; then
  echo "[STEP 2] drift"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    DB_BACKEND=mysql DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
      Rscript "$PROJECT_ROOT/r/metric_drift_analysis_db_v8.R" \
      --date "$CURRENT" --profile-id "$PROFILE_ID"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

if [[ "$RUN_RISK_V3" == "true" ]]; then
  echo "[STEP 3] risk v3"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_day_v3_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_ROOT_CAUSE" == "true" ]]; then
  echo "[STEP 4] root cause"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/root_cause_and_contribution_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_ACTION_ENGINE" == "true" ]]; then
  echo "[STEP 5] action engine"
  if [[ -f "$PROJECT_ROOT/pipelines/action_engine_runner_v2.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/action_engine_runner_v2.py" \
      --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
      --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  elif [[ -f "$PROJECT_ROOT/pipelines/action_engine_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/action_engine_runner.py" \
      --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
      --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
  else
    echo "[WARN] action engine runner not found, skip"
  fi
fi

if [[ "$RUN_TIME_ANOMALY" == "true" ]]; then
  echo "[STEP 6] time anomaly"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_CORR_ANOMALY" == "true" ]]; then
  echo "[STEP 7] correlation anomaly"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" --profile-id "$PROFILE_ID" --dt-from "$START_DATE" --dt-to "$END_DATE" --truncate
fi

if [[ "$RUN_SCENARIO_SUMMARY" == "true" ]]; then
  echo "[STEP 8] scenario summary"
  echo "[WARN] use scenario_experiment_runner.py directly with --scenario-run-id after scenario injection"
fi

echo "[DONE] pre-ML backfill pipeline v5 completed"
