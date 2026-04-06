#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

SCENARIO_NAME="${1:?scenario_name required}"
DT_FROM="${2:?start_date required}"
DT_TO="${3:?end_date required}"
PROFILE_ID="${4:-finance_bank}"
INTENSITY="${5:-medium}"

DB_BACKEND="${DB_BACKEND:-mysql}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

export DB_BACKEND DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME

echo "[INFO] scenario test v2 DB_HOST=$DB_HOST DB_PORT=$DB_PORT DB_NAME=$DB_NAME DB_USER=$DB_USER"
echo "[INFO] scenario=$SCENARIO_NAME intensity=$INTENSITY start=$DT_FROM end=$DT_TO profile=$PROFILE_ID"

echo "[STEP 0] inject scenario"
"$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario_injector.py" \
  --host "$DB_HOST" --port "$DB_PORT" \
  --user "$DB_USER" --password "$DB_PASSWORD" \
  --db "$DB_NAME" \
  --profile-id "$PROFILE_ID" \
  --scenario-name "$SCENARIO_NAME" \
  --intensity "$INTENSITY" \
  --dt-from "$DT_FROM" --dt-to "$DT_TO"

echo "[STEP 1] rerun pre-ML on impacted period"
"$PYTHON_BIN" "$PROJECT_ROOT/pipelines/validation_layer_runner_v2.py" \
  --host "$DB_HOST" --port "$DB_PORT" \
  --user "$DB_USER" --password "$DB_PASSWORD" \
  --db "$DB_NAME" \
  --profile-id "$PROFILE_ID" \
  --dt-from "$DT_FROM" --dt-to "$DT_TO" \
  --truncate

CURRENT="$DT_FROM"
while [[ "$CURRENT" < "$DT_TO" || "$CURRENT" == "$DT_TO" ]]; do
  echo "[STEP 1-DRIFT] date=$CURRENT"

  Rscript "$PROJECT_ROOT/r/metric_drift_analysis_db_v8.R" \
    --date "$CURRENT" \
    --profile-id "$PROFILE_ID"

  if [[ -f "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/time_pattern_anomaly_runner.py" \
      --host "$DB_HOST" --port "$DB_PORT" \
      --user "$DB_USER" --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --date "$CURRENT"
  fi

  if [[ -f "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" ]]; then
    "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/correlation_anomaly_runner.py" \
      --host "$DB_HOST" --port "$DB_PORT" \
      --user "$DB_USER" --password "$DB_PASSWORD" \
      --db "$DB_NAME" \
      --profile-id "$PROFILE_ID" \
      --date "$CURRENT"
  fi

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/risk_score_day_v4_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$CURRENT" --dt-to "$CURRENT" \
    --truncate

  CURRENT=$(date -I -d "$CURRENT + 1 day")
done

"$PYTHON_BIN" "$PROJECT_ROOT/pipelines/root_cause_and_contribution_runner.py" \
  --host "$DB_HOST" --port "$DB_PORT" \
  --user "$DB_USER" --password "$DB_PASSWORD" \
  --db "$DB_NAME" \
  --profile-id "$PROFILE_ID" \
  --dt-from "$DT_FROM" --dt-to "$DT_TO" \
  --truncate

echo "[STEP 2] rerun ML layer"
bash "$PROJECT_ROOT/deploy/run_ml_backfill_pipeline_v2.sh" "$DT_FROM" "$DT_TO" "$PROFILE_ID"

echo "[STEP 3] summarize scenario result"
SCENARIO_RUN_ID=$(mysql -N \
  -h "$DB_HOST" -P "$DB_PORT" \
  -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
  -e "SELECT MAX(scenario_run_id) FROM scenario_experiment_run WHERE profile_id='$PROFILE_ID' AND scenario_name='$SCENARIO_NAME';")

"$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario_experiment_runner.py" \
  --host "$DB_HOST" --port "$DB_PORT" \
  --user "$DB_USER" --password "$DB_PASSWORD" \
  --db "$DB_NAME" \
  --profile-id "$PROFILE_ID" \
  --scenario-run-id "$SCENARIO_RUN_ID" \
  --dt-from "$DT_FROM" --dt-to "$DT_TO" \
  --truncate

echo "[DONE] scenario test v2 completed: scenario_name=$SCENARIO_NAME intensity=$INTENSITY run_id=$SCENARIO_RUN_ID"
