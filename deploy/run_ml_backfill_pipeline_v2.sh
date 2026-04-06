#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

START_DATE="${1:-2026-01-01}"
END_DATE="${2:-2026-03-31}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

MODEL_NAME="${MODEL_NAME:-ml_risk_model}"
MODEL_VERSION="${MODEL_VERSION:-ml_risk_safe_v6}"
MODEL_OUT="${MODEL_OUT:-ml_risk_model_safe.joblib}"
REPORT_OUT="${REPORT_OUT:-ml_risk_model_report_safe.json}"
IMPORTANCE_OUT="${IMPORTANCE_OUT:-ml_feature_importance_safe.csv}"

RUN_FEATURE_VECTOR="${RUN_FEATURE_VECTOR:-true}"
RUN_MODEL_TRAIN="${RUN_MODEL_TRAIN:-true}"
RUN_PREDICTION="${RUN_PREDICTION:-true}"
RUN_IMPORTANCE_LOAD="${RUN_IMPORTANCE_LOAD:-true}"
RUN_ML_DRIFT="${RUN_ML_DRIFT:-true}"
TRAIN_ALL_HISTORY="${TRAIN_ALL_HISTORY:-true}"
TRAIN_MODE="${TRAIN_MODE:-curated_history}"

echo "[INFO] ML BACKFILL V5 START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID TRAIN_ALL_HISTORY=$TRAIN_ALL_HISTORY TRAIN_MODE=$TRAIN_MODE"

if [[ "$RUN_FEATURE_VECTOR" == "true" ]]; then
  echo "[STEP 1] feature vector"
  "$PYTHON_BIN" "$PROJECT_ROOT/ml/ml_feature_vector_builder.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" --dt-to "$END_DATE" \
    --truncate
fi

if [[ "$RUN_MODEL_TRAIN" == "true" ]]; then
  echo "[STEP 2] model train"
  TRAIN_ALL_HISTORY_FLAG=()
  if [[ "$TRAIN_ALL_HISTORY" == "true" ]]; then
    TRAIN_ALL_HISTORY_FLAG+=(--train-all-history)
  fi

  "$PYTHON_BIN" "$PROJECT_ROOT/ml/ml_risk_model_train.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" --dt-to "$END_DATE" \
    --model-path "$MODEL_OUT" \
    --report-path "$REPORT_OUT" \
    --importance-csv "$IMPORTANCE_OUT" \
    --model-version "$MODEL_VERSION" \
    --train-mode "$TRAIN_MODE" \
    "${TRAIN_ALL_HISTORY_FLAG[@]}"
fi

if [[ "$RUN_PREDICTION" == "true" ]]; then
  echo "[STEP 3] prediction"
  "$PYTHON_BIN" "$PROJECT_ROOT/ml/ml_prediction_runner.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" --dt-to "$END_DATE" \
    --model-path "$MODEL_OUT" \
    --model-version "$MODEL_VERSION" \
    --truncate
fi

if [[ "$RUN_IMPORTANCE_LOAD" == "true" ]]; then
  echo "[STEP 4] importance load"
  "$PYTHON_BIN" "$PROJECT_ROOT/ml/ml_feature_importance_loader.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --date "$END_DATE" \
    --csv "$IMPORTANCE_OUT" \
    --model-name "$MODEL_NAME" \
    --model-version "$MODEL_VERSION" \
    --truncate
fi

if [[ "$RUN_ML_DRIFT" == "true" ]]; then
  echo "[STEP 5] ML feature drift"
  "$PYTHON_BIN" "$PROJECT_ROOT/ml/ml_feature_drift_analyzer.py" \
    --host "$DB_HOST" --port "$DB_PORT" \
    --user "$DB_USER" --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --profile-id "$PROFILE_ID" \
    --dt-from "$START_DATE" --dt-to "$END_DATE" \
    --truncate
fi

echo "[DONE] ML backfill pipeline v5 completed"
