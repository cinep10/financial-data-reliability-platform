#!/usr/bin/env bash
set -euo pipefail

START_DATE="${1:-2026-02-23}"
END_DATE="${2:-2026-03-09}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

BASELINE_DAYS="${BASELINE_DAYS:-28}"
MODEL_NAME="${MODEL_NAME:-logistic_risk_classifier}"
MODEL_VERSION="${MODEL_VERSION:-v1}"
MODEL_OUT="${MODEL_OUT:-ml/ml_risk_model.joblib}"
REPORT_OUT="${REPORT_OUT:-ml/ml_risk_model_report.json}"
IMPORTANCE_OUT="${IMPORTANCE_OUT:-ml/ml_feature_importance.csv}"

RUN_ML_DRIFT="${RUN_ML_DRIFT:-true}"
RUN_RISK_V3="${RUN_RISK_V3:-true}"
RUN_FEATURE_VECTOR="${RUN_FEATURE_VECTOR:-true}"
RUN_MODEL_TRAIN="${RUN_MODEL_TRAIN:-true}"
RUN_IMPORTANCE_LOAD="${RUN_IMPORTANCE_LOAD:-true}"
RUN_PREDICTION="${RUN_PREDICTION:-true}"

export DB_BACKEND="${DB_BACKEND:-mysql}"
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

echo "[INFO] ML BACKFILL START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"

if [[ "$RUN_ML_DRIFT" == "true" ]]; then
  echo "[STEP 1] ml feature drift backfill"
  python3 ml/ml_feature_drift_analyzer.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE"     --dt-to "$END_DATE"     --baseline-days "$BASELINE_DAYS"     --truncate
fi

if [[ "$RUN_RISK_V3" == "true" ]]; then
  echo "[STEP 2] risk score v3 backfill"
  python3 ml/risk_score_runner_v3.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE"     --dt-to "$END_DATE"     --truncate
fi

if [[ "$RUN_FEATURE_VECTOR" == "true" ]]; then
  echo "[STEP 3] feature vector backfill"
  python3 ml/ml_feature_vector_builder.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE"     --dt-to "$END_DATE"     --truncate
fi

if [[ "$RUN_MODEL_TRAIN" == "true" ]]; then
  echo "[STEP 4] model training"
  python3 ml/ml_risk_model_train_fixed.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE"     --dt-to "$END_DATE"     --model-out "$MODEL_OUT"     --report-out "$REPORT_OUT"     --importance-out "$IMPORTANCE_OUT"
fi

if [[ "$RUN_IMPORTANCE_LOAD" == "true" ]]; then
  echo "[STEP 5] feature importance load"
  python3 ml/ml_feature_importance_loader.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --date "$END_DATE"     --csv "$IMPORTANCE_OUT"     --model-name "$MODEL_NAME"     --model-version "$MODEL_VERSION"     --truncate
fi

if [[ "$RUN_PREDICTION" == "true" ]]; then
  echo "[STEP 6] prediction backfill"
  python3 ml/ml_prediction_runner.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE"     --dt-to "$END_DATE"     --model-path "$MODEL_OUT"     --model-name "$MODEL_NAME"     --model-version "$MODEL_VERSION"     --truncate
fi

echo "[DONE] ML backfill pipeline completed"
