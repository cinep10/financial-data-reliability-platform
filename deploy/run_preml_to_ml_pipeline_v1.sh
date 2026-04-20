#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_NAME="${DB_NAME:-weblog}"

PROFILE_ID="${1:?profile_id required}"
DT_FROM="${2:?dt_from required}"
DT_TO="${3:?dt_to required}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

VIEW_SQL="${PROJECT_ROOT}/sql/stream_ml_feature_view_v4.sql"
TRUTH_SQL="${PROJECT_ROOT}/sql/stream_truth_and_ml_v2.sql"

CLASS_MODEL_PATH="${PROJECT_ROOT}/artifacts/ml_v2/stream_multiclass_model_v2.joblib"
RISK_MODEL_PATH="${PROJECT_ROOT}/artifacts/ml_v2/stream_risk_regressor_v1.joblib"

echo "[INFO] run_preml_to_ml_pipeline_v1"
echo "[INFO] profile_id=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO}"
echo "[INFO] db=${DB_NAME} host=${DB_HOST}:${DB_PORT}"

run_mysql_file() {
  local sql_file="$1"
  echo "[INFO] mysql < ${sql_file}"
  mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" < "${sql_file}"
}

run_python() {
  echo "[INFO] $*"
  "$@"
}

echo "============================================================"
echo "[STEP 1] ML feature view 생성"
echo "============================================================"
run_mysql_file "${VIEW_SQL}"

echo "============================================================"
echo "[STEP 2] 자동 튜닝"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/stream/auto_stream_risk_threshold_tuner_v3.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}"

echo "============================================================"
echo "[STEP 3] risk signal 재생성"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/stream/stream_risk_signal_builder_auto_v3.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}"

echo "============================================================"
echo "[STEP 4] truth v2 생성"
echo "============================================================"
run_mysql_file "${TRUTH_SQL}"

echo "============================================================"
echo "[STEP 5] classification 학습"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ml/stream/train_stream_multiclass_model_v2.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --view-name "vw_stream_ml_training_dataset_v4"

echo "============================================================"
echo "[STEP 6] regression 학습"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ml/stream/train_stream_risk_regressor_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --view-name "vw_stream_ml_training_dataset_v4"

echo "============================================================"
echo "[STEP 7] classification prediction 저장"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ml/stream/predict_stream_multiclass_to_db_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --model-path "${CLASS_MODEL_PATH}" \
  --view-name "vw_stream_ml_training_dataset_v4"

echo "============================================================"
echo "[STEP 8] regression prediction 저장"
echo "============================================================"
run_python "${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ml/stream/predict_stream_risk_to_db_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --model-path "${RISK_MODEL_PATH}" \
  --view-name "vw_stream_ml_training_dataset_v4"

echo "============================================================"
echo "[DONE] pre-ml -> ml pipeline completed"
echo "============================================================"
echo "[CHECK] classification table: stream_ml_prediction_day"
echo "[CHECK] regression table    : stream_ml_risk_prediction_day"
echo "[CHECK] model artifacts     : artifacts/ml_v2"

