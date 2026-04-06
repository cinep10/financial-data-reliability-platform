#!/usr/bin/env bash
set -euo pipefail

PROFILE_ID="${1:-finance_bank}"
DT_FROM="${2:-2026-02-23}"
DT_TO="${3:-2026-03-09}"

HOST="${DB_HOST:-127.0.0.1}"
PORT="${DB_PORT:-3306}"
USER="${DB_USER:-nethru}"
PASSWORD="${DB_PASSWORD:-nethru1234}"
DB="${DB_NAME:-weblog}"

python3 ml/ml_feature_vector_builder.py --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" --db "$DB" --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" --truncate
python3 ml/ml_risk_model_train.py --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" --db "$DB" --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
python3 ml/ml_prediction_runner.py --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" --db "$DB" --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" --model-path ml_risk_model_safe.joblib --model-version ml_risk_safe_v1 --truncate
python3 ml/ml_feature_importance_loader.py --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" --db "$DB" --profile-id "$PROFILE_ID" --date "$DT_TO" --csv ml_feature_importance_safe.csv --model-version ml_risk_safe_v1 --truncate
python3 ml/ml_feature_drift_analyzer.py --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" --db "$DB" --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" --truncate

echo "[DONE] safe ML pipeline completed"
