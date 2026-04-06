#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE_YAML="${PROFILE_YAML:-$PROJECT_ROOT/configs/profiles/finance_bank.yaml}"

START_DATE="${1:-2026-02-23}"
END_DATE="${2:-2026-03-09}"
PROFILE_ID="${3:-finance_bank}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

BASE_URL="${BASE_URL:-https://www.finance-bank.example.com}"
#PROFILE_YAML="${PROFILE_YAML:-configs/profiles/finance_bank.yaml}"
LOG_OUT="${LOG_OUT:-/home/dwkim_nethru/log/logdata/finance/finance_bank_base.log}"
TSV_OUT="${TSV_OUT:-/home/dwkim_nethru/log/logdata/finance/finance_bank_base.tsv}"

BASELINE_DAYS="${BASELINE_DAYS:-28}"
MODEL_NAME="${MODEL_NAME:-logistic_risk_classifier}"
MODEL_VERSION="${MODEL_VERSION:-v1}"
MODEL_OUT="${MODEL_OUT:-ml/ml_risk_model.joblib}"
REPORT_OUT="${REPORT_OUT:-ml/ml_risk_model_report.json}"
IMPORTANCE_OUT="${IMPORTANCE_OUT:-ml/ml_feature_importance.csv}"
WRITE_LEGACY="${WRITE_LEGACY:-true}"

RUN_PRE_ML="${RUN_PRE_ML:-true}"
RUN_ML="${RUN_ML:-true}"

export DB_BACKEND="${DB_BACKEND:-mysql}"
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD
export BASE_URL PROFILE_YAML LOG_OUT TSV_OUT
export BASELINE_DAYS MODEL_NAME MODEL_VERSION MODEL_OUT REPORT_OUT IMPORTANCE_OUT
export WRITE_LEGACY

echo "[INFO] FULL BACKFILL START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"

if [[ "$RUN_PRE_ML" == "true" ]]; then
  echo "[PHASE A] pre-ML pipeline"
  bash ./run_pre_ml_backfill_pipeline.sh "$START_DATE" "$END_DATE" "$PROFILE_ID"
fi

if [[ "$RUN_ML" == "true" ]]; then
  echo "[PHASE B] ML pipeline"
  bash ./run_ml_backfill_pipeline.sh "$START_DATE" "$END_DATE" "$PROFILE_ID"
fi

echo "[DONE] full end-to-end backfill pipeline completed"
