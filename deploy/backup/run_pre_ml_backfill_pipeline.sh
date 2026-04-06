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
LOG_OUT="${LOG_OUT:-/mnt/d/etl_storage/log/logdata/finance/finance_bank_base.log}"
TSV_OUT="${TSV_OUT:-/mnt/d/etl_storage/log/logdata/finance/finance_bank_base.tsv}"

WRITE_LEGACY="${WRITE_LEGACY:-true}"
RUN_SIMULATION="${RUN_SIMULATION:-true}"
RUN_PARSE_LOAD="${RUN_PARSE_LOAD:-true}"
RUN_COLLECTOR="${RUN_COLLECTOR:-true}"
RUN_ANALYZER="${RUN_ANALYZER:-true}"
RUN_VALIDATION="${RUN_VALIDATION:-true}"
RUN_DRIFT="${RUN_DRIFT:-true}"
RUN_RISK_V2="${RUN_RISK_V2:-true}"

export DB_BACKEND="${DB_BACKEND:-mysql}"
export DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD

echo "[INFO] PRE-ML BACKFILL START_DATE=$START_DATE END_DATE=$END_DATE PROFILE_ID=$PROFILE_ID"

if [[ "$RUN_SIMULATION" == "true" ]]; then
  echo "[STEP 1] weblog simulation"
  weblog-sim     --profile "$PROFILE_YAML"     --start "${START_DATE}T00:00:00"     --end "${END_DATE}T23:59:59"     --avg-rps 1     --seed 42     --out "$LOG_OUT"
fi

if [[ "$RUN_PARSE_LOAD" == "true" ]]; then
  echo "[STEP 2] parse + load raw log"
  python3 pipelines/parse_webserver_log.py     --base-url "$BASE_URL"     "$LOG_OUT"     "$TSV_OUT"

  python3 pipelines/load_tsv_to_db_v2.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD"     --db "$DB_NAME"     --table stg_webserver_log_hit     --tsv "$TSV_OUT"     --columns "dt,ts,ip,method,url_raw,url_full,url_norm,host,path,query,status,bytes,latency_ms,ref,ref_host,ua,kv_raw,uid,pcid,sid,device_type,evt,accept_lang,cc,page_type"     --truncate-target
fi

if [[ "$RUN_COLLECTOR" == "true" ]]; then
  echo "[STEP 3] collector"
  python3 pipelines/collector_a_v2.py     --db-host "$DB_HOST" --db-port "$DB_PORT"     --db-user "$DB_USER" --db-pass "$DB_PASSWORD" --db-name "$DB_NAME"     --dt-from "$START_DATE" --dt-to "$END_DATE"     --base-url "$BASE_URL"     --seed 42     --truncate-target
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
  python3 pipelines/analyzer_b_v4.py "${ANALYZER_ARGS[@]}"
fi

if [[ "$RUN_VALIDATION" == "true" ]]; then
  echo "[STEP 5] validation"
  python3 pipelines/validation_layer_runner_v2.py     --host "$DB_HOST" --port "$DB_PORT"     --user "$DB_USER" --password "$DB_PASSWORD" --db "$DB_NAME"     --profile-id "$PROFILE_ID"     --dt-from "$START_DATE" --dt-to "$END_DATE"     --truncate
fi

if [[ "$RUN_DRIFT" == "true" ]]; then
  echo "[STEP 6] drift v1/v2 backfill"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    echo "  -> drift date=$CURRENT"
    Rscript r/metric_drift_analysis_db_v7.R --date "$CURRENT" --profile-id "$PROFILE_ID"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

if [[ "$RUN_RISK_V2" == "true" ]]; then
  echo "[STEP 7] risk score v2 backfill"
  CURRENT="$START_DATE"
  while [[ "$CURRENT" < "$END_DATE" || "$CURRENT" == "$END_DATE" ]]; do
    echo "  -> risk v2 date=$CURRENT"
    python3 pipelines/risk_score_runner_v2.py       --host "$DB_HOST" --port "$DB_PORT"       --user "$DB_USER" --password "$DB_PASSWORD"       --db "$DB_NAME" --profile-id "$PROFILE_ID" --date "$CURRENT"
    CURRENT=$(date -I -d "$CURRENT + 1 day")
  done
fi

echo "[DONE] pre-ML backfill pipeline completed"
