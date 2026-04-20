#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

SCENARIO_NAME="${1:?scenario name required}"
START_DATE="${2:?start date required}"
END_DATE="${3:?end date required}"
PROFILE_ID="${4:?profile id required}"
INTENSITY="${5:-medium}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:?DB_USER required}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_NAME="${DB_NAME:?DB_NAME required}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
BASE_URL="${BASE_URL:-https://www.finance-bank.example.com}"
PROFILE_YAML="${PROFILE_YAML:-$PROJECT_ROOT/configs/profiles/${PROFILE_ID}.yaml}"

LOG_DIR="${LOG_DIR:-/mnt/d/etl_storage/log/logdata/finance}"
STAGE_DIR="${STAGE_DIR:-$LOG_DIR/staging}"
mkdir -p "$LOG_DIR" "$STAGE_DIR"

RUN_RESET="${RUN_RESET:-false}"
RUN_SOURCE_SIMULATION="${RUN_SOURCE_SIMULATION:-true}"
RUN_SOURCE_SAFE_PARSE_LOAD="${RUN_SOURCE_SAFE_PARSE_LOAD:-true}"
RUN_SCENARIO_PLAN="${RUN_SCENARIO_PLAN:-true}"
RUN_STREAM_ADAPTER="${RUN_STREAM_ADAPTER:-true}"
RUN_BATCH_ADAPTER="${RUN_BATCH_ADAPTER:-true}"
RUN_PRE_ML="${RUN_PRE_ML:-true}"
RUN_STREAMING_PIPELINE="${RUN_STREAMING_PIPELINE:-true}"
RUN_ML="${RUN_ML:-false}"
RUN_AI="${RUN_AI:-false}"

USE_SCENARIO_STREAM_INJECTION="${USE_SCENARIO_STREAM_INJECTION:-true}"
RUN_STREAM_POST_PROCESS="${RUN_STREAM_POST_PROCESS:-true}"

RUN_TEST_RESET_MODE="${RUN_TEST_RESET_MODE:-full}"
RUN_STREAM_RESET_MODE="${RUN_STREAM_RESET_MODE:-stream_full}"
RUN_AI_RESET_MODE="${RUN_AI_RESET_MODE:-ai_only}"

SOURCE_LOG_MODE="${SOURCE_LOG_MODE:-dated_file}"
SOURCE_LOG_PATH="${SOURCE_LOG_PATH:-}"
DATE_SCOPED_LOG="$LOG_DIR/${PROFILE_ID}_${START_DATE}_${END_DATE}.log"
BASE_LOG="$LOG_DIR/${PROFILE_ID}_base.log"

if [[ -z "$SOURCE_LOG_PATH" ]]; then
  if [[ "$SOURCE_LOG_MODE" == "dated_file" ]]; then
    SOURCE_LOG_PATH="$DATE_SCOPED_LOG"
  else
    SOURCE_LOG_PATH="$BASE_LOG"
  fi
fi

announce() {
  echo "=================================================="
  echo "$1"
  echo "=================================================="
}

run_simulator() {
  announce "[1] SOURCE LOG GENERATION"
  "$PYTHON_BIN" "$PROJECT_ROOT/simulator/weblog_sim/cli.py" \
    --profile "$PROFILE_YAML" \
    --start "${START_DATE}T00:00:00" \
    --end "${END_DATE}T23:59:59" \
    --avg-rps "${AVG_RPS:-1}" \
    --seed "${SIM_SEED:-42}" \
    --out "$SOURCE_LOG_PATH"
}

run_source_safe_parse_load() {
  announce "[2] SOURCE-SAFE PARSE/LOAD"
  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/ingest/parse_webserver_log_range_safe.py" \
    --base-url "$BASE_URL" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --dedup \
    "$SOURCE_LOG_PATH" "$STAGE_DIR/${PROFILE_ID}_${START_DATE}_${END_DATE}.tsv"

  "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/ingest/load_tsv_to_db_range_safe.py" \
    --host "$DB_HOST" \
    --port "$DB_PORT" \
    --user "$DB_USER" \
    --password "$DB_PASSWORD" \
    --db "$DB_NAME" \
    --table stg_webserver_log_hit \
    --tsv "$STAGE_DIR/${PROFILE_ID}_${START_DATE}_${END_DATE}.tsv" \
    --dt-from "$START_DATE" \
    --dt-to "$END_DATE" \
    --delete-date-range
}

run_unified_scenario_steps() {
  announce "[3] SCENARIO / EXOGENOUS"

  if [[ "$RUN_SCENARIO_PLAN" == "true" ]]; then
    if [[ -f "$PROJECT_ROOT/pipelines/scenario/unified_scenario_run_registry.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario/unified_scenario_run_registry.py" \
        --db-host "$DB_HOST" --db-port "$DB_PORT" \
        --db-user "$DB_USER" --db-pass "$DB_PASSWORD" \
        --db-name "$DB_NAME" \
        --profile-id "$PROFILE_ID" \
        --scenario-name "$SCENARIO_NAME" \
        --scenario-intensity "$INTENSITY" \
        --dt-from "$START_DATE" --dt-to "$END_DATE"
    fi

    if [[ -f "$PROJECT_ROOT/core/exogenous/exogenous_state_builder.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/core/exogenous/exogenous_state_builder.py" \
        --db-host "$DB_HOST" --db-port "$DB_PORT" \
        --db-user "$DB_USER" --db-pass "$DB_PASSWORD" \
        --db-name "$DB_NAME" \
        --profile-id "$PROFILE_ID" \
        --dt-from "$START_DATE" --dt-to "$END_DATE"
    fi

    if [[ -f "$PROJECT_ROOT/pipelines/scenario/source_generator_adapter.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario/source_generator_adapter.py" \
        --db-host "$DB_HOST" --db-port "$DB_PORT" \
        --db-user "$DB_USER" --db-pass "$DB_PASSWORD" \
        --db-name "$DB_NAME" \
        --profile-id "$PROFILE_ID" \
        --dt-from "$START_DATE" --dt-to "$END_DATE"
    fi
  fi

  if [[ "$RUN_STREAM_ADAPTER" == "true" ]]; then
    if [[ -f "$PROJECT_ROOT/pipelines/scenario/stream_injection_adapter_v5.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario/stream_injection_adapter_v5.py" \
        --db-host "$DB_HOST" --db-port "$DB_PORT" \
        --db-user "$DB_USER" --db-pass "$DB_PASSWORD" \
        --db-name "$DB_NAME" \
        --profile-id "$PROFILE_ID" \
        --dt-from "$START_DATE" --dt-to "$END_DATE"
    fi
  fi

  if [[ "$RUN_BATCH_ADAPTER" == "true" ]]; then
    if [[ -f "$PROJECT_ROOT/pipelines/scenario/batch_metric_adapter_v2.py" ]]; then
      "$PYTHON_BIN" "$PROJECT_ROOT/pipelines/scenario/batch_metric_adapter_v2.py" \
        --db-host "$DB_HOST" --db-port "$DB_PORT" \
        --db-user "$DB_USER" --db-pass "$DB_PASSWORD" \
        --db-name "$DB_NAME" \
        --profile-id "$PROFILE_ID" \
        --dt-from "$START_DATE" --dt-to "$END_DATE"
    fi
  fi
}

run_pre_ml() {
  announce "[4] PRE-ML"
  RUN_SIMULATION=false \
  SOURCE_LOG_PATH="$SOURCE_LOG_PATH" \
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
  bash "$PROJECT_ROOT/deploy/run_pre_ml_backfill_pipeline_final.sh" "$START_DATE" "$END_DATE" "$PROFILE_ID"
}

run_streaming() {
  announce "[5] STREAMING"

  if [[ "$USE_SCENARIO_STREAM_INJECTION" == "true" ]]; then
    DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
    RUN_POST_PROCESS="$RUN_STREAM_POST_PROCESS" \
    bash "$SCRIPT_DIR/run_finance_stream_scenario_injection_v3.sh" "$PROFILE_ID" "$START_DATE" "$END_DATE"
  else
    DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASS="$DB_PASSWORD" DB_NAME="$DB_NAME" KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
    bash "$SCRIPT_DIR/run_streaming_kafka_pipeline_final_v2.sh" "$PROFILE_ID" "$START_DATE" "$END_DATE"
  fi
}

run_ml() {
  announce "[6] ML"
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
  bash "$PROJECT_ROOT/deploy/run_preml_to_ml_pipeline_v1.sh" "$START_DATE" "$END_DATE" "$PROFILE_ID"
}

run_ai() {
  announce "[7] AI"
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
  RUN_RESET=false RESET_MODE="$RUN_AI_RESET_MODE" FORCE_FALLBACK="${FORCE_FALLBACK:-true}" \
  bash "$PROJECT_ROOT/deploy/run_ai_layer_pipeline_v2.sh" "$START_DATE" "$END_DATE" "$PROFILE_ID"
}

if [[ "$RUN_RESET" == "true" ]]; then
  announce "[0] RESET"
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
    bash "$PROJECT_ROOT/deploy/reset_test_tables_v2.sh" "$RUN_TEST_RESET_MODE"

  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASSWORD="$DB_PASSWORD" DB_NAME="$DB_NAME" \
    bash "$PROJECT_ROOT/deploy/reset_streaming_kafka_results.sh" "$RUN_STREAM_RESET_MODE"
fi

if [[ "$RUN_SOURCE_SIMULATION" == "true" ]]; then
  run_simulator
fi
if [[ "$RUN_SOURCE_SAFE_PARSE_LOAD" == "true" ]]; then
  run_source_safe_parse_load
fi

run_unified_scenario_steps

if [[ "$RUN_PRE_ML" == "true" ]]; then
  run_pre_ml
fi
if [[ "$RUN_STREAMING_PIPELINE" == "true" ]]; then
  run_streaming
fi
if [[ "$RUN_ML" == "true" ]]; then
  run_ml
fi
if [[ "$RUN_AI" == "true" ]]; then
  run_ai
fi

announce "[DONE]"
echo "[DONE] unified one-shot scenario pipeline completed scenario=$SCENARIO_NAME dt_from=$START_DATE dt_to=$END_DATE profile=$PROFILE_ID use_scenario_stream_injection=$USE_SCENARIO_STREAM_INJECTION"
