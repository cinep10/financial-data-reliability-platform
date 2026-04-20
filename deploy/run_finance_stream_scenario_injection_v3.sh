#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASS="${DB_PASS:-${DB_PASSWORD:-nethru1234}}"
DB_NAME="${DB_NAME:-weblog}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"

PROFILE_ID="${1:?profile_id required}"
DT_FROM="${2:?dt_from required}"
DT_TO="${3:?dt_to required}"
RUN_TS="${RUN_TS:-$(date +%Y%m%d%H%M%S)}"
TOPIC="${TOPIC:-stream.${PROFILE_ID}.${DT_FROM}.${RUN_TS}}"
CONSUMER_GROUP="${CONSUMER_GROUP:-reliability-${PROFILE_ID}-${DT_FROM}-${DT_TO}-${RUN_TS}}"
MAX_MESSAGES="${MAX_MESSAGES:-50000}"
IDLE_TIMEOUT_SEC="${IDLE_TIMEOUT_SEC:-10}"
RUN_POST_PROCESS="${RUN_POST_PROCESS:-true}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[INFO] finance stream scenario injection v3"
echo "[INFO] profile=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO} topic=${TOPIC}"

python3 pipelines/scenario/build_exogenous_timeline_from_registry_v3.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" --clear-range

python3 pipelines/scenario/stream_injection_adapter_v5.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" --clear-range

python3 pipelines/stream/kafka_producer_from_injection_queue_v1.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO" \
  --topic "$TOPIC" --kafka-bootstrap "$KAFKA_BOOTSTRAP"

python3 pipelines/stream/kafka_consumer_to_stg_event_stream_v4.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --kafka-bootstrap "$KAFKA_BOOTSTRAP" --topic "$TOPIC" --consumer-group "$CONSUMER_GROUP" \
  --truncate-target-for-date "$DT_FROM" --max-messages "$MAX_MESSAGES" --idle-timeout-sec "$IDLE_TIMEOUT_SEC"

if [[ "$RUN_POST_PROCESS" == "true" ]]; then
  DB_HOST="$DB_HOST" DB_PORT="$DB_PORT" DB_USER="$DB_USER" DB_PASS="$DB_PASS" DB_NAME="$DB_NAME" \
  bash "$SCRIPT_DIR/run_streaming_kafka_pipeline_anomalyfix_v2.sh" "$PROFILE_ID" "$DT_FROM" "$DT_TO"
fi

echo "[DONE] finance stream scenario injection completed"
