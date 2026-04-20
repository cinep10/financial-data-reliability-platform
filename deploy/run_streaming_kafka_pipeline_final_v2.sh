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

echo "[INFO] run_streaming_kafka_pipeline_final_v2"
echo "[INFO] profile=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO} topic=${TOPIC}"

run_py() { python3 "$@"; }

echo "----------------------------------"
echo "[1] PRODUCER"
echo "----------------------------------"
run_py pipelines/stream/kafka_producer_from_event_log_raw_v3.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --dt-from "$DT_FROM" --dt-to "$DT_TO"   --topic "$TOPIC" --kafka-bootstrap "$KAFKA_BOOTSTRAP"

echo "----------------------------------"
echo "[2] CONSUMER"
echo "----------------------------------"
run_py pipelines/stream/kafka_consumer_to_stg_event_stream_v4.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --kafka-bootstrap "$KAFKA_BOOTSTRAP"   --topic "$TOPIC" --consumer-group "$CONSUMER_GROUP"   --truncate-target-for-date "$DT_FROM"   --max-messages "$MAX_MESSAGES" --idle-timeout-sec "$IDLE_TIMEOUT_SEC"

echo "----------------------------------"
echo "[3] STREAM METRIC"
echo "----------------------------------"
run_py pipelines/stream/stream_completeness_runner_v3.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_duplicate_runner_v4.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_ordering_runner_v3.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_latency_runner_v3.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

echo "----------------------------------"
echo "[4] AGG / RISK"
echo "----------------------------------"
run_py pipelines/stream/stream_reliability_aggregator_v5.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_risk_signal_builder_v5.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_to_risk_bridge.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_to_root_cause_bridge_v4.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"
run_py pipelines/stream/stream_to_action_bridge.py   --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME"   --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

echo "[DONE] streaming pipeline completed"
