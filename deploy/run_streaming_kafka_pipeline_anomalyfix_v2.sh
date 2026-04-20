#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASS="${DB_PASS:-${DB_PASSWORD:-nethru1234}}"
DB_NAME="${DB_NAME:-weblog}"

PROFILE_ID="${1:?profile_id required}"
DT_FROM="${2:?dt_from required}"
DT_TO="${3:?dt_to required}"

echo "[INFO] anomaly-fix post-processing v2 profile=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO}"

python3 pipelines/stream/stream_duplicate_runner_v4.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_ordering_runner_v3.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_latency_runner_v3.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_completeness_runner_v3.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_reliability_aggregator_v5.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_risk_signal_builder_v5.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_to_risk_bridge.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_to_root_cause_bridge_v4.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

python3 pipelines/stream/stream_to_action_bridge.py \
  --db-host "$DB_HOST" --db-port "$DB_PORT" --db-user "$DB_USER" --db-pass "$DB_PASS" --db-name "$DB_NAME" \
  --profile-id "$PROFILE_ID" --dt-from "$DT_FROM" --dt-to "$DT_TO"

echo "[DONE] anomaly-fix post-processing completed"
