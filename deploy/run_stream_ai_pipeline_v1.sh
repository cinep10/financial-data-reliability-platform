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

echo "[INFO] run_stream_ai_pipeline_v1"
echo "[INFO] profile_id=${PROFILE_ID} dt_from=${DT_FROM} dt_to=${DT_TO}"

"${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ai/stream/build_stream_ai_context_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}"

"${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ai/stream/llm_stream_incident_reasoner_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --force-fallback

"${PYTHON_BIN}" "${PROJECT_ROOT}/pipelines/ai/stream/ai_stream_action_recommender_v1.py" \
  --host "${DB_HOST}" \
  --port "${DB_PORT}" \
  --user "${DB_USER}" \
  --password "${DB_PASSWORD}" \
  --db "${DB_NAME}" \
  --profile-id "${PROFILE_ID}" \
  --dt-from "${DT_FROM}" \
  --dt-to "${DT_TO}" \
  --force-fallback

echo "[DONE] stream ai pipeline completed"
