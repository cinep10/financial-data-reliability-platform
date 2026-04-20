#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-drop_and_recreate}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-}"
DB_NAME="${DB_NAME:-weblog}"

run_sql() {
  mysql -h "${DB_HOST}" -P "${DB_PORT}" -u "${DB_USER}" -p"${DB_PASSWORD}" "${DB_NAME}" -e "$1"
}

echo "[INFO] reset_stream_ai_tables_v1 mode=${MODE}"

case "${MODE}" in
  ai_only)
    run_sql "
      DELETE FROM ai_recommended_action_day;
      DELETE FROM ai_incident_summary_day;
      DELETE FROM ai_stream_incident_context_day;
    "
    echo "[OK] reset mode: ai_only"
    ;;

  drop_and_recreate)
    run_sql "
      DROP TABLE IF EXISTS ai_recommended_action_day;
      DROP TABLE IF EXISTS ai_incident_summary_day;
      DROP TABLE IF EXISTS ai_stream_incident_context_day;
    "
    echo "[OK] reset mode: drop_and_recreate"
    ;;

  full_results)
    run_sql "
      DROP TABLE IF EXISTS ai_recommended_action_day;
      DROP TABLE IF EXISTS ai_incident_summary_day;
      DROP TABLE IF EXISTS ai_stream_incident_context_day;
      DELETE FROM stream_ml_prediction_day;
      DELETE FROM stream_ml_risk_prediction_day;
    "
    echo "[OK] reset mode: full_results"
    ;;

  *)
    echo "[ERROR] unknown mode: ${MODE}"
    echo "usage: bash deploy/reset_stream_ai_tables_v1.sh [ai_only|drop_and_recreate|full_results]"
    exit 1
    ;;
esac
