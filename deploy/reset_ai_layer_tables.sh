#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-ai_only}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

run_sql() {
  mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" -e "$1"
}

case "$MODE" in
  ai_only)
    run_sql "
      DELETE FROM ai_recommended_action_day;
      DELETE FROM ai_incident_summary_day;
      DELETE FROM ai_prompt_log;
    "
    echo "[OK] reset mode: ai_only"
    ;;
  ai_keep_summary)
    run_sql "
      DELETE FROM ai_recommended_action_day;
      DELETE FROM ai_prompt_log;
    "
    echo "[OK] reset mode: ai_keep_summary"
    ;;
  ai_and_ml)
    run_sql "
      DELETE FROM ai_recommended_action_day;
      DELETE FROM ai_incident_summary_day;
      DELETE FROM ai_prompt_log;
      DELETE FROM ml_prediction_result;
      DELETE FROM ml_feature_importance;
    "
    echo "[OK] reset mode: ai_and_ml"
    ;;
  full_results)
    run_sql "
      DELETE FROM ai_recommended_action_day;
      DELETE FROM ai_incident_summary_day;
      DELETE FROM ai_prompt_log;
      DELETE FROM ml_prediction_result;
      DELETE FROM ml_feature_importance;
      DELETE FROM scenario_experiment_result_day;
    "
    echo "[OK] reset mode: full_results"
    ;;
  *)
    echo "[ERROR] unknown mode: $MODE"
    echo "Usage: bash reset_ai_layer_tables.sh [ai_only|ai_keep_summary|ai_and_ml|full_results]"
    exit 1
    ;;
esac
