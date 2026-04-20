#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-stream_bridge_only}"

DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
DB_USER="${DB_USER:-nethru}"
DB_PASSWORD="${DB_PASSWORD:-nethru1234}"
DB_NAME="${DB_NAME:-weblog}"

run_sql() {
  mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" -e "$1"
}

case "$MODE" in
  stream_bridge_only)
    run_sql "
      DELETE FROM data_reliability_action_day WHERE action_source='stream';
      DELETE FROM data_risk_root_cause_day WHERE cause_source='stream';
      TRUNCATE TABLE stream_anomaly_truth_day;
      TRUNCATE TABLE stream_risk_signal_day;
      TRUNCATE TABLE stream_reliability_summary_day;
      TRUNCATE TABLE stream_reliability_summary_minute;
      TRUNCATE TABLE stream_latency_result;
      TRUNCATE TABLE stream_ordering_result;
      TRUNCATE TABLE stream_duplicate_result;
      TRUNCATE TABLE stream_completeness_result;
      TRUNCATE TABLE stream_risk_threshold_profile;
      TRUNCATE TABLE stg_event_stream;
    "
    echo "[OK] reset mode: stream_bridge_only"
    ;;
  stream_and_risk_bridge)
    run_sql "
      DELETE FROM data_reliability_action_day WHERE action_source='stream';
      DELETE FROM data_risk_root_cause_day WHERE cause_source='stream';
      UPDATE data_risk_score_day_v3
      SET stream_risk_score=NULL, stream_primary_issue=NULL, stream_status=NULL
      WHERE stream_risk_score IS NOT NULL OR stream_primary_issue IS NOT NULL OR stream_status IS NOT NULL;
      TRUNCATE TABLE stream_anomaly_truth_day;
      TRUNCATE TABLE stream_risk_signal_day;
      TRUNCATE TABLE stream_reliability_summary_day;
      TRUNCATE TABLE stream_reliability_summary_minute;
      TRUNCATE TABLE stream_latency_result;
      TRUNCATE TABLE stream_ordering_result;
      TRUNCATE TABLE stream_duplicate_result;
      TRUNCATE TABLE stream_completeness_result;
      TRUNCATE TABLE stream_risk_threshold_profile;
      TRUNCATE TABLE stg_event_stream;
    "
    echo "[OK] reset mode: stream_and_risk_bridge"
    ;;
  stream_full)
    run_sql "
      DELETE FROM data_reliability_action_day WHERE action_source='stream';
      DELETE FROM data_risk_root_cause_day WHERE cause_source='stream';
      UPDATE data_risk_score_day_v3
      SET stream_risk_score=NULL, stream_primary_issue=NULL, stream_status=NULL
      WHERE stream_risk_score IS NOT NULL OR stream_primary_issue IS NOT NULL OR stream_status IS NOT NULL;
      TRUNCATE TABLE stream_anomaly_truth_day;
      TRUNCATE TABLE stream_risk_signal_day;
      TRUNCATE TABLE stream_reliability_summary_day;
      TRUNCATE TABLE stream_reliability_summary_minute;
      TRUNCATE TABLE stream_latency_result;
      TRUNCATE TABLE stream_ordering_result;
      TRUNCATE TABLE stream_duplicate_result;
      TRUNCATE TABLE stream_completeness_result;
      TRUNCATE TABLE stream_risk_threshold_profile;
      TRUNCATE TABLE stream_injection_event_queue;
      TRUNCATE TABLE stg_event_stream;
      TRUNCATE TABLE stg_event_batch;
      TRUNCATE TABLE event_log_raw;
      TRUNCATE TABLE stg_wc_log_hit;
    "
    echo "[OK] reset mode: stream_full"
    ;;
  *)
    echo "[ERROR] unknown mode: $MODE"
    echo "Usage: bash reset_streaming_kafka_results.sh [stream_bridge_only|stream_and_risk_bridge|stream_full]"
    exit 1
    ;;
esac
