#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PLAN_FILE="${1:?plan csv required}"
PROFILE_ID="${2:-finance_bank}"

TRAIN_ALL_HISTORY="${TRAIN_ALL_HISTORY:-true}"
TRAIN_MODE="${TRAIN_MODE:-curated_history}"

if [[ ! -f "$PLAN_FILE" ]]; then
  echo "[ERROR] plan file not found: $PLAN_FILE"
  exit 1
fi

echo "[INFO] scenario matrix start plan=$PLAN_FILE profile=$PROFILE_ID"
echo "[INFO] TRAIN_ALL_HISTORY=$TRAIN_ALL_HISTORY TRAIN_MODE=$TRAIN_MODE"

tail -n +2 "$PLAN_FILE" | while IFS=, read -r START_DATE END_DATE SCENARIO_NAME INTENSITY NOTE
do
  [[ -z "${START_DATE}" ]] && continue
  echo "--------------------------------------------"
  echo "[RUN] $START_DATE ~ $END_DATE | $SCENARIO_NAME | $INTENSITY | ${NOTE:-}"

  if [[ "$SCENARIO_NAME" == "baseline" ]]; then
    bash "$PROJECT_ROOT/deploy/run_pre_ml_backfill_pipeline_final.sh" "$START_DATE" "$END_DATE" "$PROFILE_ID"
    TRAIN_ALL_HISTORY="$TRAIN_ALL_HISTORY" \
    TRAIN_MODE="$TRAIN_MODE" \
    bash "$PROJECT_ROOT/deploy/run_ml_backfill_pipeline_v2.sh" "$START_DATE" "$END_DATE" "$PROFILE_ID"
  else
    TRAIN_ALL_HISTORY="$TRAIN_ALL_HISTORY" \
    TRAIN_MODE="$TRAIN_MODE" \
    bash "$PROJECT_ROOT/deploy/run_scenario_test_pipeline.sh" \
      "$SCENARIO_NAME" "$START_DATE" "$END_DATE" "$PROFILE_ID" "$INTENSITY"
  fi
done

echo "[DONE] scenario matrix completed"
