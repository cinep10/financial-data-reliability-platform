# Unified Scenario Review and Recommended Changes

## Review summary

Your recent run succeeded end-to-end:

- `run_unified_scenario_pipeline.sh` built `exogenous_state_timeline`, ran stream and batch adapters, reran pre-ML, and summarized the scenario. That matches the logged steps and the uploaded script flow.
- `run_pre_ml_backfill_pipeline_final.sh` reran collector, analyzer, validation, drift, risk v4, root cause, and action engine as expected.
- The existing Kafka streaming pipeline already completes producer -> consumer -> reliability -> bridge flow successfully.

## What is good

1. Unified scenario execution is working.
2. The new unified pipeline correctly shifts the source of truth upward into `scenario_plan` and `exogenous_state_timeline`.
3. Streaming and batch can now share one scenario source.

## What should be changed

### 1. Avoid accumulating duplicate active scenario plans
Current `run_unified_scenario_pipeline.sh` always inserts a new `scenario_plan` row with `active_flag=1`.
On repeated runs for the same date/scenario/profile, multiple active plans can accumulate and overlap.

Recommendation:
- Before inserting a new plan, deactivate overlapping active plans for the same `profile_id + scenario_name + date range`.
- Or use a dedicated `scenario_run_id` and tie `exogenous_state_timeline` rows to that run.

### 2. Separate generator preview from generator apply
Current step 2 only writes a preview JSON to `/tmp/source_generator_adapter_preview.json`.
It does not actually feed the generator.

Recommendation:
- Keep `source_generator_adapter.py` as preview/export.
- Add a small loader used by the simulator so generator code can query `exogenous_state_timeline` by `dt/hh/profile_id`.

### 3. Clarify role split between old and new scenario runners
Current scripts overlap:
- `run_scenario_test_pipeline.sh`: old metric-direct scenario injection flow
- `run_unified_scenario_pipeline.sh`: new exogenous-source scenario flow
- `run_scenario_matrix.sh`: still calls the old runner for non-baseline rows

Recommendation:
- Keep old runner only for legacy regression / fallback.
- Move matrix runs to the unified runner by default.
- Rename old runner semantics clearly in docs.

## Recommended role split

### Keep
- `run_pre_ml_backfill_pipeline_final.sh`: canonical pre-ML recomputation pipeline
- `run_streaming_kafka_pipeline.sh`: canonical streaming reliability execution pipeline

### Legacy only
- `run_scenario_test_pipeline.sh`: legacy metric-direct scenario injection runner

### New default
- `run_unified_scenario_pipeline.sh`: default scenario execution runner for batch/stream/performance/availability

### Should be updated
- `run_scenario_matrix.sh` should call the unified runner for non-baseline scenarios

## Generator integration recommendation

The simulator should not call the DB from deep random helper functions repeatedly.
Instead, load a day/hour exogenous snapshot once, then pass it into the generator/session builder.

Recommended flow:

scenario_plan
  ->
exogenous_state_timeline
  ->
ExogenousTimelineLoader
  ->
generator / session builder / page chooser / outcome logic

Use the loaded snapshot to influence:
- traffic volume
- page mix / funnel composition
- conversion rate
- timeout / retry behavior
- system degradation flags in kv

## Practical next work items

1. Add `core/exogenous/exogenous_loader.py`
2. Patch simulator entrypoint to optionally read timeline rows
3. Update `run_unified_scenario_pipeline.sh` to deactivate overlapping plans before insert
4. Add `run_scenario_matrix_unified.sh`
5. Keep `run_scenario_test_pipeline.sh` only for fallback/legacy comparison
