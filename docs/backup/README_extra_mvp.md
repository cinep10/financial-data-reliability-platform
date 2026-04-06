# Extra MVP Artifacts

## Included files
- `run_backfill_pipeline.sh`: end-to-end batch backfill runner
- `run_daily_pipeline.sh`: one-day pipeline wrapper
- `ml_feature_drift_psi.py`: PSI-like feature drift detector for ML input monitoring
- `risk_score_engine_v2.py`: risk scoring including ML feature drift
- `data_reliability_architecture.md`: architecture diagrams

## Recommended execution order

```bash
bash run_backfill_pipeline.sh 2026-02-23 2026-03-09 finance_bank

python3 ml_feature_drift_psi.py   --host 127.0.0.1 --port 3306   --user nethru --password nethru1234 --db weblog   --profile-id finance_bank --date 2026-03-09

python3 risk_score_engine_v2.py   --host 127.0.0.1 --port 3306   --user nethru --password nethru1234 --db weblog   --profile-id finance_bank --date 2026-03-09
```
