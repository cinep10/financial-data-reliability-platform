# Safe ML pipeline

These files are designed to avoid schema-mismatch failures against the current AS-IS DB.

Files:
- ml_feature_vector_builder_safe.py
- ml_risk_model_train_safe.py
- ml_prediction_runner_safe.py
- ml_feature_importance_loader_safe.py
- ml_feature_drift_analyzer_safe.py
- run_ml_pipeline_safe.sh

Key safety features:
- builder reads table columns dynamically
- risk label source supports current `data_risk_score_day_v3`
- prediction explanations go into `ml_prediction_result.note`
- drift analyzer writes to current `ml_feature_drift_result`
