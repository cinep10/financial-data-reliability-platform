# ML Risk Model 전체

포함 파일
- ml_feature_vector_builder.py
- ml_risk_model_train.py
- ml_prediction_runner.py
- grafana_ml_prediction_queries.sql

권장 순서
1. ml_feature_vector_builder.py 로 feature table 생성
2. ml_risk_model_train.py 로 모델 학습
3. ml_prediction_runner.py 로 날짜별 예측 적재
4. Grafana에 prediction 패널 추가

추천 포인트
- 모델은 단순 Logistic Regression으로 유지
- 목적은 성능 과시가 아니라 'Data Reliability -> ML Data Reliability' 연결
- feature drift / risk score / predicted risk 를 함께 보여주는 것이 핵심
