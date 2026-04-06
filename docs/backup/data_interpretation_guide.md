# Data Interpretation Guide

## 1. How to read risk score
- validation_warn_count up: rule consistency issues dominate
- drift_alert_count up: behavior distribution shifted strongly
- ml_feature_alert_count up: model input reliability degraded

## 2. How to read root cause output
Top causes are ranked by:
- source severity
- drift magnitude
- whether the metric belongs to traffic / funnel / quality

Typical mappings:
- traffic_drop: page views, DAU, raw events are below baseline
- funnel_distortion: starts/submits or views/starts misalign
- quality_shift: missing rate or collector/raw mismatch expands
- mapping_gap: validation warns that events exist without expected downstream outcome

## 3. How to validate scenarios
Expected responses:
- campaign:
  - page_view_count up
  - loan/card start up
  - drift alerts may rise mildly
  - risk score should rise modestly, not explode
- holiday:
  - DAU / page_view down
  - funnel volume down
  - drift alerts may rise, but quality should stay stable
- weather:
  - moderate traffic pattern change
  - prediction should move less than system_issue
- system_issue:
  - missing rate up
  - collector/raw instability
  - strongest drift + highest risk score

## 4. Stability checklist
- risk score should not be alert every day
- top root causes should change with scenario
- missing rate should only spike in system_issue-like conditions
- prediction probability should react in same direction as drift, but smoother
