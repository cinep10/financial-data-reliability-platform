DROP VIEW IF EXISTS vw_stream_ml_training_dataset_v4;
CREATE VIEW vw_stream_ml_training_dataset_v4 AS
SELECT
    s.profile_id,
    s.dt,
    s.service_domain,

    s.missing_rate,
    s.duplicate_ratio,
    s.ordering_gap_score,
    s.avg_event_delay_ms,
    s.stream_risk_score,

    /* 변화량 */
    s.avg_event_delay_ms
      - LAG(s.avg_event_delay_ms) OVER (
          PARTITION BY s.profile_id, s.service_domain
          ORDER BY s.dt
        ) AS delay_diff,

    s.stream_risk_score
      - LAG(s.stream_risk_score) OVER (
          PARTITION BY s.profile_id, s.service_domain
          ORDER BY s.dt
        ) AS risk_diff,

    s.missing_rate
      - LAG(s.missing_rate) OVER (
          PARTITION BY s.profile_id, s.service_domain
          ORDER BY s.dt
        ) AS missing_diff,

    s.duplicate_ratio
      - LAG(s.duplicate_ratio) OVER (
          PARTITION BY s.profile_id, s.service_domain
          ORDER BY s.dt
        ) AS duplicate_diff,

    /* 이동 평균 / 표준편차 */
    AVG(s.avg_event_delay_ms) OVER (
      PARTITION BY s.profile_id, s.service_domain
      ORDER BY s.dt
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS delay_ma_7d,

    AVG(s.stream_risk_score) OVER (
      PARTITION BY s.profile_id, s.service_domain
      ORDER BY s.dt
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS risk_ma_7d,

    STDDEV_POP(s.avg_event_delay_ms) OVER (
      PARTITION BY s.profile_id, s.service_domain
      ORDER BY s.dt
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS delay_std_7d,

    /* 이전 상태 */
    LAG(s.primary_stream_issue) OVER (
      PARTITION BY s.profile_id, s.service_domain
      ORDER BY s.dt
    ) AS prev_issue,

    /* 달력 feature */
    DAYOFWEEK(s.dt) AS dayofweek,
    MONTH(s.dt) AS month_no,
    CASE WHEN DAYOFWEEK(s.dt) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,

    /* 교차 feature */
    s.missing_rate * s.avg_event_delay_ms AS missing_x_delay,
    s.duplicate_ratio * s.ordering_gap_score AS duplicate_x_ordering,

    /* label */
    CASE
        WHEN t.truth_missing = 1 THEN 'missing'
        WHEN t.truth_duplicate = 1 THEN 'duplicate'
        WHEN t.truth_delay = 1 THEN 'delay'
        WHEN t.truth_ordering = 1 THEN 'ordering'
        ELSE 'normal'
    END AS label,

    CASE
        WHEN t.truth_missing = 1
          OR t.truth_duplicate = 1
          OR t.truth_delay = 1
          OR t.truth_ordering = 1
        THEN 1 ELSE 0
    END AS anomaly_flag,

    CASE
        WHEN t.truth_missing = 1 THEN 90
        WHEN t.truth_duplicate = 1 THEN 75
        WHEN t.truth_ordering = 1 THEN 65
        WHEN t.truth_delay = 1 THEN 55
        ELSE 10
    END AS target_risk_score

FROM stream_risk_signal_day s
JOIN stream_anomaly_truth_day t
  ON s.profile_id = t.profile_id
 AND s.dt = t.dt
 AND s.service_domain = t.service_domain;
