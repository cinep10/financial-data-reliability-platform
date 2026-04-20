/* =========================================================
   1. Ground truth table from injection queue
   ========================================================= */
CREATE TABLE IF NOT EXISTS stream_anomaly_truth_day AS
SELECT
    profile_id,
    dt,
    service_domain,
    MAX(CASE WHEN anomaly_tag = 'duplicate' THEN 1 ELSE 0 END) AS truth_duplicate,
    MAX(CASE WHEN anomaly_tag = 'delay' THEN 1 ELSE 0 END) AS truth_delay,
    MAX(CASE WHEN anomaly_tag = 'ordering' THEN 1 ELSE 0 END) AS truth_ordering,
    0 AS truth_missing
FROM stream_injection_event_queue
WHERE 1=0;

/* add primary key if table newly created without one */
ALTER TABLE stream_anomaly_truth_day
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

ALTER TABLE stream_anomaly_truth_day
    ADD UNIQUE KEY IF NOT EXISTS uq_stream_truth_day (profile_id, dt, service_domain);

/* =========================================================
   2. Rebuild truth table by date range
   Replace dates/profile before execution if needed
   ========================================================= */
DELETE FROM stream_anomaly_truth_day
WHERE profile_id = 'finance_bank'
  AND dt BETWEEN '2026-04-01' AND '2026-04-30';

INSERT INTO stream_anomaly_truth_day
(
    profile_id,
    dt,
    service_domain,
    truth_duplicate,
    truth_delay,
    truth_ordering,
    truth_missing
)
SELECT
    q.profile_id,
    q.dt,
    q.service_domain,

    /* duplicate */
    MAX(CASE WHEN q.any_tag = 'duplicate' THEN 1 ELSE 0 END) AS truth_duplicate,

    /* delay */
    MAX(CASE WHEN q.any_tag = 'delay' THEN 1 ELSE 0 END) AS truth_delay,

    /* ordering */
    MAX(CASE WHEN q.any_tag = 'ordering' THEN 1 ELSE 0 END) AS truth_ordering,

    /* missing (count 기반 추론) */
    CASE
        WHEN src.src_cnt > q.queue_cnt THEN 1
        ELSE 0
    END AS truth_missing

FROM
(
    SELECT
        profile_id,
        dt,
        service_domain,
        COUNT(*) AS queue_cnt,
        MAX(anomaly_tag) AS any_tag
    FROM stream_injection_event_queue
    WHERE profile_id = 'finance_bank'
      AND dt BETWEEN '2026-04-01' AND '2026-04-30'
    GROUP BY profile_id, dt, service_domain
) q

JOIN
(
    SELECT
        profile_id,
        dt,
        service_domain,
        COUNT(*) AS src_cnt
    FROM
    (
        SELECT
            'finance_bank' AS profile_id,
            dt,
            service_domain
        FROM event_log_raw
        WHERE dt BETWEEN '2026-04-01' AND '2026-04-30'
    ) s
    GROUP BY profile_id, dt, service_domain
) src
ON q.profile_id = src.profile_id
AND q.dt = src.dt
AND q.service_domain = src.service_domain

GROUP BY
    q.profile_id,
    q.dt,
    q.service_domain,
    src.src_cnt,
    q.queue_cnt;

/* =========================================================
   3. Prediction table view
   ========================================================= */
DROP VIEW IF EXISTS vw_stream_anomaly_pred_day;
CREATE VIEW vw_stream_anomaly_pred_day AS
SELECT
    profile_id,
    dt,
    service_domain,
    CASE WHEN primary_stream_issue = 'missing' THEN 1 ELSE 0 END AS pred_missing,
    CASE WHEN primary_stream_issue = 'duplicate' THEN 1 ELSE 0 END AS pred_duplicate,
    CASE WHEN primary_stream_issue = 'delay' THEN 1 ELSE 0 END AS pred_delay,
    CASE WHEN primary_stream_issue = 'ordering' THEN 1 ELSE 0 END AS pred_ordering,
    primary_stream_issue,
    stream_risk_score,
    status
FROM stream_risk_signal_day;

/* =========================================================
   4. Precision / Recall by anomaly type
   ========================================================= */
DROP VIEW IF EXISTS vw_stream_anomaly_eval_day;
CREATE VIEW vw_stream_anomaly_eval_day AS
SELECT
    t.profile_id,
    t.dt,
    t.service_domain,

    t.truth_missing,
    p.pred_missing,
    t.truth_duplicate,
    p.pred_duplicate,
    t.truth_delay,
    p.pred_delay,
    t.truth_ordering,
    p.pred_ordering,

    CASE WHEN t.truth_missing = 1 AND p.pred_missing = 1 THEN 1 ELSE 0 END AS tp_missing,
    CASE WHEN t.truth_missing = 0 AND p.pred_missing = 1 THEN 1 ELSE 0 END AS fp_missing,
    CASE WHEN t.truth_missing = 1 AND p.pred_missing = 0 THEN 1 ELSE 0 END AS fn_missing,

    CASE WHEN t.truth_duplicate = 1 AND p.pred_duplicate = 1 THEN 1 ELSE 0 END AS tp_duplicate,
    CASE WHEN t.truth_duplicate = 0 AND p.pred_duplicate = 1 THEN 1 ELSE 0 END AS fp_duplicate,
    CASE WHEN t.truth_duplicate = 1 AND p.pred_duplicate = 0 THEN 1 ELSE 0 END AS fn_duplicate,

    CASE WHEN t.truth_delay = 1 AND p.pred_delay = 1 THEN 1 ELSE 0 END AS tp_delay,
    CASE WHEN t.truth_delay = 0 AND p.pred_delay = 1 THEN 1 ELSE 0 END AS fp_delay,
    CASE WHEN t.truth_delay = 1 AND p.pred_delay = 0 THEN 1 ELSE 0 END AS fn_delay,

    CASE WHEN t.truth_ordering = 1 AND p.pred_ordering = 1 THEN 1 ELSE 0 END AS tp_ordering,
    CASE WHEN t.truth_ordering = 0 AND p.pred_ordering = 1 THEN 1 ELSE 0 END AS fp_ordering,
    CASE WHEN t.truth_ordering = 1 AND p.pred_ordering = 0 THEN 1 ELSE 0 END AS fn_ordering
FROM stream_anomaly_truth_day t
LEFT JOIN vw_stream_anomaly_pred_day p
  ON t.profile_id = p.profile_id
 AND t.dt = p.dt
 AND t.service_domain = p.service_domain;

/* =========================================================
   5. Summary metrics for ML readiness
   ========================================================= */
SELECT
    'missing' AS anomaly_type,
    SUM(tp_missing) AS tp,
    SUM(fp_missing) AS fp,
    SUM(fn_missing) AS fn,
    ROUND(SUM(tp_missing) / NULLIF(SUM(tp_missing) + SUM(fp_missing), 0), 4) AS precision_score,
    ROUND(SUM(tp_missing) / NULLIF(SUM(tp_missing) + SUM(fn_missing), 0), 4) AS recall_score
FROM vw_stream_anomaly_eval_day
UNION ALL
SELECT
    'duplicate',
    SUM(tp_duplicate),
    SUM(fp_duplicate),
    SUM(fn_duplicate),
    ROUND(SUM(tp_duplicate) / NULLIF(SUM(tp_duplicate) + SUM(fp_duplicate), 0), 4),
    ROUND(SUM(tp_duplicate) / NULLIF(SUM(tp_duplicate) + SUM(fn_duplicate), 0), 4)
FROM vw_stream_anomaly_eval_day
UNION ALL
SELECT
    'delay',
    SUM(tp_delay),
    SUM(fp_delay),
    SUM(fn_delay),
    ROUND(SUM(tp_delay) / NULLIF(SUM(tp_delay) + SUM(fp_delay), 0), 4),
    ROUND(SUM(tp_delay) / NULLIF(SUM(tp_delay) + SUM(fn_delay), 0), 4)
FROM vw_stream_anomaly_eval_day
UNION ALL
SELECT
    'ordering',
    SUM(tp_ordering),
    SUM(fp_ordering),
    SUM(fn_ordering),
    ROUND(SUM(tp_ordering) / NULLIF(SUM(tp_ordering) + SUM(fp_ordering), 0), 4),
    ROUND(SUM(tp_ordering) / NULLIF(SUM(tp_ordering) + SUM(fn_ordering), 0), 4)
FROM vw_stream_anomaly_eval_day;

/* =========================================================
   6. ML training dataset
   ========================================================= */
DROP VIEW IF EXISTS vw_stream_ml_training_dataset_v1;
CREATE VIEW vw_stream_ml_training_dataset_v1 AS
SELECT
    s.profile_id,
    s.dt,
    s.service_domain,
    s.missing_rate,
    s.duplicate_ratio,
    s.ordering_gap_score,
    s.avg_event_delay_ms,
    s.stream_risk_score,
    CASE
        WHEN t.truth_missing = 1 THEN 'missing'
        WHEN t.truth_duplicate = 1 THEN 'duplicate'
        WHEN t.truth_delay = 1 THEN 'delay'
        WHEN t.truth_ordering = 1 THEN 'ordering'
        ELSE 'normal'
    END AS label
FROM stream_risk_signal_day s
LEFT JOIN stream_anomaly_truth_day t
  ON s.profile_id = t.profile_id
 AND s.dt = t.dt
 AND s.service_domain = t.service_domain;
