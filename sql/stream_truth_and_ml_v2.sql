/* =========================================================
   stream truth v2 + evaluation + ML dataset
   Replace profile/date literals if needed
   ========================================================= */

CREATE TABLE IF NOT EXISTS stream_anomaly_truth_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    src_cnt BIGINT NOT NULL DEFAULT 0,
    queue_cnt BIGINT NOT NULL DEFAULT 0,
    truth_missing TINYINT(1) NOT NULL DEFAULT 0,
    truth_duplicate TINYINT(1) NOT NULL DEFAULT 0,
    truth_delay TINYINT(1) NOT NULL DEFAULT 0,
    truth_ordering TINYINT(1) NOT NULL DEFAULT 0,
    truth_label VARCHAR(20) NOT NULL DEFAULT 'normal',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DELETE FROM stream_anomaly_truth_day
WHERE profile_id = 'finance_bank'
  AND dt BETWEEN '2026-04-01' AND '2026-04-30';

INSERT INTO stream_anomaly_truth_day
(
    profile_id,
    dt,
    service_domain,
    src_cnt,
    queue_cnt,
    truth_missing,
    truth_duplicate,
    truth_delay,
    truth_ordering,
    truth_label
)
SELECT
    src.profile_id,
    src.dt,
    src.service_domain,
    src.src_cnt,
    COALESCE(q.queue_cnt, 0) AS queue_cnt,

    CASE WHEN src.src_cnt > COALESCE(q.queue_cnt, 0) THEN 1 ELSE 0 END AS truth_missing,
    CASE WHEN COALESCE(q.duplicate_cnt, 0) > 0 THEN 1 ELSE 0 END AS truth_duplicate,
    CASE WHEN COALESCE(q.delay_cnt, 0) > 0 THEN 1 ELSE 0 END AS truth_delay,
    CASE WHEN COALESCE(q.ordering_cnt, 0) > 0 THEN 1 ELSE 0 END AS truth_ordering,

    CASE
        WHEN src.src_cnt > COALESCE(q.queue_cnt, 0) THEN 'missing'
        WHEN COALESCE(q.duplicate_cnt, 0) > 0 THEN 'duplicate'
        WHEN COALESCE(q.delay_cnt, 0) > 0 THEN 'delay'
        WHEN COALESCE(q.ordering_cnt, 0) > 0 THEN 'ordering'
        ELSE 'normal'
    END AS truth_label

FROM
(
    SELECT
        'finance_bank' AS profile_id,
        dt,
        service_domain,
        COUNT(*) AS src_cnt
    FROM event_log_raw
    WHERE dt BETWEEN '2026-04-01' AND '2026-04-30'
    GROUP BY dt, service_domain
) src

LEFT JOIN
(
    SELECT
        profile_id,
        dt,
        service_domain,
        COUNT(*) AS queue_cnt,
        SUM(CASE WHEN anomaly_tag = 'duplicate' THEN 1 ELSE 0 END) AS duplicate_cnt,
        SUM(CASE WHEN anomaly_tag = 'delay' THEN 1 ELSE 0 END) AS delay_cnt,
        SUM(CASE WHEN anomaly_tag = 'ordering' THEN 1 ELSE 0 END) AS ordering_cnt
    FROM stream_injection_event_queue
    WHERE profile_id = 'finance_bank'
      AND dt BETWEEN '2026-04-01' AND '2026-04-30'
    GROUP BY profile_id, dt, service_domain
) q
ON src.profile_id = q.profile_id
AND src.dt = q.dt
AND src.service_domain = q.service_domain;

/* prediction view from current signal */
DROP VIEW IF EXISTS vw_stream_anomaly_pred_day_v2;
CREATE VIEW vw_stream_anomaly_pred_day_v2 AS
SELECT
    profile_id,
    dt,
    service_domain,
    CASE WHEN primary_stream_issue = 'missing' THEN 1 ELSE 0 END AS pred_missing,
    CASE WHEN primary_stream_issue = 'duplicate' THEN 1 ELSE 0 END AS pred_duplicate,
    CASE WHEN primary_stream_issue = 'delay' THEN 1 ELSE 0 END AS pred_delay,
    CASE WHEN primary_stream_issue = 'ordering' THEN 1 ELSE 0 END AS pred_ordering,
    CASE
        WHEN primary_stream_issue IN ('missing','duplicate','delay','ordering') THEN primary_stream_issue
        ELSE 'normal'
    END AS pred_label,
    stream_risk_score,
    status
FROM stream_risk_signal_day;

/* row-level evaluation */
DROP VIEW IF EXISTS vw_stream_anomaly_eval_day_v2;
CREATE VIEW vw_stream_anomaly_eval_day_v2 AS
SELECT
    t.profile_id,
    t.dt,
    t.service_domain,
    t.truth_label,
    COALESCE(p.pred_label, 'normal') AS pred_label,
    t.truth_missing,
    COALESCE(p.pred_missing, 0) AS pred_missing,
    t.truth_duplicate,
    COALESCE(p.pred_duplicate, 0) AS pred_duplicate,
    t.truth_delay,
    COALESCE(p.pred_delay, 0) AS pred_delay,
    t.truth_ordering,
    COALESCE(p.pred_ordering, 0) AS pred_ordering,
    CASE WHEN t.truth_label = COALESCE(p.pred_label, 'normal') THEN 1 ELSE 0 END AS is_correct
FROM stream_anomaly_truth_day t
LEFT JOIN vw_stream_anomaly_pred_day_v2 p
  ON t.profile_id = p.profile_id
 AND t.dt = p.dt
 AND t.service_domain = p.service_domain;

/* anomaly-wise precision / recall */
DROP VIEW IF EXISTS vw_stream_anomaly_metrics_v2;
CREATE VIEW vw_stream_anomaly_metrics_v2 AS
SELECT
    anomaly_type,
    tp,
    fp,
    fn,
    ROUND(tp / NULLIF(tp + fp, 0), 4) AS precision_score,
    ROUND(tp / NULLIF(tp + fn, 0), 4) AS recall_score
FROM (
    SELECT
        'missing' AS anomaly_type,
        SUM(CASE WHEN truth_missing = 1 AND pred_missing = 1 THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN truth_missing = 0 AND pred_missing = 1 THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN truth_missing = 1 AND pred_missing = 0 THEN 1 ELSE 0 END) AS fn
    FROM vw_stream_anomaly_eval_day_v2
    UNION ALL
    SELECT
        'duplicate',
        SUM(CASE WHEN truth_duplicate = 1 AND pred_duplicate = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_duplicate = 0 AND pred_duplicate = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_duplicate = 1 AND pred_duplicate = 0 THEN 1 ELSE 0 END)
    FROM vw_stream_anomaly_eval_day_v2
    UNION ALL
    SELECT
        'delay',
        SUM(CASE WHEN truth_delay = 1 AND pred_delay = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_delay = 0 AND pred_delay = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_delay = 1 AND pred_delay = 0 THEN 1 ELSE 0 END)
    FROM vw_stream_anomaly_eval_day_v2
    UNION ALL
    SELECT
        'ordering',
        SUM(CASE WHEN truth_ordering = 1 AND pred_ordering = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_ordering = 0 AND pred_ordering = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN truth_ordering = 1 AND pred_ordering = 0 THEN 1 ELSE 0 END)
    FROM vw_stream_anomaly_eval_day_v2
) z;

/* ML-ready dataset */
DROP VIEW IF EXISTS vw_stream_ml_training_dataset_v2;
CREATE VIEW vw_stream_ml_training_dataset_v2 AS
SELECT
    s.profile_id,
    s.dt,
    s.service_domain,
    s.missing_rate,
    s.duplicate_ratio,
    s.ordering_gap_score,
    s.avg_event_delay_ms,
    s.stream_risk_score,
    s.status,
    t.truth_missing,
    t.truth_duplicate,
    t.truth_delay,
    t.truth_ordering,
    t.truth_label AS label
FROM stream_risk_signal_day s
JOIN stream_anomaly_truth_day t
  ON s.profile_id = t.profile_id
 AND s.dt = t.dt
 AND s.service_domain = t.service_domain
;
