-- =========================================================
-- 004_stream_ingestion_layer.sql
-- Stream ingestion + reliability layer
-- =========================================================

CREATE TABLE IF NOT EXISTS stg_event_stream (
  stream_ingest_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  raw_event_id BIGINT UNSIGNED NOT NULL,

  dt DATE NOT NULL,
  ts DATETIME NOT NULL,
  event_name VARCHAR(100) NOT NULL,

  service_domain VARCHAR(50) NULL,
  funnel_stage VARCHAR(50) NULL,
  is_conversion TINYINT(1) NOT NULL DEFAULT 0,

  uid VARCHAR(128) NULL,
  pcid VARCHAR(128) NULL,
  sid VARCHAR(128) NULL,

  stream_topic VARCHAR(100) NULL,
  stream_partition INT NULL,
  stream_offset BIGINT NULL,
  sequence_no BIGINT NULL,

  producer_ts DATETIME NULL,
  ingest_ts DATETIME NOT NULL,
  event_delay_ms BIGINT NULL,

  status INT NULL,
  latency_ms INT NULL,

  load_status VARCHAR(20) DEFAULT 'loaded',
  anomaly_tag VARCHAR(50) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (stream_ingest_id),

  KEY idx_stg_event_stream_dt (dt),
  KEY idx_stg_event_stream_ingest_ts (ingest_ts),
  KEY idx_stg_event_stream_domain_ts (service_domain, ingest_ts),
  KEY idx_stg_event_stream_event_name (event_name),
  KEY idx_stg_event_stream_uid (uid),
  KEY idx_stg_event_stream_pcid (pcid),
  KEY idx_stg_event_stream_sid (sid),
  KEY idx_stg_event_stream_topic_offset (stream_topic, stream_partition, stream_offset),
  KEY idx_stg_event_stream_raw_event_id (raw_event_id),

  CONSTRAINT fk_stg_event_stream_raw_event
    FOREIGN KEY (raw_event_id) REFERENCES event_log_raw(raw_event_id)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_completeness_result (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  metric_minute DATETIME NOT NULL,
  service_domain VARCHAR(50) NULL,
  expected_count BIGINT NOT NULL DEFAULT 0,
  actual_count BIGINT NOT NULL DEFAULT 0,
  missing_count BIGINT NOT NULL DEFAULT 0,
  missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'ok',
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stream_completeness_metric_minute (metric_minute),
  KEY idx_stream_completeness_domain (service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_duplicate_result (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  metric_minute DATETIME NOT NULL,
  service_domain VARCHAR(50) NULL,
  total_count BIGINT NOT NULL DEFAULT 0,
  duplicate_count BIGINT NOT NULL DEFAULT 0,
  duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'ok',
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stream_duplicate_metric_minute (metric_minute),
  KEY idx_stream_duplicate_domain (service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_ordering_result (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  metric_minute DATETIME NOT NULL,
  service_domain VARCHAR(50) NULL,
  ordering_violation_count BIGINT NOT NULL DEFAULT 0,
  ordering_gap_score DECIMAL(18,6) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'ok',
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stream_ordering_metric_minute (metric_minute),
  KEY idx_stream_ordering_domain (service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_latency_result (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  metric_minute DATETIME NOT NULL,
  service_domain VARCHAR(50) NULL,
  avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
  p95_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
  consumer_lag BIGINT NULL,
  sla_breach_count BIGINT NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL DEFAULT 'ok',
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stream_latency_metric_minute (metric_minute),
  KEY idx_stream_latency_domain (service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_reliability_summary_minute (
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  metric_minute DATETIME NOT NULL,
  service_domain VARCHAR(50) NOT NULL DEFAULT 'all',
  missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
  duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
  ordering_gap_score DECIMAL(18,6) NOT NULL DEFAULT 0,
  avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
  stream_risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, metric_minute, service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stream_reliability_summary_day (
  profile_id VARCHAR(64) NOT NULL DEFAULT 'default',
  dt DATE NOT NULL,
  service_domain VARCHAR(50) NOT NULL DEFAULT 'all',
  avg_missing_rate DECIMAL(18,6) NOT NULL DEFAULT 0,
  avg_duplicate_ratio DECIMAL(18,6) NOT NULL DEFAULT 0,
  total_ordering_violations BIGINT NOT NULL DEFAULT 0,
  avg_event_delay_ms DECIMAL(18,6) NOT NULL DEFAULT 0,
  stream_risk_score DECIMAL(18,6) NOT NULL DEFAULT 0,
  note VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (profile_id, dt, service_domain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
