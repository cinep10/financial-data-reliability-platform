-- =========================================================
-- 001_legacy_staging.sql
-- Legacy parsed/raw staging + WC compatibility
-- =========================================================

CREATE TABLE IF NOT EXISTS stg_webserver_log_hit (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  dt DATE NOT NULL,
  ts DATETIME NOT NULL,
  ip VARCHAR(45) NOT NULL,
  method VARCHAR(10) NOT NULL,
  url_raw TEXT NOT NULL,
  url_full TEXT NOT NULL,
  url_norm TEXT NOT NULL,
  host VARCHAR(255) NULL,
  path VARCHAR(2048) NULL,
  query TEXT NULL,
  status INT NOT NULL,
  bytes BIGINT NULL,
  latency_ms INT NULL,
  ref TEXT NULL,
  ref_host VARCHAR(255) NULL,
  ua TEXT NULL,
  kv_raw TEXT NULL,
  uid VARCHAR(128) NULL,
  pcid VARCHAR(128) NULL,
  sid VARCHAR(128) NULL,
  device_type VARCHAR(50) NULL,
  evt VARCHAR(50) NULL,
  accept_lang VARCHAR(100) NULL,
  cc VARCHAR(20) NULL,
  page_type VARCHAR(50) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stg_webserver_log_hit_dt_ts (dt, ts),
  KEY idx_stg_webserver_log_hit_uid (uid),
  KEY idx_stg_webserver_log_hit_pcid (pcid),
  KEY idx_stg_webserver_log_hit_sid (sid),
  KEY idx_stg_webserver_log_hit_evt (evt),
  KEY idx_stg_webserver_log_hit_page_type (page_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS stg_wc_log_hit (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  dt DATE NOT NULL,
  ts DATETIME NOT NULL,
  ip VARCHAR(45) NOT NULL,
  method VARCHAR(10) NOT NULL,
  url_raw TEXT NOT NULL,
  url_full TEXT NOT NULL,
  url_norm TEXT NOT NULL,
  host VARCHAR(255) NULL,
  path VARCHAR(2048) NULL,
  query TEXT NULL,
  status INT NOT NULL,
  bytes BIGINT NULL,
  ref TEXT NULL,
  ua TEXT NULL,
  kv_raw TEXT NULL,
  uid VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_stg_wc_log_hit_dt_ts (dt, ts),
  KEY idx_stg_wc_log_hit_uid (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
