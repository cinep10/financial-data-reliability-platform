from __future__ import annotations
import argparse, pymysql

REQUIRED_COLUMNS = [
    ("source_type", "VARCHAR(50) NULL"),
    ("path", "VARCHAR(255) NULL"),
    ("evt", "VARCHAR(50) NULL"),
    ("load_status", "VARCHAR(20) DEFAULT 'loaded'"),
    ("anomaly_tag", "VARCHAR(50) NULL"),
]

CREATE_SQL = '''
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
  source_type VARCHAR(50) NULL,
  path VARCHAR(255) NULL,
  evt VARCHAR(50) NULL,
  load_status VARCHAR(20) DEFAULT 'loaded',
  anomaly_tag VARCHAR(50) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (stream_ingest_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
'''

def connect_mysql(a):
    return pymysql.connect(
        host=a.db_host, port=a.db_port, user=a.db_user, password=a.db_pass,
        database=a.db_name, charset='utf8mb4', autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def has_column(cur, t, c):
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name=%s AND column_name=%s",
        (t, c),
    )
    return int(cur.fetchone()['cnt']) > 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db-host', default='127.0.0.1')
    ap.add_argument('--db-port', type=int, default=3306)
    ap.add_argument('--db-user', required=True)
    ap.add_argument('--db-pass', default='')
    ap.add_argument('--db-name', required=True)
    ap.add_argument('--drop-and-recreate', action='store_true')
    args = ap.parse_args()

    conn = connect_mysql(args)
    try:
        with conn.cursor() as cur:
            if args.drop_and_recreate:
                cur.execute('DROP TABLE IF EXISTS stg_event_stream')
            cur.execute(CREATE_SQL)
            for col, ddl in REQUIRED_COLUMNS:
                if not has_column(cur, 'stg_event_stream', col):
                    cur.execute(f'ALTER TABLE stg_event_stream ADD COLUMN {col} {ddl}')
        conn.commit()
        print('[ensure_stg_event_stream_schema] done')
    finally:
        conn.close()

if __name__ == '__main__':
    main()

