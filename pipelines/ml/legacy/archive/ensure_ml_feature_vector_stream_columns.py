from __future__ import annotations
import argparse
import pymysql

ADD_COLS = [
    ("stream_missing_rate", "DECIMAL(20,6) NULL"),
    ("stream_duplicate_ratio", "DECIMAL(20,6) NULL"),
    ("stream_ordering_gap_score", "DECIMAL(20,6) NULL"),
    ("stream_avg_event_delay_ms", "DECIMAL(20,6) NULL"),
    ("stream_risk_score", "DECIMAL(20,6) NULL"),
    ("stream_signal_count", "INT NULL"),
    ("weather_type", "VARCHAR(50) NULL"),
    ("campaign_flag_text", "VARCHAR(50) NULL"),
    ("system_flag_text", "VARCHAR(50) NULL"),
    ("volume_multiplier", "DECIMAL(20,6) NULL"),
    ("conversion_multiplier", "DECIMAL(20,6) NULL"),
    ("timeout_multiplier", "DECIMAL(20,6) NULL"),
    ("retry_multiplier", "DECIMAL(20,6) NULL"),
]

def conn(a):
    return pymysql.connect(host=a.host, port=a.port, user=a.user, password=a.password, database=a.db,
                           charset='utf8mb4', autocommit=False, cursorclass=pymysql.cursors.DictCursor)

def has_col(cur, table, col):
    cur.execute("SELECT COUNT(*) AS cnt FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s AND column_name=%s", (table, col))
    return int(cur.fetchone()['cnt']) > 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=3306)
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', default='')
    ap.add_argument('--db', required=True)
    args = ap.parse_args()
    c = conn(args)
    try:
        with c.cursor() as cur:
            for col, ddl in ADD_COLS:
                if not has_col(cur, 'ml_feature_vector_day', col):
                    cur.execute(f"ALTER TABLE ml_feature_vector_day ADD COLUMN {col} {ddl}")
        c.commit()
        print('[ensure_ml_feature_vector_stream_columns] done')
    finally:
        c.close()

if __name__ == '__main__':
    main()
