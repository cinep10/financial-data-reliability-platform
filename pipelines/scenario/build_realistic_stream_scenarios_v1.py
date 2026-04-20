#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from datetime import datetime, timedelta
import pymysql

SCENARIO_POOL = [
    ('baseline', 'medium', {}),
    ('partial_missing_auth', 'mild', {}),
    ('partial_missing_card', 'medium', {}),
    ('partial_missing_loan', 'high', {}),
    ('duplicate_auth', 'mild', {}),
    ('duplicate_card', 'medium', {}),
    ('duplicate_loan', 'high', {}),
    ('delay_auth', 'mild', {}),
    ('delay_branch', 'medium', {}),
    ('delay_card', 'high', {}),
    ('mixed_missing_delay_auth', 'medium', {'overlap': True}),
    ('mixed_duplicate_delay_card', 'medium', {'overlap': True}),
    ('mixed_missing_duplicate_loan', 'high', {'overlap': True}),
    ('borderline_delay_auth', 'mild', {'borderline': True, 'noise_ratio': 0.10}),
    ('borderline_duplicate_card', 'mild', {'borderline': True, 'noise_ratio': 0.10}),
    ('borderline_missing_loan', 'mild', {'borderline': True, 'noise_ratio': 0.10}),
    ('noisy_baseline', 'mild', {'noise_ratio': 0.15}),
]

def connect(args):
    return pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, charset='utf8mb4', autocommit=False,
        cursorclass=pymysql.cursors.DictCursor
    )

def daterange(dt_from, dt_to):
    cur = datetime.strptime(dt_from, '%Y-%m-%d').date()
    end = datetime.strptime(dt_to, '%Y-%m-%d').date()
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scenario_experiment_run (
            scenario_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            profile_id VARCHAR(64) NOT NULL,
            scenario_name VARCHAR(100) NOT NULL,
            scenario_type VARCHAR(30) NOT NULL DEFAULT 'unified',
            dt_from DATE NOT NULL,
            dt_to DATE NOT NULL,
            parameters_json LONGTEXT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (scenario_run_id),
            KEY idx_profile_dt (profile_id, dt_from, dt_to)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--host', required=True)
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', default='')
    ap.add_argument('--db', required=True)
    ap.add_argument('--profile-id', required=True)
    ap.add_argument('--dt-from', required=True)
    ap.add_argument('--dt-to', required=True)
    ap.add_argument('--replace-range', action='store_true')
    args = ap.parse_args()

    conn = connect(args)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            if args.replace_range:
                cur.execute("""
                    DELETE FROM scenario_experiment_run
                    WHERE profile_id=%s AND dt_from >= %s AND dt_to <= %s
                """, (args.profile_id, args.dt_from, args.dt_to))

            for idx, dt in enumerate(daterange(args.dt_from, args.dt_to)):
                name, intensity, params = SCENARIO_POOL[idx % len(SCENARIO_POOL)]
                payload = {'intensity': intensity, **params}
                cur.execute("""
                    INSERT INTO scenario_experiment_run
                    (profile_id, scenario_name, scenario_type, dt_from, dt_to, parameters_json)
                    VALUES (%s, %s, 'unified', %s, %s, %s)
                """, (args.profile_id, name, dt, dt, json.dumps(payload, ensure_ascii=False)))
        conn.commit()
        print('[build_realistic_stream_scenarios_v1] done')
    finally:
        conn.close()

if __name__ == '__main__':
    main()
