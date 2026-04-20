from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import List

import pymysql


def parse_columns(s: str) -> List[str]:
    return [x.strip() for x in s.split(',') if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description='Load TSV into MySQL/MariaDB table')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=3306)
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', default='')
    ap.add_argument('--db', required=True)
    ap.add_argument('--table', required=True)
    ap.add_argument('--tsv', required=True)
    ap.add_argument('--columns', required=True)
    ap.add_argument('--charset', default='utf8mb4')
    ap.add_argument('--fallback-insert', action='store_true')
    ap.add_argument('--truncate-target', action='store_true', help='truncate target table before loading')
    args = ap.parse_args()

    cols = parse_columns(args.columns)
    tsv = Path(args.tsv)

    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset=args.charset,
        local_infile=True,
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            if args.truncate_target:
                print(f"[load_tsv_to_db] truncating table {args.table}")
                cur.execute(f"TRUNCATE TABLE {args.table}")
                conn.commit()

            try:
                sql = (
                    f"LOAD DATA LOCAL INFILE %s INTO TABLE {args.table} CHARACTER SET {args.charset} "
                    "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' "
                    f"({','.join(cols)})"
                )
                cur.execute(sql, (str(tsv),))
                conn.commit()
                print(f"[load_tsv_to_db] loaded via LOAD DATA: {cur.rowcount}")
                return
            except Exception:
                conn.rollback()
                print("[load_tsv_to_db] LOAD DATA failed, fallback insert")
                if not args.fallback_insert:
                    raise

            placeholders = ','.join(['%s'] * len(cols))
            sql = f"INSERT INTO {args.table} ({','.join(cols)}) VALUES ({placeholders})"
            with tsv.open('r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, delimiter='\t')
                batch = []
                total = 0
                for row in reader:
                    batch.append([None if x == '' else x for x in row[:len(cols)]])
                    if len(batch) >= 1000:
                        cur.executemany(sql, batch)
                        total += len(batch)
                        batch = []
                if batch:
                    cur.executemany(sql, batch)
                    total += len(batch)
            conn.commit()
            print(f"[load_tsv_to_db] loaded via fallback insert: {total}")
    finally:
        conn.close()

if __name__ == '__main__':
    main()
