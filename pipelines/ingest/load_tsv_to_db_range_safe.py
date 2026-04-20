#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pymysql

def connect(args):
    return pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, charset="utf8mb4", autocommit=False,
        local_infile=True, cursorclass=pymysql.cursors.DictCursor
    )

def main():
    ap = argparse.ArgumentParser(description="Date-range safe TSV loader")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--table", default="stg_webserver_log_hit")
    ap.add_argument("--tsv", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--delete-date-range", action="store_true")
    args = ap.parse_args()

    tsv = Path(args.tsv).resolve()
    conn = connect(args)
    try:
        with conn.cursor() as cur:
            if args.delete_date_range:
                cur.execute(
                    f"DELETE FROM {args.table} WHERE dt BETWEEN %s AND %s",
                    (args.dt_from, args.dt_to),
                )
                print(f"[load_tsv_to_db_range_safe] deleted existing rows in {args.table} for {args.dt_from}~{args.dt_to}")

            sql = f"""
            LOAD DATA LOCAL INFILE %s
            INTO TABLE {args.table}
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY '\\t'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES
            (dt,ts,ip,method,url_raw,url_full,url_norm,host,path,`query`,
             status,bytes,latency_ms,ref,ref_host,ua,kv_raw,uid,pcid,sid,
             device_type,evt,accept_lang,cc,page_type)
            """
            cur.execute(sql, (str(tsv),))
        conn.commit()
        print(f"[load_tsv_to_db_range_safe] loaded tsv={tsv} into table={args.table}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
