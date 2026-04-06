#!/usr/bin/env python3
import argparse
import os

import pandas as pd
import pymysql


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--model-name", default="ml_risk_model")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    required = ["feature_name", "coefficient", "abs_coefficient", "importance_mean", "importance_std", "importance_rank"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    conn = pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, autocommit=False, cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cur:
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_importance WHERE profile_id=%s AND dt=%s AND model_version=%s",
                    (args.profile_id, args.date, args.model_version),
                )

            insert_sql = """
            REPLACE INTO ml_feature_importance (
                profile_id, dt, model_name, model_version, feature_name,
                coefficient, abs_coefficient, importance_mean, importance_std, importance_rank,
                run_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s
            )
            """

            for _, row in df.iterrows():
                run_id = f"mlimp_{args.profile_id}_{args.date.replace('-', '')}_{args.model_version}"
                cur.execute(
                    insert_sql,
                    (
                        args.profile_id, args.date, args.model_name, args.model_version, row["feature_name"],
                        None if pd.isna(row["coefficient"]) else float(row["coefficient"]),
                        None if pd.isna(row["abs_coefficient"]) else float(row["abs_coefficient"]),
                        None if pd.isna(row["importance_mean"]) else float(row["importance_mean"]),
                        None if pd.isna(row["importance_std"]) else float(row["importance_std"]),
                        int(row["importance_rank"]),
                        run_id,
                    ),
                )

            conn.commit()
            print(f"[OK] feature importance loaded: rows={len(df)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
