from __future__ import annotations

import argparse
from datetime import datetime
import pandas as pd
import pymysql


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(host=host, port=port, user=user, password=password, database=db, charset="utf8mb4", autocommit=False, cursorclass=pymysql.cursors.DictCursor)


def ensure_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_importance (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          model_name VARCHAR(100) NOT NULL,
          model_version VARCHAR(64) NOT NULL,
          feature_schema_version VARCHAR(20) NULL,
          feature_name VARCHAR(100) NOT NULL,
          coefficient DECIMAL(20,10) NULL,
          abs_coefficient DECIMAL(20,10) NULL,
          importance_mean DECIMAL(20,10) NULL,
          importance_std DECIMAL(20,10) NULL,
          importance_rank INT NOT NULL,
          run_id VARCHAR(64) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, model_name, model_version, feature_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Load feature importance csv to DB v2")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--model-name", default="logistic_risk_classifier")
    ap.add_argument("--model-version", default="ml_risk_v2")
    ap.add_argument("--feature-schema-version", default="v2")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    df = pd.read_csv(args.csv).copy()
    if "abs_coefficient" in df.columns:
        df = df.sort_values("abs_coefficient", ascending=False)
    elif "importance_mean" in df.columns:
        df = df.sort_values("importance_mean", ascending=False)
    df["importance_rank"] = range(1, len(df) + 1)
    run_id = f"mlimp2_{args.profile_id}_{args.date.replace('-', '')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
            if args.truncate:
                cur.execute(
                    "DELETE FROM ml_feature_importance WHERE profile_id=%s AND dt=%s AND model_name=%s AND model_version=%s",
                    (args.profile_id, args.date, args.model_name, args.model_version),
                )
            rows = []
            for _, r in df.iterrows():
                rows.append((
                    args.profile_id, args.date, args.model_name, args.model_version, args.feature_schema_version,
                    str(r["feature_name"]),
                    None if pd.isna(r.get("coefficient")) else float(r.get("coefficient")),
                    None if pd.isna(r.get("abs_coefficient")) else float(r.get("abs_coefficient")),
                    None if pd.isna(r.get("importance_mean")) else float(r.get("importance_mean")),
                    None if pd.isna(r.get("importance_std")) else float(r.get("importance_std")),
                    int(r["importance_rank"]),
                    run_id,
                ))
            if rows:
                cur.executemany(
                    """
                    INSERT INTO ml_feature_importance (
                      profile_id, dt, model_name, model_version, feature_schema_version, feature_name,
                      coefficient, abs_coefficient, importance_mean, importance_std, importance_rank, run_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      feature_schema_version=VALUES(feature_schema_version),
                      coefficient=VALUES(coefficient),
                      abs_coefficient=VALUES(abs_coefficient),
                      importance_mean=VALUES(importance_mean),
                      importance_std=VALUES(importance_std),
                      importance_rank=VALUES(importance_rank),
                      run_id=VALUES(run_id)
                    """,
                    rows,
                )
        conn.commit()
        print(f"[OK] loaded feature importance rows={len(rows)} run_id={run_id}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
