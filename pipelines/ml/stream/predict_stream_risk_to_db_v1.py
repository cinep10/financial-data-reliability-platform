#!/usr/bin/env python3
from __future__ import annotations

import argparse
import urllib.parse

import joblib
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

MODEL_VERSION = "stream_risk_regressor_v1"


def make_engine(args):
    pw = urllib.parse.quote_plus(args.password)
    url = (
        f"mysql+pymysql://{args.user}:{pw}"
        f"@{args.host}:{args.port}/{args.db}?charset=utf8mb4"
    )
    return create_engine(url)


def make_write_conn(args):
    return pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.db,
        charset="utf8mb4",
        autocommit=False,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--model-path", required=True)
    ap.add_argument("--view-name", default="vw_stream_ml_training_dataset_v4")
    args = ap.parse_args()

    engine = make_engine(args)

    query = text(f"""
        SELECT *
        FROM {args.view_name}
        WHERE profile_id = :profile_id
          AND dt BETWEEN :dt_from AND :dt_to
        ORDER BY dt, service_domain
    """)

    df = pd.read_sql(
        query,
        engine,
        params={
            "profile_id": args.profile_id,
            "dt_from": args.dt_from,
            "dt_to": args.dt_to,
        },
    )

    if df.empty:
        raise ValueError("Prediction source dataframe is empty")

    print("[DEBUG] risk first source rows:")
    print(df[["profile_id", "dt", "service_domain"]].head(5))

    # 회귀 모델도 학습 때 사용한 전체 컬럼을 그대로 맞춘다
    feature_cols_num = [
        "missing_rate",
        "duplicate_ratio",
        "ordering_gap_score",
        "avg_event_delay_ms",
        "stream_risk_score",
        "delay_diff",
        "risk_diff",
        "missing_diff",
        "duplicate_diff",
        "delay_ma_7d",
        "risk_ma_7d",
        "delay_std_7d",
        "dayofweek",
        "month_no",
        "is_weekend",
        "missing_x_delay",
        "duplicate_x_ordering",
    ]
    feature_cols_cat = ["service_domain", "prev_issue"]

    required_cols = feature_cols_num + feature_cols_cat + ["profile_id", "dt"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {args.view_name}: {missing_cols}")

    for c in feature_cols_num:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    for c in feature_cols_cat:
        df[c] = df[c].fillna("none").astype(str)

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    if df["dt"].isna().any():
        bad = df.loc[df["dt"].isna(), ["profile_id", "service_domain"]].head(10)
        raise ValueError(f"Invalid dt values found:\n{bad}")

    X = df[feature_cols_num + feature_cols_cat].copy()

    print("[DEBUG] risk X columns =", X.columns.tolist())

    model = joblib.load(args.model_path)
    preds = model.predict(X)

    rows = list(
        zip(
            df["profile_id"].astype(str).tolist(),
            df["dt"].dt.strftime("%Y-%m-%d").tolist(),
            df["service_domain"].astype(str).tolist(),
            pd.Series(preds).astype(float).tolist(),
            [MODEL_VERSION] * len(df),
        )
    )

    print("[DEBUG] risk sample rows =", rows[:3])

    conn = make_write_conn(args)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stream_ml_risk_prediction_day (
                    profile_id VARCHAR(64) NOT NULL,
                    dt DATE NOT NULL,
                    service_domain VARCHAR(50) NOT NULL,
                    predicted_risk_score DOUBLE NOT NULL,
                    model_version VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (profile_id, dt, service_domain, model_version)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cur.execute("""
                DELETE FROM stream_ml_risk_prediction_day
                WHERE profile_id=%s
                  AND dt BETWEEN %s AND %s
                  AND model_version=%s
            """, (args.profile_id, args.dt_from, args.dt_to, MODEL_VERSION))

            cur.executemany("""
                INSERT INTO stream_ml_risk_prediction_day
                (profile_id, dt, service_domain, predicted_risk_score, model_version)
                VALUES (%s, %s, %s, %s, %s)
            """, rows)

        conn.commit()
        print(f"[predict_stream_risk_to_db_v1] done rows={len(rows)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
