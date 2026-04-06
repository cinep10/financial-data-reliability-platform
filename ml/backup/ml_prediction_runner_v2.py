from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
import joblib
import pandas as pd
import pymysql


def connect_mysql(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def date_range(start: str, end: str):
    dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while dt <= end_dt:
        yield dt.strftime("%Y-%m-%d")
        dt += timedelta(days=1)


def get_table_columns(cur, table_name: str) -> set[str]:
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    return {row["Field"] for row in cur.fetchall()}


def ensure_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_prediction_result (
          profile_id VARCHAR(64) NOT NULL,
          dt DATE NOT NULL,
          model_name VARCHAR(100) NOT NULL,
          model_version VARCHAR(64) NOT NULL,
          feature_schema_version VARCHAR(20) NULL,
          predicted_label TINYINT NULL,
          predicted_risk_status VARCHAR(20) NOT NULL,
          prob_not_alert DECIMAL(20,6) NULL,
          prob_alert DECIMAL(20,6) NULL,
          actual_risk_status VARCHAR(20) NULL,
          actual_risk_score DECIMAL(20,6) NULL,
          top_reason_1 VARCHAR(255) NULL,
          top_reason_2 VARCHAR(255) NULL,
          top_reason_3 VARCHAR(255) NULL,
          run_id VARCHAR(64) NULL,
          note VARCHAR(255) NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (profile_id, dt, model_name, model_version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def ensure_optional_columns(cur) -> None:
    cols = get_table_columns(cur, "ml_prediction_result")
    alters = {
        "feature_schema_version": "ALTER TABLE ml_prediction_result ADD COLUMN feature_schema_version VARCHAR(20) NULL AFTER model_version",
        "top_reason_1": "ALTER TABLE ml_prediction_result ADD COLUMN top_reason_1 VARCHAR(255) NULL AFTER actual_risk_score",
        "top_reason_2": "ALTER TABLE ml_prediction_result ADD COLUMN top_reason_2 VARCHAR(255) NULL AFTER top_reason_1",
        "top_reason_3": "ALTER TABLE ml_prediction_result ADD COLUMN top_reason_3 VARCHAR(255) NULL AFTER top_reason_2",
    }
    for col, ddl in alters.items():
        if col not in cols:
            try:
                cur.execute(ddl)
            except Exception:
                pass


def load_row(cur, profile_id: str, dt: str):
    cur.execute("SELECT * FROM ml_feature_vector_day WHERE profile_id=%s AND dt=%s", (profile_id, dt))
    return cur.fetchone()


def label_to_status(label: int) -> str:
    return "alert" if int(label) == 1 else "not_alert"


def build_reasons(artifact: dict, row: dict) -> tuple[str | None, str | None, str | None]:
    model = artifact["model"]
    feature_cols = artifact["feature_columns"]
    X = pd.DataFrame([{c: row.get(c, 0) for c in feature_cols}]).apply(pd.to_numeric, errors="coerce").fillna(0)
    transformed = model.named_steps["preprocessor"].transform(X)
    clf = model.named_steps["clf"]
    coefs = clf.coef_[0]
    contrib = transformed[0] * coefs
    pairs = sorted(zip(feature_cols, contrib), key=lambda x: abs(float(x[1])), reverse=True)[:3]
    reasons = []
    for feat, val in pairs:
        direction = "increase" if float(val) >= 0 else "decrease"
        reasons.append(f"{feat}:{direction}:{float(val):.4f}")
    while len(reasons) < 3:
        reasons.append(None)
    return reasons[0], reasons[1], reasons[2]


def main() -> None:
    ap = argparse.ArgumentParser(description="Run ML risk prediction v2 with schema check and explanations")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", default="")
    ap.add_argument("--db", required=True)
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--date")
    ap.add_argument("--dt-from")
    ap.add_argument("--dt-to")
    ap.add_argument("--model-path", default="ml_risk_model_v2.joblib")
    ap.add_argument("--model-name", default="logistic_risk_classifier")
    ap.add_argument("--model-version", default="ml_risk_v2")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    if args.date:
        dates = [args.date]
    elif args.dt_from and args.dt_to:
        dates = list(date_range(args.dt_from, args.dt_to))
    else:
        raise ValueError("Provide --date or --dt-from/--dt-to")

    artifact = joblib.load(args.model_path)
    model = artifact["model"]
    feature_cols = artifact["feature_columns"]
    feature_schema_version = artifact.get("feature_schema_version", "unknown")
    run_id = f"mlpred2_{args.profile_id}_{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.db)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            ensure_optional_columns(cur)
            table_cols = get_table_columns(cur, "ml_prediction_result")
            if args.truncate:
                cur.execute(
                    """
                    DELETE FROM ml_prediction_result
                    WHERE profile_id=%s AND dt BETWEEN %s AND %s AND model_name=%s AND model_version=%s
                    """,
                    (args.profile_id, dates[0], dates[-1], args.model_name, args.model_version),
                )
            rows = []
            for dt in dates:
                row = load_row(cur, args.profile_id, dt)
                if not row:
                    continue
                missing = [c for c in feature_cols if c not in row]
                if missing:
                    raise ValueError(f"Missing features in ml_feature_vector_day: {missing}")
                X = pd.DataFrame([{c: row.get(c, 0) for c in feature_cols}]).apply(pd.to_numeric, errors="coerce").fillna(0)
                pred = int(model.predict(X)[0])
                probs = model.predict_proba(X)[0]
                prob_not_alert = float(probs[0]) if len(probs) == 2 else 1.0 - float(probs[0])
                prob_alert = float(probs[1]) if len(probs) == 2 else float(probs[0])
                reason1, reason2, reason3 = build_reasons(artifact, row)
                rows.append({
                    "profile_id": args.profile_id,
                    "dt": dt,
                    "model_name": args.model_name,
                    "model_version": args.model_version,
                    "feature_schema_version": feature_schema_version,
                    "predicted_label": pred,
                    "predicted_risk_status": label_to_status(pred),
                    "prob_not_alert": round(prob_not_alert, 6),
                    "prob_alert": round(prob_alert, 6),
                    "actual_risk_status": row.get("target_risk_grade"),
                    "actual_risk_score": row.get("target_risk_score"),
                    "top_reason_1": reason1,
                    "top_reason_2": reason2,
                    "top_reason_3": reason3,
                    "run_id": run_id,
                    "note": f"model_path={args.model_path}",
                })
            ordered_cols = [
                "profile_id","dt","model_name","model_version","feature_schema_version",
                "predicted_label","predicted_risk_status","prob_not_alert","prob_alert",
                "actual_risk_status","actual_risk_score","top_reason_1","top_reason_2","top_reason_3",
                "run_id","note",
            ]
            cols = [c for c in ordered_cols if c in table_cols]
            placeholders = ",".join(["%s"] * len(cols))
            updates = ",".join([f"{c}=VALUES({c})" for c in cols if c not in ("profile_id","dt","model_name","model_version")])
            sql = f"INSERT INTO ml_prediction_result ({','.join(cols)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
            vals = [tuple(r[c] for c in cols) for r in rows]
            if vals:
                cur.executemany(sql, vals)
        conn.commit()
        print(f"[OK] prediction completed: rows={len(rows)} run_id={run_id} schema={feature_schema_version}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
