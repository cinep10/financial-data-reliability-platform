#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
import joblib, pandas as pd, pymysql
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

def connect(args):
    return pymysql.connect(
        host=args.host, port=args.port, user=args.user, password=args.password,
        database=args.db, charset='utf8mb4'
    )

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
    ap.add_argument('--view-name', default='vw_stream_ml_training_dataset_v3')
    ap.add_argument('--output-dir', default='artifacts/ml_v2')
    args = ap.parse_args()

    conn = connect(args)
    df = pd.read_sql(f"SELECT * FROM {args.view_name} WHERE profile_id=%s AND dt BETWEEN %s AND %s",
                     conn, params=[args.profile_id, args.dt_from, args.dt_to])
    conn.close()

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    num_cols = [
        'missing_rate','duplicate_ratio','ordering_gap_score','avg_event_delay_ms',
        'stream_risk_score','delay_diff','risk_diff','missing_diff','duplicate_diff',
        'delay_ma_7d','risk_ma_7d','delay_std_7d'
    ]
    cat_cols = ['service_domain','prev_issue']

    X = df[num_cols + cat_cols].copy()
    y = df['target_risk_score'].fillna(10.0).astype(float)

    pre = ColumnTransformer([
        ('num', Pipeline([('imp', SimpleImputer(strategy='constant', fill_value=0.0))]), num_cols),
        ('cat', Pipeline([
            ('imp', SimpleImputer(strategy='constant', fill_value='none')),
            ('oh', OneHotEncoder(handle_unknown='ignore'))
        ]), cat_cols)
    ])

    model = Pipeline([
        ('pre', pre),
        ('reg', RandomForestRegressor(
            n_estimators=400,
            max_depth=8,
            min_samples_leaf=2,
            random_state=42
        ))
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    metrics = {
        'mae': float(mean_absolute_error(y_test, pred)),
        'r2': float(r2_score(y_test, pred))
    }

    joblib.dump(model, outdir / 'stream_risk_regressor_v1.joblib')
    (outdir / 'regression_metrics_v1.json').write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding='utf-8')
    pd.DataFrame({'actual': y_test.values, 'pred': pred}).to_csv(outdir / 'regression_predictions_v1.csv', index=False)
    print('[train_stream_risk_regressor_v1] done')

if __name__ == '__main__':
    main()
