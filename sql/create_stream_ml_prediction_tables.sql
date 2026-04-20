CREATE TABLE IF NOT EXISTS stream_ml_prediction_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    predicted_label VARCHAR(20) NOT NULL,
    anomaly_prob DOUBLE NULL,
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, service_domain, model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stream_ml_risk_prediction_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    service_domain VARCHAR(50) NOT NULL,
    predicted_risk_score DOUBLE NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, service_domain, model_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
