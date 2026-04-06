CREATE TABLE IF NOT EXISTS ai_incident_summary_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    risk_score DECIMAL(20,6) NULL,
    actual_risk_status VARCHAR(20) NULL,
    predicted_risk_status VARCHAR(20) NULL,
    predicted_alert_prob DECIMAL(20,6) NULL,
    incident_title VARCHAR(255) NULL,
    incident_level VARCHAR(20) NULL,
    executive_summary TEXT NULL,
    technical_summary TEXT NULL,
    business_impact TEXT NULL,
    recommended_actions TEXT NULL,
    confidence_score DECIMAL(20,6) NULL,
    llm_model VARCHAR(100) NULL,
    prompt_version VARCHAR(50) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ai_recommended_action_day (
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    action_rank INT NOT NULL,
    action_type VARCHAR(50) NULL,
    action_title VARCHAR(255) NULL,
    action_detail TEXT NULL,
    owner_hint VARCHAR(100) NULL,
    priority VARCHAR(20) NULL,
    evidence TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, dt, action_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ai_prompt_log (
    run_id VARCHAR(64) NOT NULL,
    profile_id VARCHAR(64) NOT NULL,
    dt DATE NOT NULL,
    prompt_version VARCHAR(50) NOT NULL,
    llm_model VARCHAR(100) NOT NULL,
    prompt_text LONGTEXT NULL,
    response_text LONGTEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
