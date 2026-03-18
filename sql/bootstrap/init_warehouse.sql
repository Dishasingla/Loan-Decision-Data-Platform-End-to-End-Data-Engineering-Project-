-- =========================
-- RAW LAYER
-- =========================

CREATE TABLE IF NOT EXISTS raw_loan_applications (
    raw_id BIGSERIAL PRIMARY KEY,
    loan_id TEXT,
    gender TEXT,
    married TEXT,
    dependents TEXT,
    education TEXT,
    self_employed TEXT,
    applicant_income BIGINT,
    coapplicant_income BIGINT,
    loan_amount NUMERIC,
    loan_amount_term INT,
    credit_history INT,
    property_area TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- FACT TABLE (SCD2)
-- =========================

DROP TABLE IF EXISTS fact_loan_decisions;

CREATE TABLE fact_loan_decisions (
    decision_id BIGSERIAL PRIMARY KEY,
    application_id TEXT,

    applicant_income BIGINT,
    coapplicant_income BIGINT,
    loan_amount NUMERIC,
    loan_term INT,
    credit_history INT,
    property_area TEXT,

    loan_status TEXT,

    decision_version INT,
    rule_id INT,

    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =========================
-- TEMPORAL SUPPORT
-- =========================

ALTER TABLE fact_loan_decisions
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE IF NOT EXISTS dim_property (
    property_key SERIAL PRIMARY KEY,
    property_area TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_credit (
    credit_key SERIAL PRIMARY KEY,
    credit_history INT UNIQUE
);