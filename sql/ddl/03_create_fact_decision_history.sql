CREATE TABLE IF NOT EXISTS fact_loan_decision_history (
    decision_sk SERIAL PRIMARY KEY,
    application_id TEXT,
    decision_version INT,
    loan_status CHAR(1),
    applicant_income NUMERIC,
    loan_amount NUMERIC,
    loan_term INT,
    credit_history INT,
    property_area TEXT,
    rule_id INT,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);