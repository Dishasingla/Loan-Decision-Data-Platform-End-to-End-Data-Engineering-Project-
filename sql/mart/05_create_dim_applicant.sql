CREATE TABLE IF NOT EXISTS dim_applicant (
    applicant_key SERIAL PRIMARY KEY,
    application_id TEXT UNIQUE,
    applicant_income FLOAT,
    coapplicant_income FLOAT,
    credit_history INT,
    created_at TIMESTAMP DEFAULT NOW()
);