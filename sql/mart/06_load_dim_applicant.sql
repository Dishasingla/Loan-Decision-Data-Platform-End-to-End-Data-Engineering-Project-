INSERT INTO dim_applicant (
    application_id,
    applicant_income,
    coapplicant_income,
    credit_history
)
SELECT DISTINCT
    application_id,
    applicant_income,
    coapplicant_income,
    credit_history
FROM fact_loan_decisions
ON CONFLICT (application_id) DO NOTHING;