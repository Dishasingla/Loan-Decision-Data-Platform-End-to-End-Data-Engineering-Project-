DROP TABLE IF EXISTS dim_credit;

CREATE TABLE dim_credit AS
SELECT DISTINCT
    credit_history AS credit_key,
    credit_history
FROM raw_loan_applications;