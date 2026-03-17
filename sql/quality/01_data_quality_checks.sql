-- Null application id check
SELECT COUNT(*) AS null_application_ids
FROM raw_loan_applications
WHERE loan_id IS NULL;

-- Duplicate applications check
SELECT loan_id, COUNT(*)
FROM raw_loan_applications
GROUP BY loan_id
HAVING COUNT(*) > 1;

-- Negative loan amount check
SELECT COUNT(*)
FROM raw_loan_applications
WHERE loan_amount < 0;

-- Fact table version integrity
SELECT application_id, COUNT(*)
FROM fact_loan_decisions
GROUP BY application_id, decision_version
HAVING COUNT(*) > 1;