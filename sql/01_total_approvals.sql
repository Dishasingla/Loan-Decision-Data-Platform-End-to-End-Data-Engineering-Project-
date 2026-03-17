WITH latest_decisions AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id
               ORDER BY decision_version DESC
           ) AS rn
    FROM fact_loan_decisions
)

SELECT
    loan_status,
    COUNT(*) AS total_loans,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM latest_decisions
WHERE rn = 1
GROUP BY loan_status;