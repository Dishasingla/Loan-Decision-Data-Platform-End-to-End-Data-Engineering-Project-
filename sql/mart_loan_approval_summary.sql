CREATE MATERIALIZED VIEW mart_loan_approval_summary AS
SELECT
    property_area,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status='Y' THEN 1 ELSE 0 END) AS approvals,
    ROUND(
        SUM(CASE WHEN loan_status='Y' THEN 1 ELSE 0 END)::numeric
        / COUNT(*) * 100, 2
    ) AS approval_rate
FROM fact_loan_decisions
WHERE is_current = TRUE
GROUP BY property_area;