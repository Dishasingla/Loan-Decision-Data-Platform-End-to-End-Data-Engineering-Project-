DROP MATERIALIZED VIEW IF EXISTS kpi_credit_performance CASCADE;

CREATE MATERIALIZED VIEW kpi_credit_performance AS
SELECT
    dc.credit_history,
    COUNT(*) AS total_applications,
    COUNT(*) FILTER (WHERE f.loan_status = 'Y') AS approvals,
    COUNT(*) FILTER (WHERE f.loan_status = 'N') AS rejections,
    ROUND(
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') * 100.0 / COUNT(*),
        2
    ) AS approval_rate
FROM mart_loan_decision f
JOIN dim_credit dc
    ON f.credit_key = dc.credit_key
GROUP BY dc.credit_history;