DROP MATERIALIZED VIEW IF EXISTS kpi_rule_performance;

CREATE MATERIALIZED VIEW kpi_rule_performance AS
SELECT
    dr.rule_name,
    COUNT(*) FILTER (WHERE f.loan_status = 'Y') AS approvals,
    COUNT(*) FILTER (WHERE f.loan_status = 'N') AS rejections,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') * 100.0 / COUNT(*),
        2
    ) AS approval_rate_pct
FROM mart_loan_decision f
JOIN dim_rule dr
    ON f.rule_key = dr.rule_id
GROUP BY dr.rule_name;