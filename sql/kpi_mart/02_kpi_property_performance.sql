DROP MATERIALIZED VIEW IF EXISTS kpi_property_performance CASCADE;

CREATE MATERIALIZED VIEW kpi_property_performance AS
SELECT
    dp.property_area,
    COUNT(*) AS total_applications,
    COUNT(*) FILTER (WHERE f.loan_status = 'Y') AS approvals,
    COUNT(*) FILTER (WHERE f.loan_status = 'N') AS rejections,
    ROUND(
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') * 100.0 / COUNT(*),
        2
    ) AS approval_rate
FROM mart_loan_decision f
JOIN dim_property dp
    ON f.property_key = dp.property_key
GROUP BY dp.property_area;