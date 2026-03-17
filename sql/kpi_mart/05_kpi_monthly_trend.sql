DROP MATERIALIZED VIEW IF EXISTS kpi_monthly_trend;

CREATE MATERIALIZED VIEW kpi_monthly_trend AS
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    COUNT(*) FILTER (WHERE f.loan_status = 'Y') AS approvals,
    COUNT(*) FILTER (WHERE f.loan_status = 'N') AS rejections,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') * 100.0 / COUNT(*),
        2
    ) AS approval_rate_pct
FROM mart_loan_decision f
JOIN dim_time dt
    ON f.date_key = dt.date_key
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;