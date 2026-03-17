DROP MATERIALIZED VIEW IF EXISTS kpi_total_approvals CASCADE;

CREATE MATERIALIZED VIEW kpi_total_approvals AS
SELECT
    COUNT(*) FILTER (WHERE loan_status = 'Y') AS total_approved,
    COUNT(*) FILTER (WHERE loan_status = 'N') AS total_rejected,
    COUNT(*) AS total_applications,
    ROUND(
        COUNT(*) FILTER (WHERE loan_status = 'Y') * 100.0 / COUNT(*),
        2
    ) AS approval_rate_pct
FROM mart_loan_decision;