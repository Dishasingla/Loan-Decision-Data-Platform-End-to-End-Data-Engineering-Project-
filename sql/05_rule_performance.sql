WITH latest_decisions AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id
               ORDER BY decision_version DESC
           ) AS rn
    FROM fact_loan_decisions
)

SELECT
    r.rule_name,
    r.rule_version,
    COUNT(*) FILTER (WHERE f.loan_status = 'Y') AS approved,
    COUNT(*) FILTER (WHERE f.loan_status = 'N') AS rejected,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') * 100.0 /
        COUNT(*),
        2
    ) AS approval_rate,
    RANK() OVER (
        ORDER BY
        COUNT(*) FILTER (WHERE f.loan_status = 'Y') DESC
    ) AS rule_rank
FROM latest_decisions f
JOIN dim_rules r
ON f.rule_id = r.rule_id
WHERE rn = 1
GROUP BY r.rule_name, r.rule_version;