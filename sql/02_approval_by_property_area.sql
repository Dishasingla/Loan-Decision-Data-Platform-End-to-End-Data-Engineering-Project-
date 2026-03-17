WITH latest_decisions AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id
               ORDER BY decision_version DESC
           ) AS rn
    FROM fact_loan_decisions
)

SELECT
    property_area,
    COUNT(*) FILTER (WHERE loan_status = 'Y') AS approved,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) FILTER (WHERE loan_status = 'Y') * 100.0 /
        COUNT(*),
        2
    ) AS approval_rate,
    RANK() OVER (
        ORDER BY
        COUNT(*) FILTER (WHERE loan_status = 'Y') DESC
    ) AS approval_rank
FROM latest_decisions
WHERE rn = 1
GROUP BY property_area;