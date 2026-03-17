WITH latest_decisions AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id
               ORDER BY decision_version DESC
           ) AS rn
    FROM fact_loan_decisions
),

income_groups AS (
    SELECT *,
        CASE
            WHEN applicant_income < 3000 THEN 'Low income'
            WHEN applicant_income BETWEEN 3000 AND 6000 THEN 'Medium income'
            ELSE 'High income'
        END AS income_category
    FROM latest_decisions
    WHERE rn = 1
)

SELECT
    income_category,
    COUNT(*) FILTER (WHERE loan_status = 'Y') AS approved,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) FILTER (WHERE loan_status = 'Y') * 100.0 /
        COUNT(*),
        2
    ) AS approval_rate,
    DENSE_RANK() OVER (
        ORDER BY
        COUNT(*) FILTER (WHERE loan_status = 'Y') DESC
    ) AS income_rank
FROM income_groups
GROUP BY income_category;