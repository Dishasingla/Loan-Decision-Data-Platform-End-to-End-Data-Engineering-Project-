DROP TABLE IF EXISTS mart_loan_decision CASCADE;

CREATE TABLE mart_loan_decision AS
SELECT

    f.application_id,

    da.applicant_key,
    dp.property_key,
    dr.rule_id AS rule_key,
    dt.date_key,
    dc.credit_key,
    f.loan_amount,
    f.loan_term,
    f.loan_status

FROM fact_loan_decisions f

JOIN dim_applicant da
    ON f.application_id = da.application_id

JOIN dim_property dp
    ON f.property_area = dp.property_area

JOIN dim_rule dr
    ON f.rule_id = dr.rule_id

JOIN dim_credit dc
    ON f.credit_history = dc.credit_history

JOIN dim_time dt
    ON DATE(f.ingestion_timestamp) = dt.full_date

WHERE f.is_current = TRUE;