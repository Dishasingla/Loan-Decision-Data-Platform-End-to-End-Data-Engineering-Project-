INSERT INTO dim_property(property_area)
SELECT DISTINCT property_area
FROM fact_loan_decisions
WHERE property_area IS NOT NULL
ON CONFLICT(property_area) DO NOTHING;