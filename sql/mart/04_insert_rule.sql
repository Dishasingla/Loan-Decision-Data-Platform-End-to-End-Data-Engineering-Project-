INSERT INTO dim_rule (rule_id, rule_name, rule_description)
VALUES (
    1,
    'Income + Loan Threshold Rule',
    'Approve if credit history good, income >= 4000, loan <= 250'
)
ON CONFLICT (rule_id) DO NOTHING;