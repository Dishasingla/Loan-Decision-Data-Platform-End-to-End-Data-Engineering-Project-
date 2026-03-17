CREATE TABLE IF NOT EXISTS dim_rule (
    rule_id INT PRIMARY KEY,
    rule_name TEXT,
    rule_description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);