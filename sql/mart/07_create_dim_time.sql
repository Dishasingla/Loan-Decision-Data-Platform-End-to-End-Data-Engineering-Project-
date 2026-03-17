CREATE TABLE IF NOT EXISTS dim_time (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name TEXT,
    day INT,
    day_of_week INT,
    day_name TEXT
);