INSERT INTO dim_time
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT,
    d,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d, 'Month'),
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d),
    TO_CHAR(d, 'Day')
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    INTERVAL '1 day'
) AS d
ON CONFLICT DO NOTHING;