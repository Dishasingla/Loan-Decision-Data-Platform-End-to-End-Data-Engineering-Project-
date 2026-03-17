CREATE TABLE IF NOT EXISTS dim_property (
    property_key SERIAL PRIMARY KEY,
    property_area TEXT UNIQUE
);