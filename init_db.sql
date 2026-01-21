CREATE TABLE water_quality_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ph FLOAT NOT NULL CHECK (ph BETWEEN 0 AND 14),
    dissolved_oxygen FLOAT NOT NULL CHECK (dissolved_oxygen >= 0),
    water_temperature FLOAT NOT NULL CHECK (water_temperature BETWEEN -10 AND 40),
    turbidity FLOAT NOT NULL CHECK (turbidity >= 0)
);