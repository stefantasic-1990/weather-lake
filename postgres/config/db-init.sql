-- Create Airflow user and database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Create Hive user and metastore database
CREATE USER hive WITH PASSWORD 'hivepass';
CREATE DATABASE metastore OWNER hive;

-- Create ETL config table
CREATE SCHEMA weather_lake AUTHORIZATION postgres;
CREATE TABLE weather_lake.weather_lake_etl_config (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_name TEXT NOT NULL UNIQUE,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    temperature_2m BOOLEAN NOT NULL,
    relative_humidity_2m BOOLEAN NOT NULL,
    precipitation BOOLEAN NOT NULL,
    wind_speed_10m BOOLEAN NOT NULL,
    wind_direction_10m BOOLEAN NOT NULL,
    pressure_msl BOOLEAN NOT NULL,
    meta_created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Grant permissions to Airflow
GRANT USAGE ON SCHEMA weather_lake TO airflow;
GRANT SELECT ON weather_lake.weather_lake_etl_config TO airflow;

-- Insert default records into ETL config table
INSERT INTO weather_lake.weather_lake_etl_config (
    dataset_name,
    latitude,
    longitude,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    wind_direction_10m,
    pressure_msl
)
VALUES 
    ('Toronto', 43.7064, -79.3986, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
    ('Vancouver', 49.2497, -123.1207, TRUE, TRUE, TRUE, FALSE, FALSE, TRUE),
    ('Calgary', 51.0501, -114.0853, TRUE, TRUE, TRUE, TRUE, FALSE, TRUE);