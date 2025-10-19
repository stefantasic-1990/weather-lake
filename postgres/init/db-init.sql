-- Create Airflow user and database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Create Hive user and metastore database
CREATE USER hive WITH PASSWORD 'hivepass';
CREATE DATABASE metastore OWNER hive;

-- Create Supserset user and metastore database
CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset OWNER superset;

-- Create ETL schema, grant usage to Airflow
CREATE SCHEMA weather_lake AUTHORIZATION postgres;
GRANT USAGE ON SCHEMA weather_lake TO airflow;

-- Create ETL config table, grant read to Airflow
CREATE TABLE weather_lake.weather_lake_ingestion_config (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    location_name TEXT NOT NULL UNIQUE,
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
GRANT SELECT ON weather_lake.weather_lake_ingestion_config TO airflow;

-- Insert records into ETL config table
INSERT INTO weather_lake.weather_lake_ingestion_config (
    location_name,
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

-- Create ETL data file log table, grant read and insert to Airflow
CREATE TABLE weather_lake.weather_lake_ingestion_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name TEXT NOT NULL UNIQUE,
    file_digest TEXT NOT NULL UNIQUE,
    meta_created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT ON weather_lake.weather_lake_ingestion_log TO airflow;