-- Create Airflow user and database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Create Hive user and metastore database
CREATE USER hive WITH PASSWORD 'hivepass';
CREATE DATABASE metastore OWNER hive;

-- Create pipeline ingestion config table
CREATE SCHEMA weather_lake AUTHORIZATION postgres;
CREATE TABLE weather_lake.weather_lake_ingestion_config (
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
    meta_valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta_valid_to TIMESTAMPTZ,
    meta_current_flag BOOLEAN NOT NULL DEFAULT true
)

-- Insert default records into ingestion config table
INSERT INTO weather_lake.weather_lake_ingestion_config (
    dataset_name,
    latitude,
    longitude,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    wind_direction_10m,
    pressure_msl
) VALUES (
    "Toronto",
    ""
)