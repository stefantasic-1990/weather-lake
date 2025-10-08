-- Create Airflow user and database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- Create Hive user and metastore database
CREATE USER hive WITH PASSWORD 'hivepass';
CREATE DATABASE metastore OWNER hive;