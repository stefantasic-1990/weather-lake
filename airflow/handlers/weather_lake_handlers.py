import requests
import hashlib
import json
import paramiko
import logging
from time import sleep
from pathlib import Path
from datetime import datetime
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.exceptions import AirflowSkipException, AirflowFailException

AWS_CONN_ID = "minio"
POSTGRES_CONN_ID = "postgres"
POSTGRES_SCHEMA = "weather_lake"
DATA_LAKE_BUCKET = "weather-lake"
TMP_FILE_DIR = "/opt/airflow/temp/"
API_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

def get_ingestion_configs_handler():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                SELECT 
                    location_name,
                    latitude,
                    longitude,
                    temperature_2m,
                    relative_humidity_2m,
                    precipitation,
                    wind_speed_10m,
                    wind_direction_10m,
                    pressure_msl
                FROM {POSTGRES_SCHEMA}.ingestion_config;
            """)
            ingestion_configs = cursor.fetchall()

    return ingestion_configs

def get_forecast_data_handler(ingestion_config):
    data_fields = ",".join(
        key for key, val in ingestion_config.items()
        if val and key not in ["location_name", "latitude", "longitude"]
    )
    params = {
        "location_name": ingestion_config["location_name"],
        "latitude": ingestion_config["latitude"],
        "longitude": ingestion_config["longitude"],
        "hourly": data_fields,
        "timezone": "GMT",
        "forecast_days": 16
    }
    
    capture_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
    file_name = f"openmeteo-16day-hourly-forecast_{ingestion_config["location_name"]}_{capture_timestamp}.json"
    file_path = TMP_FILE_DIR + file_name

    openmeteo_endpoint = "https://api.open-meteo.com/v1/forecast"
    response = requests.get(API_ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    data.pop("generationtime_ms", None)
    with open(file_path, "w") as temp_file:
        json.dump(data, temp_file)

    file_digest = hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    
    return {"file_path": file_path, "file_digest": file_digest}

def check_forecast_data_newness_handler(file_path, file_digest):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            curs.execute(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM {POSTGRES_SCHEMA}.ingestion_log
                    WHERE file_digest = '{file_digest}'
                    AND meta_created_at >= now() - interval '24 hours'
                );
            """)
            duplicate_forecast = curs.fetchone()[0]

            if duplicate_forecast is True:
                raise AirflowSkipException("No new data for this ingestion config. Skipping...")

    return {"file_path": file_path, "file_digest": file_digest}

def archive_raw_forecast_data_handler(file_path, file_digest):
    file_name = Path(file_path).name
    _, location_name, capture_timestamp = file_name.removesuffix(".json").split("_")
    dt = datetime.strptime(capture_timestamp, "%Y%m%d%H%M")
    object_key = (
        "forecast_raw/"
        f"location_name={location_name}/"
        f"capture_year={dt.year}/"
        f"capture_month={dt.month:02d}/"
        f"capture_day={dt.day:02d}/"
        f"capture_hour={dt.hour:02d}/"
        f"capture_minute={dt.minute:02d}/"
        f"{file_name}"
    )
    
    minio_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    minio_hook.load_file(
        filename=file_path,
        key=object_key,
        bucket_name=DATA_LAKE_BUCKET,
        replace=True
    )

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            curs.execute(f"""
                INSERT INTO {POSTGRES_SCHEMA}.weather_lake_ingestion_log (file_name, file_digest)
                VALUES ('{file_name}', '{file_digest}');
            """)

    return object_key

def process_forecast_data_handler(object_keys):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("spark-submit", username="spark", password="spark")

    object_keys = ",".join(object_keys)

    object_keys_str = ",".join(object_keys)
    spark_command = (
        "bash -lc '"
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--conf spark.executor.instances=1 "
        "/opt/spark/apps/weather-lake-load.py "
        f"--weatherlake-bucket-name {weatherlake_bucket_name} "
        f"--forecast-object-keys {object_keys}'"
    )

    logging.info("Executing Spark job...")
    stdin, stdout, stderr = ssh.exec_command(spark_command)
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            chunk = stdout.channel.recv(4096).decode("utf-8")
            logging.info(chunk)

        if stderr.channel.recv_stderr_ready():
            error_chunk = stderr.channel.recv_stderr(4096).decode("utf-8")
            logging.info(error_chunk)

        sleep(0.5)

    exit_code = stdout.channel.recv_exit_status()
    logging.info(stdout.read().decode("utf-8"))
    logging.info(stderr.read().decode("utf-8"))
    ssh.close()

    if exit_code != 0:
        raise AirflowFailException(f"Spark job failed to execute.")