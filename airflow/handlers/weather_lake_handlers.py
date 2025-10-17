import requests
import hashlib
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

weatherlake_bucket_name = "weather-lake"

def get_ingestion_configs_handler():
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="weather_lake")

    with pg_hook.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as curs:
            curs.execute("""
                SELECT *
                FROM weather_lake.weather_lake_ingestion_config;
            """)
            ingestion_configs = curs.fetchall()

    return ingestion_configs

def get_forecast_data_handler(ingestion_config):
    data_fields = ",".join([
        key for key, val in ingestion_config.items() if val is True and key not in [
            "id", "location_name", "latitude", "longitude"
        ]
    ])
    params = {
        "location_name": ingestion_config["location_name"],
        "latitude": ingestion_config["latitude"],
        "longitude": ingestion_config["longitude"],
        "hourly": data_fields,
        "timezone": "GMT",
        "forecast_days": 16
    }
    
    capture_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
    temp_file_dir="/opt/airflow/temp/"
    forecast_file_name = f"openmeteo-16day-hourly-forecast_{ingestion_config["location_name"]}_{capture_timestamp}.json"
    forecast_file_path = temp_file_dir + forecast_file_name

    hasher=hashlib.sha256()
    openmeteo_endpoint = "https://api.open-meteo.com/v1/forecast"
    with requests.get(openmeteo_endpoint, params=params, stream=True, timeout=60) as req:
        req.raise_for_status()
        with open(forecast_file_path, "wb") as temp_file:
            for chunk in req.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                hasher.update(chunk)
                temp_file.write(chunk)

    forecast_file_digest = hasher.hexdigest()
    
    return {"forecast_file_path": forecast_file_path, "forecast_file_digest": forecast_file_digest}

def check_forecast_data_newness_handler(forecast_file_path, forecast_file_digest):
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="weather_lake")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            curs.execute(f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM weather_lake.weather_lake_ingestion_log
                    WHERE file_digest = '{forecast_file_digest}'
                    AND meta_created_at >= now() - interval '24 hours'
                );
            """)
            duplicate_forecast = curs.fetchone()[0]

            if duplicate_forecast is True:
                raise AirflowSkipException("No new data for this ETL config. Skipping...")

    return {"forecast_file_path": forecast_file_path, "forecast_file_digest": forecast_file_digest}

def archive_raw_forecast_data_handler(forecast_file_path, forecast_file_digest):
    forecast_file_name = Path(forecast_file_path).name.removesuffix(".json")
    forecast_name, location_name, capture_timestamp = forecast_file_name.split("_")
    dt = datetime.strptime(capture_timestamp, "%Y%m%d%H%M")

    forecast_object_key = (
        "raw/"
        f"location_name={location_name}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"{forecast_name}.json"
    )
    
    minio_hook = S3Hook(aws_conn_id="minio")
    minio_hook.load_file(
        filename=forecast_file_path,
        key=forecast_object_key,
        bucket_name=weatherlake_bucket_name,
        replace=True
    )
    
    # forecast_file_path = Path(temp_file_path).name
    # curs.execute(f"""
    #     INSERT INTO weather_lake.weather_lake_ingestion_log (file_name, file_digest)
    #     VALUES (%s, %s);
    # """, (forecast_file_path, forecast_file_digest))

    return forecast_object_key

def process_forecast_data_handler(forecast_object_keys):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("spark-submit", username="spark", password="spark")

    forecast_object_keys = ",".join(forecast_object_keys)

    forecast_object_keys_str = ",".join(forecast_object_keys)
    spark_command = (
        "bash -lc '"
        "/opt/spark/bin/spark-submit "
        "--master spark://spark-master:7077 "
        "--conf spark.executor.instances=1 "
        "/opt/spark/apps/weather-lake-load.py "
        f"--weatherlake-bucket-name {weatherlake_bucket_name} "
        f"--forecast-object-keys {forecast_object_keys}'"
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