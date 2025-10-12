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

temp_file_dir="/opt/airflow/temp/"

def get_etl_configs_handler():
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="weather_lake")

    with pg_hook.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as curs:
            curs.execute("""
                SELECT *
                FROM weather_lake.weather_lake_etl_config;
            """)
            configs = curs.fetchall()

    return configs

def download_weather_data_handler(config):
    fields = ",".join([
        key for key, val in config.items() if val is True and key not in [
            "id", "geolocation_name", "latitude", "longitude"
        ]
    ])
    params = {
        "geolocation_name": config["geolocation_name"],
        "latitude": config["latitude"],
        "longitude": config["longitude"],
        "hourly": fields,
        "timezone": "GMT",
        "forecast_days": 16,
        "format": "csv"
    }
    
    forecast_timestamp = datetime.utcnow().strftime("%Y%m%d%H%M")
    temp_file_name = f"openmeteo-16day-hourly-forecast_{config["geolocation_name"]}_{forecast_timestamp}.csv"
    temp_file_path = temp_file_dir+temp_file_name
    openmeteo_endpoint = "https://api.open-meteo.com/v1/forecast"
    print(openmeteo_endpoint)
    hasher=hashlib.sha256()

    with requests.get(openmeteo_endpoint, params=params, stream=True, timeout=60) as req:
        req.raise_for_status()

        with open(temp_file_path, "wb") as temp_file:
            for chunk in req.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                hasher.update(chunk)
                temp_file.write(chunk)

    temp_file_digest = hasher.hexdigest()
    
    return {"temp_file_path": temp_file_path, "temp_file_digest": temp_file_digest}

def check_data_newness_handler(temp_file_path, temp_file_digest):
    pg_hook = PostgresHook(postgres_conn_id="postgres", schema="weather_lake")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as curs:
            curs.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM weather_lake.weather_lake_ingestion_log
                    WHERE file_digest = %s
                    AND meta_created_at >= now() - interval '3 hours'
                );
            """, (temp_file_digest,))
            already_ingested = curs.fetchone()[0]

            # if already_ingested is True:
            #     raise AirflowSkipException("No new data for this ETL config. Skipping...")

            # temp_file_name = Path(temp_file_path).name
            # curs.execute(f"""
            #     INSERT INTO weather_lake.weather_lake_ingestion_log (file_name, file_digest)
            #     VALUES (%s, %s);
            # """, (temp_file_name, temp_file_digest))

    return temp_file_path

def archive_raw_csv_data_handler(temp_file_path):
    temp_file_name = Path(temp_file_path).name.removesuffix(".csv")
    forecast_name, geolocation_name, forecast_timestamp = temp_file_name.split("_")
    dt = datetime.strptime(forecast_timestamp, "%Y%m%d%H%M")

    archive_bucket_name = "weather-lake-raw-archive"
    object_key = (
        f"{geolocation_name}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"{forecast_name}_{dt.hour:02d}{dt.minute:02d}.csv"
    )
    
    minio_hook = S3Hook(aws_conn_id="minio")
    minio_hook.load_file(
        filename=temp_file_path,
        key=object_key,
        bucket_name=archive_bucket_name,
        replace=True
    )

    return object_key

def transform_data_to_parquet_handler(object_keys):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("spark-submit", username="spark", password="spark")

    raw_bucket = "weather-lake-raw-archive"
    warehouse_bucket = "weather-lake"
    object_keys = ",".join(object_keys)

    object_keys_str = ",".join(object_keys)
    command = (
        "bash -lc '" \
        "/opt/spark/bin/spark-submit " \
        "--packages org.apache.hadoop:hadoop-aws:3.3.4 " \
        "--master spark://spark-master:7077 " \
        "/opt/spark/apps/weather-lake-load.py "
        f"--raw-bucket {raw_bucket} "
        f"--warehouse-bucket {warehouse_bucket} "
        f"--object-keys {object_keys}'"
    )
    stdin, stdout, stderr = ssh.exec_command(command)

    logging.info("Executing Spark job...")
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