import requests
import hashlib
from airflow.sdk import dag, task
from datetime import datetime

openmeteo_api_endpoint=(
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=43.7064&longitude=-79.3986&"
    "hourly=temperature_2m,relative_humidity_2m,precipitation,"
    "wind_speed_10m,wind_direction_10m,pressure_msl&format=csv"
)
temp_file_dir="/opt/airflow/temp/"
temp_file_name="data.csv"
temp_file_path=temp_file_dir+temp_file_name
chunk_size=8192

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def weather_lake_ingestion():

    @task
    def download_weather_data():
        hasher=hashlib.sha256()

        with requests.get(openmeteo_api_endpoint, stream=True, timeout=60) as req:
            req.raise_for_status()

            with open(temp_file_path, "wb") as temp_file:
                for chunk in req.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    hasher.update(chunk)
                    temp_file.write(chunk)

        temp_file_digest = hasher.hexdigest()
        
        return str(temp_file_path), temp_file_digest

    download_weather_data_task=download_weather_data()

    @task
    def check_data_newness():
        return

    check_data_newness_task=check_data_newness()

    @task
    def archive_raw_csv_data():
        return

    archive_raw_csv_data_task=archive_raw_csv_data()

    @task
    def transform_data_to_parquet():
        return

    transform_data_to_parquet_task=transform_data_to_parquet()
    
    @task
    def register_new_partition():
        return

    register_new_partition_task=register_new_partition()

    (
        download_weather_data_task >>
        check_data_newness_task >>
        archive_raw_csv_data_task >>
        transform_data_to_parquet_task >>
        register_new_partition_task
    )

weather_lake_ingestion()