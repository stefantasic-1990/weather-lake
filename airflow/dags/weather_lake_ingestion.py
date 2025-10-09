from airflow.sdk import dag, task
from handlers import weather_lake_ingestion_handlers as hand
from datetime import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def weather_lake_ingestion():

    @task
    def get_etl_configs():
        return hand.get_etl_configs_handler()
    
    get_etl_configs_task=get_etl_configs()

    @task
    def download_weather_data():
        return hand.download_weather_data_handler()

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
        get_etl_configs_task >>
        download_weather_data_task >>
        check_data_newness_task >>
        archive_raw_csv_data_task >>
        transform_data_to_parquet_task >>
        register_new_partition_task
    )

weather_lake_ingestion()