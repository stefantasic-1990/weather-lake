from airflow.sdk import dag, task
from handlers import weather_lake_handlers as hand
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
    def download_weather_data(config):
        return hand.download_weather_data_handler(config)

    download_weather_data_task=download_weather_data.expand(config=get_etl_configs_task)

    @task
    def check_data_newness(temp_file_path, temp_file_digest):
        return hand.check_data_newness_handler(temp_file_path, temp_file_digest)

    check_data_newness_task=check_data_newness.expand_kwargs(download_weather_data_task)

    @task
    def archive_raw_csv_data(temp_file_path):
        return hand.archive_raw_csv_data_handler(temp_file_path)

    archive_raw_csv_data_task=archive_raw_csv_data.expand(temp_file_path=check_data_newness_task)

    @task
    def transform_data_to_parquet(object_keys):
        return hand.transform_data_to_parquet_handler(object_keys)

    transform_data_to_parquet_task=transform_data_to_parquet(object_keys=archive_raw_csv_data_task)
    
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