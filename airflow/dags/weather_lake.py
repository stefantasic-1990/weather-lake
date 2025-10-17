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
    def get_ingestion_configs():
        return hand.get_ingestion_configs_handler()
    
    get_ingestion_configs_task=get_ingestion_configs()

    @task
    def get_forecast_data(ingestion_config):
        return hand.get_forecast_data_handler(ingestion_config)

    get_forecast_data_task=get_forecast_data.expand(ingestion_config=get_ingestion_configs_task)

    @task
    def check_forecast_data_newness(forecast_file_path, forecast_file_digest):
        return hand.check_forecast_data_newness_handler(forecast_file_path, forecast_file_digest)

    check_forecast_data_newness_task=check_forecast_data_newness.expand_kwargs(get_forecast_data_task)

    @task
    def archive_raw_forecast_data(forecast_file_path, forecast_file_digest):
        return hand.archive_raw_forecast_data_handler(forecast_file_path, forecast_file_digest)

    archive_raw_forecast_data_task=archive_raw_forecast_data.expand_kwargs(check_forecast_data_newness_task)

    @task
    def process_forecast_data(forecast_object_key):
        return hand.process_forecast_data_handler(forecast_object_key)

    process_forecast_data_task=process_forecast_data(archive_raw_forecast_data_task)

    (
        get_ingestion_configs_task >>
        get_forecast_data_task >>
        check_forecast_data_newness_task >>
        archive_raw_forecast_data_task >>
        process_forecast_data_task
    )

weather_lake_ingestion()