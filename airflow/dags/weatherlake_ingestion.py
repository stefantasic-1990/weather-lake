from datetime import timedelta
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule
from handlers import weatherlake_ingestion_handlers as hand
from datetime import datetime

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=1),
    "trigger_rule": TriggerRule.all_done_min_one_success
}

@dag(
    dag_id="WEATHERLAKE_INGESTION",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10
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