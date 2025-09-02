from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

@dag(
    "read_json_weather_data",
    start_date=datetime(2025, 1, 1),
    # schedule_interval="0 * * * *",
    schedule_interval=None,
    catchup=False,
    description="Reads json files, and aggregates it into a csv",
)
def api_pipeline():
    context = get_current_context()
    execution_date = context["execution_date"]

    spark_job = SparkSubmitOperator(
        task_id="read_json_files_task",
        application="dags/rw_data_spark.py",
        name="read_json_files",
        application_args=[
            "--s3_bucket", "s3://af-weather-lake-01",
            "--output_path", "/tmp/data/weather_results/{{ execution_date.strftime('%Y-%m-%d_%H%M') }}"
        ],
        conn_id='spark_weather',
    )
    spark_job

dag_instance = api_pipeline()