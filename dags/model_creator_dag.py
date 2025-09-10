from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

@dag(
    "model_creator_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    description="Creates a model with S3 data",
)
def api_pipeline():

    spark_job = SparkSubmitOperator(
        task_id="create_model_with_new_data",
        application="dags/model_creator.py",
        name="create_new_model",
        verbose=True,
        conn_id='spark_weather',
        env_vars={
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )
    spark_job

dag_instance = api_pipeline()