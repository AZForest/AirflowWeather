from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# import openmeteo_requests

from datetime import datetime
import os
import csv
import random
import subprocess
import json
import boto3
import tempfile
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

@dag(
    "fetch_current_weather_data",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    description="Fetches the current weather for miami, nassau, havana",
)
def api_pipeline():

    def get_data_path():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        return file_date
        # return f"/tmp/data/weather/{file_date}"
        # /data/weather/date={file_data}/{location}/weather_data.json
    
    def custom_fetch(location, url): 
        # file_path = f"{get_data_path()}/{location}/weather_data.json"
        try:
            print("Using curl subprocess...")
            result = subprocess.run(
                ['curl', '-s', '--max-time', '30', url],
                capture_output=True,
                text=True,
                timeout=35
            )
            
            if result.returncode == 0:
                # data = json.loads(result.stdout)
                # print(f"Data: {data}")
                # for d in data:
                #     print(d)
                # print(f"Ex date: {execution_date}")
                print(result.stdout)

                # directory = os.path.dirname(file_path)
                # if not os.path.exists(directory):
                #     os.makedirs(directory)

                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                    temp_file.write(result.stdout)
                    temp_path = temp_file.name
                
                try:
                    key = f'data/weather/date={get_data_path()}/{location}/{location}_{get_data_path()}.json'
                    aws_result = subprocess.run([
                        'aws', 's3', 'cp', 
                        temp_path, 
                        f's3://af-weather-lake-01/{key}',
                        '--content-type', 'application/json'
                    ], capture_output=True, text=True, timeout=60)
                    
                    if aws_result.returncode == 0:
                        print(f"Successfully uploaded to S3: {key}")
                    else:
                        print(f"AWS CLI failed: {aws_result.stderr}")
                        raise Exception("S3 upload failed")
                finally:
                    # Clean up temp file
                    os.unlink(temp_path)

                print(f"Written to file: {key}")
            else:
                print(f"Curl failed: {result.stderr}")
                raise Exception(f"Curl failed with return code {result.returncode}")
                
        except Exception as e:
            print(f"Subprocess failed: {e}")
            raise

    spark_job = SparkSubmitOperator(
        task_id="predict_city_with_new_data",
        application="dags/predict_city.py",
        name="predict_cit_job",
        verbose=True,
        conn_id='spark_weather',
        env_vars={
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
        application_args=[
            '{{ execution_date.strftime("%Y-%m-%d_%H%M") }}'
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )
    
    @task
    def fetch_miami_data():
        url = "https://api.open-meteo.com/v1/forecast?latitude=25.76&longitude=80.19&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,cloud_cover,surface_pressure,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m,wind_direction_10m,wind_gusts_10m&temperature_unit=fahrenheit"
        custom_fetch("miami", url)
    @task
    def fetch_nassau_data():
        url = "https://api.open-meteo.com/v1/forecast?latitude=25.04&longitude=77.35&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,cloud_cover,surface_pressure,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m,wind_direction_10m,wind_gusts_10m&temperature_unit=fahrenheit"
        custom_fetch("nassau", url)
    @task
    def fetch_havana_data():
        url = "https://api.open-meteo.com/v1/forecast?latitude=23.13&longitude=82.35&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,cloud_cover,surface_pressure,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m,wind_direction_10m,wind_gusts_10m&temperature_unit=fahrenheit"
        custom_fetch("havana", url)

    fetch_miami_data() >> fetch_nassau_data() >> fetch_havana_data() >> spark_job
demo_dag = api_pipeline() 