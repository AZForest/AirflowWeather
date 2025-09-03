from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="./.env")

db_user = os.getenv("MY_DB_USER")
db_pass = os.getenv("MY_DB_PASSWORD")

@dag(
    "write_to_aurora",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",
    # schedule_interval=None,
    catchup=False,
    description="Reads json files, and puts into a db table",
)
def api_pipeline():
    # context = get_current_context()
    # execution_date = context["execution_date"]
    AURORA_HOST = "kaabullyzugwom3tv55kdlehui.dsql.us-west-2.on.aws"
    AURORA_USER = "admin"
    AURORA_PORT = 5432
    AWS_REGION = "us-west-2"

    generate_dsql_token = BashOperator(
        task_id="generate_dsql_token",
        bash_command=f"""
        TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
            --region {AWS_REGION} \
            --expires-in 900 \
            --hostname {AURORA_HOST})
        echo $TOKEN > /tmp/dsql_token.txt
        """,
        do_xcom_push=False,  # don’t log sensitive token
    )

    spark_job = SparkSubmitOperator(
        task_id="read_json_files_task",
        application="dags/rw_data_spark.py",
        name="read_json_files",
        jars="/Users/alexforest/jars/postgresql-42.7.3.jar",
        verbose=True,
        conn_id='spark_weather',
        env_vars={
            "AURORA_HOST": "kaabullyzugwom3tv55kdlehui.dsql.us-west-2.on.aws",
            "AURORA_USER": "admin",
            "AURORA_PORT": "5432",
            "AURORA_TOKEN": "",
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
        application_args=[
            "--db-host", AURORA_HOST,
            "--db-port", str(AURORA_PORT),
            "--db-user", AURORA_USER,
            "--db-password-file", "/tmp/dsql_token.txt",  # pass token file, not raw string
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    )
    generate_dsql_token >> spark_job

dag_instance = api_pipeline()