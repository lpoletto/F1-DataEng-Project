from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from utils.helpers import notify_custom_email

local_tz = timezone("America/Argentina/Buenos_Aires")

params = {"execution_date": "YYYY-MM-DD"}

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "on_failure_callback": notify_custom_email
}

with DAG(
    dag_id="pl_driver_standings",
    default_args=default_args,
    params=params,
    description="Carga de datos de la tabla driver_standings",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['driver_standings', 'incremental_load']
) as dag:
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_driver_standings_to_bronze.py',
        conn_id="spark_default",
        driver_class_path=Variable.get("driver_class_path"),
        application_args=["{{ params.execution_date if params.execution_date != 'YYYY-MM-DD' else macros.ds_add(data_interval_end | ds, -1) }}"],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False
    )
    
    wait_for_results_file = S3KeySensor(
        task_id="wait_for_results_file",
        bucket_name=Variable.get("silver_bucket_name"),
        bucket_key="{{ params.execution_date if params.execution_date != 'YYYY-MM-DD' else macros.ds_add(data_interval_end | ds, -1) }}/results/*/*.parquet",
        aws_conn_id="aws_default",
        wildcard_match=True,
        poke_interval=60 * 5, # Chequea cada 5 minutos
        timeout=60 * 60 * 2, # Se rinde después de 2 horas
        mode="reschedule", # Libera recursos mientras espera
    )

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_driver_standings_to_silver.py',
        conn_id="spark_default",
        driver_class_path=Variable.get("driver_class_path"),
        application_args=["{{ params.execution_date if params.execution_date != 'YYYY-MM-DD' else macros.ds_add(data_interval_end | ds, -1) }}"],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False
    )

    load_bronze >> wait_for_results_file >> load_silver