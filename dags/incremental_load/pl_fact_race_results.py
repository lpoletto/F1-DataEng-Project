from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from utils.helpers import *


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
    dag_id="pl_fact_race_results",
    default_args=default_args,
    params=params,
    description="Carga de datos de la tabla fact_race_results",
    # schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    tags=['fact_race_results', 'incremental_load']
) as dag:
    

    # Tasks
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

    wait_for_races_file = S3KeySensor(
        task_id="wait_for_races_file",
        bucket_name=Variable.get("silver_bucket_name"),
        bucket_key="{{ params.execution_date if params.execution_date != 'YYYY-MM-DD' else macros.ds_add(data_interval_end | ds, -1) }}/races/*/*.parquet",
        aws_conn_id="aws_default",
        wildcard_match=True,
        poke_interval=60 * 5, # Chequea cada 5 minutos
        timeout=60 * 60 * 2, # Se rinde después de 2 horas
        mode="reschedule", # Libera recursos mientras espera
    )

    load_gold = SparkSubmitOperator(
        task_id="load_gold_fact_race_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_fact_race_results_to_gold.py',
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

    merge_stg_to_gold = SparkSubmitOperator(
        task_id="merge_stg_to_fact_race_results",
        application=f'{Variable.get("spark_scripts_dir")}/merge_fact_race_results_to_gold.py',
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

    [wait_for_results_file, wait_for_races_file] >> load_gold >> merge_stg_to_gold