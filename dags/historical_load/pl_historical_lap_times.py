from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

local_tz = timezone("America/Argentina/Buenos_Aires")

params = {"execution_date": f"{Variable.get('end_date')}"}

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_historical_lap_times",
    default_args=default_args,
    params=params,
    description="Carga de datos de la tabla lap_times",
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
    tags=['lap_times', 'historical_load']
) as dag:
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_lap_times",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_history_lap_times_to_bronze.py',
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

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_lap_times",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_lap_times_to_silver.py',
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

    load_bronze >> load_silver