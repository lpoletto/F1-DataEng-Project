from os import environ as env
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_qualifying",
    default_args=default_args,
    description="Carga de datos de la tabla qualifying",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['qualifying', 'incremental_load']
) as dag:
    
    execution_date = f'{Variable.get("end_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_qualifying",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_qualifying_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_qualifying",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_qualifying_to_silver.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_bronze >> load_silver