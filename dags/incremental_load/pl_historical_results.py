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
    dag_id="results_historical_load_etl",
    default_args=default_args,
    description="Carga de datos de la tabla results",
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
    tags=['results', 'historical_load']
) as dag:
    
    execution_date = f'{Variable.get("execution_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_history_results_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_results_to_silver.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_bronze >> load_silver