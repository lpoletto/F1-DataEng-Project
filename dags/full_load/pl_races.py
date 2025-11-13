from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.datasets import Dataset

EXECUTION_DATE = Variable.get("execution_date") # Parámetro para la fecha de ejecución
SILVER_BUCKET = Variable.get("silver_bucket_path")
DATASET_RACES = Dataset(f"{SILVER_BUCKET}/{EXECUTION_DATE}/races")

local_tz = timezone("America/Argentina/Buenos_Aires")

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_races",
    default_args=default_args,
    description="Carga de datos de la tabla races",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['races', 'full_load']
) as dag:
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_races",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_races_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_races",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_races_to_silver.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE, DATASET_RACES.uri],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py',
        outlets=[DATASET_RACES]
    )

    load_gold = SparkSubmitOperator(
        task_id="load_gold_dim_races",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_dim_race_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_bronze >> load_silver >> load_gold