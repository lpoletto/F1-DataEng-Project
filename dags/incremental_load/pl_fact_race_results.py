from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.datasets import Dataset
from utils.helpers import *


EXECUTION_DATE = Variable.get("execution_date")
SILVER_BUCKET = Variable.get("silver_bucket_path")
DATASET_RESULTS = Dataset(f"{SILVER_BUCKET}/{EXECUTION_DATE}/results")
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
    dag_id="pl_fact_race_results",
    default_args=default_args,
    description="Carga de datos de la tabla fact_race_results",
    # schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    schedule=[DATASET_RESULTS, DATASET_RACES],
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    max_active_runs=1,
    tags=['fact_race_results', 'incremental_load']
) as dag:
    
    # execution_date = f'{Variable.get("execution_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    load_gold = SparkSubmitOperator(
        task_id="load_gold_fact_race_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_fact_race_results_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    merge_stg_to_gold = SparkSubmitOperator(
        task_id="merge_stg_to_fact_race_results",
        application=f'{Variable.get("spark_scripts_dir")}/merge_fact_race_results_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_gold >> merge_stg_to_gold