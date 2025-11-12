from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from utils.helpers import *


default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_fact_driver_standings",
    default_args=default_args,
    description="Carga de datos de la tabla fact_driver_standings",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['fact_driver_standings', 'incremental_load']
) as dag:
    
    execution_date = f'{Variable.get("execution_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    load_gold = SparkSubmitOperator(
        task_id="load_gold_fact_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_fact_driver_standings_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    merge_stg_to_gold = SparkSubmitOperator(
        task_id="merge_stg_to_fact_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/merge_fact_driver_standings_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_gold >> merge_stg_to_gold