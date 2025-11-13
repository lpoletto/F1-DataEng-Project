from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.datasets import Dataset


EXECUTION_DATE = Variable.get("execution_date")

local_tz = timezone("America/Argentina/Buenos_Aires")
default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_circuits",
    default_args=default_args,
    description="Carga de datos de la tabla circuits",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['circuits', 'full_load']
) as dag:
    
    # execution_date = f'{Variable.get("execution_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_circuits",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_circuits_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_circuits",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_circuits_to_silver.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_gold = SparkSubmitOperator(
        task_id="load_gold_dim_circuits",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_dim_circuit_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[EXECUTION_DATE],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_bronze >> load_silver >> load_gold