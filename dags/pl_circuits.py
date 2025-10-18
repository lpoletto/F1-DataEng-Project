from os import environ as env
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

defaul_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="pl_circuits_to_bronze",
    default_args=defaul_args,
    description="Carga de datos de circuits desde MySQL a Bronze en MinIO",
    schedule_interval="@weekly",
    catchup=False,
) as dag:
    
    execution_date = f'{Variable.get("execution_date")}' # Parámetro para la fecha de ejecución
    # Tasks
    ingest_circuits_to_bronze = SparkSubmitOperator(
        task_id="ingest_circuits_to_bronze",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_circuits_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
    )

    ingest_circuits_to_silver = SparkSubmitOperator(
        task_id="ingest_circuits_to_silver",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_circuits_to_silver.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
    )

    ingest_circuits_to_gold = SparkSubmitOperator(
        task_id="ingest_circuits_to_gold",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_circuits_to_gold.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[execution_date],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    ingest_circuits_to_bronze >> ingest_circuits_to_silver >> ingest_circuits_to_gold