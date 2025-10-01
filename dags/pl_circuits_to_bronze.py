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
    
    # Tasks
    ingest_circuits_to_bronze = SparkSubmitOperator(
        task_id="ingest_circuits_to_bronze",
        application=f'{Variable.get("spark_scripts_dir")}/1_circuits_ingest_bronze.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    ingest_circuits_to_bronze