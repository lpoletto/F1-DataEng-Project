from os import environ as env
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from utils.common_functions import create_bucket

defaul_args = {
    "owner": "Lautaro Poletto",
    "start_date": datetime(2025, 9, 29),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# Define el nombre del bucket y la región (opcional)
# BUCKET_NAME = Variable.get("bucket_name")
REGION = None  # Cambia esto según sea necesario

with DAG(
    dag_id="start_up_init_DAG",
    default_args=defaul_args,
    description="Si se enfoca en configurar el entorno de datos (esquemas, tablas, buckets)",
    # schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tasks
    create_bronze_bucket = PythonOperator(
        task_id="create_bronze_bucket",
        python_callable=create_bucket,
        op_kwargs={"bucket_name": Variable.get("bronze_bucket_name"), "region": REGION},  # Pasa los argumentos

    )

    create_silver_bucket = PythonOperator(
        task_id="create_silver_bucket",
        python_callable=create_bucket,
        op_kwargs={"bucket_name": Variable.get("silver_bucket_name"), "region": REGION},  # Pasa los argumentos

    )

    create_gold_bucket = PythonOperator(
        task_id="create_gold_bucket",
        python_callable=create_bucket,
        op_kwargs={"bucket_name": Variable.get("gold_bucket_name"), "region": REGION},  # Pasa los argumentos

    )

    create_bronze_bucket >> create_silver_bucket >> create_gold_bucket