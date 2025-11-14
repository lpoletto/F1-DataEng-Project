from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from utils.helpers import create_dim_date, create_bucket, create_a_database, execute_sql_query


SQL_QUERY_CREATE_GOLD_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS {env["GOLD_SCHEMA"]};
"""

SQL_QUERY_CREATE_SILVER_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS {env["SILVER_SCHEMA"]};
"""

SQL_QUERY_CREATE_STG_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS {env["STG_SCHEMA"]};
"""

# Define el nombre del bucket y la región (opcional)
# BUCKET_NAME = Variable.get("bucket_name")
REGION = None  # Cambia esto según sea necesario

local_tz = timezone("America/Argentina/Buenos_Aires")

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="start_up_init",
    default_args=default_args,
    description="Si se enfoca en configurar el entorno de datos (esquemas, tablas, buckets)",
    schedule_interval=None, # Se ejecuta manualmente
    catchup=False, # No ejecuta tareas pasadas
    tags=["setup", "init"],
    dagrun_timeout=timedelta(minutes=20), # Tiempo máximo de ejecución del DAG, sino falla
    max_active_runs=1,  # Solo permite una ejecución activa del DAG a la vez
    params = {
        "end_date": "2030-12-31", # Parámetro para la fecha de fin en la creación de dim_date 
        "description": "end_date in format YYYY-MM-DD"
    }
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

    create_dwh_db = PythonOperator(
        task_id="create_dwh_db",
        python_callable=create_a_database,
        op_kwargs={"db_name": Variable.get("F1_DWH")}
    )

    create_gold_schema = PythonOperator(
        task_id="create_gold_schema",
        python_callable=execute_sql_query,
        op_kwargs={
            "sql_query": SQL_QUERY_CREATE_GOLD_SCHEMA,
            "db_name": "{{ task_instance.xcom_pull(task_ids='create_dwh_db') }}"
        }
    )

    create_silver_schema = PythonOperator(
        task_id="create_silver_schema",
        python_callable=execute_sql_query,
        op_kwargs={
            "sql_query": SQL_QUERY_CREATE_SILVER_SCHEMA,
            "db_name": "{{ task_instance.xcom_pull(task_ids='create_dwh_db') }}"
        }
    )

    create_stg_schema = PythonOperator(
        task_id="create_stg_schema",
        python_callable=execute_sql_query,
        op_kwargs={
            "sql_query": SQL_QUERY_CREATE_STG_SCHEMA,
            "db_name": "{{ task_instance.xcom_pull(task_ids='create_dwh_db') }}"
        }
    )

    create_table_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=create_dim_date,
        op_kwargs={"end_date": "{{ dag_run.conf['end_date'] }}"},  # Pasa los argumentos. Usa el parámetro desde la UI
    )

    create_bronze_bucket >> create_silver_bucket >> create_gold_bucket

    create_dwh_db >> [create_gold_schema,create_silver_schema,create_stg_schema, create_table_dim_date]