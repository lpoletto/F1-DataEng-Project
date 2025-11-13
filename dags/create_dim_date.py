from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from utils.helpers import create_dim_date, create_bucket, create_a_database, execute_sql_query


local_tz = timezone("America/Argentina/Buenos_Aires")

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="create_dim_date",
    default_args=default_args,
    description="Crea y carga la tabla dim_date en la base de datos DWH",
    schedule_interval=None, # Se ejecuta manualmente
    catchup=False, # No ejecuta tareas pasadas
    tags=["setup", "init"],
    dagrun_timeout=timedelta(minutes=20), # Tiempo máximo de ejecución del DAG, sino falla
    max_active_runs=1,  # Solo permite una ejecución activa del DAG a la vez
    params = {
        "date_from": "YYYY-MM-DD" # Parámetro para la fecha de fin en la creación de dim_date 
    }
) as dag:
    
    # Tasks

    create_table_dim_date = PythonOperator(
        task_id="load_dim_date",
        python_callable=create_dim_date,
        op_kwargs={"end_date": "{{params.date_from}}"},  # Pasa los argumentos. Usa el parámetro desde la UI
    )

    create_table_dim_date