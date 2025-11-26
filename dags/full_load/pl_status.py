from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

local_tz = timezone("America/Argentina/Buenos_Aires")

params = {"execution_date": "YYYY-MM-DD"}

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29, tzinfo=local_tz),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_status",
    default_args=default_args,
    params=params,
    description="Carga de datos de la tabla status",
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['status', 'full_load']
) as dag:
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_status",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_status_to_bronze.py',
        conn_id="spark_default",
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[
            """
            {{
            dag_run.conf.get(
                'execution_date',
                macros.ds_add(data_interval_end | ds, -1)
            )
            }}
            """
        ],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_gold = SparkSubmitOperator(
        task_id="load_gold_dim_status",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_dim_status_to_gold.py',
        conn_id="spark_default",
        driver_class_path=Variable.get("driver_class_path"),
        application_args=[
            """
            {{
            dag_run.conf.get(
                'execution_date',
                macros.ds_add(data_interval_end | ds, -1)
            )
            }}
            """
        ],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py'
    )

    load_bronze >> load_gold