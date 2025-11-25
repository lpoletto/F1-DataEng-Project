from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

local_tz = timezone("America/Argentina/Buenos_Aires")

params = {"execution_date": f"{Variable.get('end_date')}"}

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 9, 29),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

with DAG(
    dag_id="pl_historical_qualifying",
    default_args=default_args,
    params= params,
    description="Carga de datos de la tabla qualifying",
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
    tags=['qualifying', 'historical_load']
) as dag:
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_qualifying",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_history_qualifying_to_bronze.py',
        conn_id="spark_default",
        dag=dag,
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

    load_silver = SparkSubmitOperator(
        task_id="transform_silver_qualifying",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_qualifying_to_silver.py',
        conn_id="spark_default",
        dag=dag,
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

    load_bronze >> load_silver