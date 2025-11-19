from os import environ as env
from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.datasets import Dataset


SILVER_BUCKET = Variable.get("silver_bucket_path")

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
    dag_id="pl_results",
    default_args=default_args,
    description="Carga de datos de la tabla results",
    params= params,
    schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    catchup=False,
    tags=['results', 'incremental_load']
) as dag:
    
    DATASET_RESULTS = Dataset(
        f"{SILVER_BUCKET}/{{{{ dag_run.conf.get('execution_date', macros.ds_add(ds, -1)) }}}}/results"
    )
    
    # Tasks
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_results_to_bronze.py',
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
        task_id="transform_silver_results",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_results_to_silver.py',
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
            ,DATASET_RESULTS.uri
        ],
        py_files= f'{Variable.get("dags_dir")}/utils/helpers.py',
        outlets=[DATASET_RESULTS]
    )

    load_bronze >> load_silver