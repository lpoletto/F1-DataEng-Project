from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from utils.helpers import *


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
    dag_id="pl_fact_driver_standings",
    default_args=default_args,
    params=params,
    description="Carga de datos de la tabla fact_driver_standings",
    # schedule_interval="0 3 * * MON",  # Ejecuta semanalmente los lunes a medianoche"
    dagrun_timeout=timedelta(hours=2),
    catchup=False,
    tags=['fact_driver_standings', 'incremental_load']
) as dag:
    
    # Tasks
    wait_for_driver_standings_file = S3KeySensor(
        task_id="wait_for_driver_standings_file",
        bucket_name=Variable.get("silver_bucket_name"),
        bucket_key="{{ dag_run.conf.get('execution_date',macros.ds_add(data_interval_end | ds, -1)) }}/driver_standings/*/*.parquet",
        aws_conn_id="aws_default",
        wildcard_match=True,
        poke_interval=60 * 5, # Chequea cada 5 minutos
        timeout=60 * 60 * 2, # Se rinde después de 2 horas
        mode="reschedule", # Libera recursos mientras espera
    )

    wait_for_races_file = S3KeySensor(
        task_id="wait_for_races_file",
        bucket_name=Variable.get("silver_bucket_name"),
        bucket_key="{{ dag_run.conf.get('execution_date',macros.ds_add(data_interval_end | ds, -1)) }}/races/*/*.parquet",
        aws_conn_id="aws_default",
        wildcard_match=True,
        poke_interval=60 * 5, # Chequea cada 5 minutos
        timeout=60 * 60 * 2, # Se rinde después de 2 horas
        mode="reschedule", # Libera recursos mientras espera
    )

    load_gold = SparkSubmitOperator(
        task_id="load_gold_fact_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/ingest_fact_driver_standings_to_gold.py',
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

    merge_stg_to_gold = SparkSubmitOperator(
        task_id="merge_stg_to_fact_driver_standings",
        application=f'{Variable.get("spark_scripts_dir")}/merge_fact_driver_standings_to_gold.py',
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

    [wait_for_driver_standings_file, wait_for_races_file] >> load_gold >> merge_stg_to_gold