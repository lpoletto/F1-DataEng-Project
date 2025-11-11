from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Lautaro',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="dag_with_cron_expression_v02",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 3 * * MON",  # Ejecutar cada lunes a medianoche
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'DAG with cron expression running...'",
    )

    task1