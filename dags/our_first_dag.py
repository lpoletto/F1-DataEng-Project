from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Lautaro",
    "retries": 2, # Maximo 2 reintentos en caso de fallo
    "retry_delay": timedelta(minutes=2), # Esperar 2 minutos entre reintentos
}

with DAG(
    dag_id="our_first_dag",
    default_args= default_args,
    description="Este es nuestro primer DAG en Airflow",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo 'Hola Mundo! Esta es mi primera tarea en Airflow!'"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo 'Hey! Soy la segunda tarea y me ejecuto después de la primera.'"
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo 'Hey! Soy la tercer tarea y me ejecuto al mismo tiempo que la segunda.'"
    )

    task1 >> [task2, task3]