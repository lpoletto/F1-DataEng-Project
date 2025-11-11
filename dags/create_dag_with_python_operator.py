from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": "@daily",
}


def saludar(ti):
    nombre = ti.xcom_pull(task_ids="get_name", key="first_name") # Para obtener el valor retornado por otra tarea
    apellido = ti.xcom_pull(task_ids="get_name", key="last_name") # Para obtener el valor retornado por otra tarea
    edad = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hola Mundo! Me llamo {nombre} {apellido} y tengo {edad} años.")

def get_name(ti):
    ti.xcom_push(key="first_name", value="Lautaro") # Para enviar un valor a otra tarea
    ti.xcom_push(key="last_name", value="Poletto")

def get_age(ti):
    ti.xcom_push(key="age", value=34)


with DAG(
    dag_id="our_dag_with_python_operator_v5",
    default_args=default_args,
    description="DAG de ejemplo para crear un DAG con PythonOperator",
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id="saludar",
        python_callable=saludar,
        # op_kwargs={ # Diccionario de argumentos para la función
        #     "edad": 34
        # },
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age,
    )


    [task2,task3] >> task1