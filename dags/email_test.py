from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG(
    dag_id="email_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    send_email = EmailOperator(
        task_id="send_test_email",
        to=["lautaropoletto@gmail.com"],
        subject="Prueba PoC con Gmail + Airflow",
        html_content="<h3>Esto es una prueba!</h3>",
    )
