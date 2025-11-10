import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook


BASE_URL = "https://f1api.dev/api"

def fetch_race_results(season, round_number, http_conn_id):
    # GET api/[year]/[round]/race
    # Usa HttpHook para realizar la solicitud
    http = HttpHook(http_conn_id=http_conn_id, method="GET")
    conn = http.get_connection(http_conn_id)  # Obtén la conexión
    base_url = conn.host  # Obtén el host desde la conexión
    endpoint = f"{season}/{round_number}/race"
    print(f"\nFetching data from endpoint: {base_url}/{endpoint}\n")
    response = http.run(endpoint)
    if response.status_code == 200:
        print(response.json())
        return response.json()
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")


default_args = {
    "owner": "Lautaro",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="get_data_f1_api_v2",
    default_args=default_args,
    schedule="0 0 * * MON",  # Ejecuta semanalmente los lunes a medianoche
    catchup=False,
    tags=["f1_api"]
) as dag:
    # Sensor para verificar si la API está disponible
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="f1_api", # Define esta conexión en la interfaz de Airflow
        endpoint="/",
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )
    
    # Tarea para obtener los resultados de la carrera
    get_race_results = PythonOperator(
        task_id="get_race_results",
        python_callable=fetch_race_results,
        op_kwargs={
            "season": 2025, 
            "round_number": 21,
            "http_conn_id": "f1_api" 
        }, # Pasa los argumentos
    )

    is_api_available >> get_race_results