from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

CITIES = {
    "Paris": {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon": {"latitude": 45.7640, "longitude": 4.8357},
    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
}

default_args = {
    "owner": "samuel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def fetch_weather(city: str, coords: dict, **context):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "current_weather": True,
        "hourly": "relativehumidity_2m,windspeed_10m",
        "timezone": "Europe/Paris",
        "forecast_days": 1,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    raw = response.json()
    context["ti"].xcom_push(key=f"raw_{city.lower()}", value=raw)
    print(f"[fetch] Données brutes récupérées pour {city}")


def transform_weather(city: str, **context):
    raw = context["ti"].xcom_pull(key=f"raw_{city.lower()}")
    current = raw["current_weather"]
    transformed = {
        "city": city,
        "temperature_c": current["temperature"],
        "windspeed_kmh": current["windspeed"],
        "weather_code": current["weathercode"],
        "humidity_pct": raw["hourly"]["relativehumidity_2m"][0],
        "collected_at": current["time"],
    }
    context["ti"].xcom_push(key=f"transformed_{city.lower()}", value=transformed)
    print(f"[transform] Données préparées pour {city} : {transformed}")


def validate_weather(city: str, **context):
    data = context["ti"].xcom_pull(key=f"transformed_{city.lower()}")
    required_fields = ["city", "temperature_c", "windspeed_kmh", "humidity_pct", "collected_at"]
    for field in required_fields:
        assert field in data and data[field] is not None, f"Champ manquant : {field}"
    print(f"[validate] Données valides pour {city}")


def load_to_db(**context):
    for city in CITIES:
        data = context["ti"].xcom_pull(key=f"transformed_{city.lower()}")
        print(f"[load] Insertion pour {city} : {data}")


def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    print(f"[alert] Échec détecté sur la tâche : {task_id}")


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline météo quotidien multi-villes — Open-Meteo",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["météo", "tp2a"],
) as dag:

    fetch_tasks = []
    transform_tasks = []
    validate_tasks = []

    for city, coords in CITIES.items():
        fetch = PythonOperator(
            task_id=f"fetch_{city.lower()}",
            python_callable=fetch_weather,
            op_kwargs={"city": city, "coords": coords},
            on_failure_callback=alert_on_failure,
        )

        transform = PythonOperator(
            task_id=f"transform_{city.lower()}",
            python_callable=transform_weather,
            op_kwargs={"city": city},
            on_failure_callback=alert_on_failure,
        )

        validate = PythonOperator(
            task_id=f"validate_{city.lower()}",
            python_callable=validate_weather,
            op_kwargs={"city": city},
            on_failure_callback=alert_on_failure,
        )

        fetch >> transform >> validate

        fetch_tasks.append(fetch)
        transform_tasks.append(transform)
        validate_tasks.append(validate)

    load = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db,
        on_failure_callback=alert_on_failure,
    )

    log_execution = PythonOperator(
        task_id="log_execution",
        python_callable=lambda **ctx: print(f"[log] Run terminé — {ctx['ts']}"),
        trigger_rule="all_done",
    )

    validate_tasks >> load >> log_execution