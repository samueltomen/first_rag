from datetime import datetime, timedelta

import requests
import psycopg2

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

DEFAULT_CITIES = {
    "Paris": {"latitude": 48.8566, "longitude": 2.3522},
    "Lyon": {"latitude": 45.7640, "longitude": 4.8357},
    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
    "Bordeaux": {"latitude": 44.8378, "longitude": -0.5792},
    "Toulouse": {"latitude": 43.6047, "longitude": 1.4442},
    "Nantes": {"latitude": 47.2184, "longitude": -1.5536},
    "Strasbourg": {"latitude": 48.5734, "longitude": 7.7521},
    "Lille": {"latitude": 50.6292, "longitude": 3.0573},
    "Rennes": {"latitude": 48.1173, "longitude": -1.6778},
    "Nice": {"latitude": 43.7102, "longitude": 7.2620},
}

default_args = {
    "owner": "samuel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_db_conn():
    conn_info = BaseHook.get_connection("weather_postgres")
    return psycopg2.connect(
        host=conn_info.host,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
        port=conn_info.port,
    )


def get_cities(context) -> dict:
    params = context["params"]
    cities_param = params.get("cities", list(DEFAULT_CITIES.keys()))
    return {city: DEFAULT_CITIES[city] for city in cities_param if city in DEFAULT_CITIES}


def fetch_weather(city: str, **context):
    cities = get_cities(context)
    if city not in cities:
        print(f"[fetch] {city} non sélectionnée, tâche ignorée")
        return
    coords = cities[city]
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
    cities = get_cities(context)
    if city not in cities:
        print(f"[transform] {city} non sélectionnée, tâche ignorée")
        return
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
    cities = get_cities(context)
    if city not in cities:
        print(f"[validate] {city} non sélectionnée, tâche ignorée")
        return
    data = context["ti"].xcom_pull(key=f"transformed_{city.lower()}")
    required_fields = ["city", "temperature_c", "windspeed_kmh", "humidity_pct", "collected_at"]
    for field in required_fields:
        assert field in data and data[field] is not None, f"Champ manquant : {field}"
    print(f"[validate] Données valides pour {city}")


def load_to_db(**context):
    cities = get_cities(context)
    conn = get_db_conn()
    cursor = conn.cursor()
    for city in cities:
        data = context["ti"].xcom_pull(key=f"transformed_{city.lower()}")
        if data is None:
            print(f"[load] Pas de données pour {city}, ignoré")
            continue
        cursor.execute(
            """
            INSERT INTO weather_data (city, temperature_c, windspeed_kmh, weather_code, humidity_pct, collected_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                data["city"],
                data["temperature_c"],
                data["windspeed_kmh"],
                data["weather_code"],
                data["humidity_pct"],
                data["collected_at"],
            ),
        )
        print(f"[load] Insertion réussie pour {city}")
    conn.commit()
    cursor.close()
    conn.close()


def log_ingestion(**context):
    cities = get_cities(context)
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO ingestion_log (dag_id, run_id, status, cities_processed)
        VALUES (%s, %s, %s, %s)
        """,
        (
            context["dag"].dag_id,
            context["run_id"],
            "success",
            ", ".join(cities.keys()),
        ),
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[log] Ingestion tracée pour : {', '.join(cities.keys())}")


def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    print(f"[alert] Échec détecté sur la tâche : {task_id}")


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline météo quotidien multi-villes — Open-Meteo + PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["météo", "tp2b"],
        params={
            "cities": Param(
                default=["Paris", "Lyon", "Marseille", "Bordeaux", "Toulouse", "Nantes", "Strasbourg", "Lille",
                         "Rennes", "Nice"],
                type="array",
                description="Liste des villes à traiter parmi les 10 disponibles",
            )
        },
) as dag:

    fetch_tasks = []
    transform_tasks = []
    validate_tasks = []

    for city in DEFAULT_CITIES:
        fetch = PythonOperator(
            task_id=f"fetch_{city.lower()}",
            python_callable=fetch_weather,
            op_kwargs={"city": city},
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
        python_callable=log_ingestion,
        trigger_rule="all_done",
    )

    validate_tasks >> load >> log_execution