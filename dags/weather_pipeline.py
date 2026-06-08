from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

CITIES = ["Paris", "Lyon", "Marseille"]

default_args = {
    "owner": "samuel",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


def fetch_weather(city: str, **context):
    print(f"[fetch] Appel API météo pour {city}")


def validate_weather(city: str, **context):
    print(f"[validate] Vérification des données pour {city}")
    required_fields = ["temperature", "humidity", "wind_speed"]
    for field in required_fields:
        assert field, f"Champ manquant : {field}"


def load_to_db(**context):
    print("[load] Insertion des données validées en base")


def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    print(f"[alert] Échec détecté sur la tâche : {task_id}")


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline météo quotidien multi-villes",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["météo", "tp2"],
) as dag:

    fetch_tasks = []
    validate_tasks = []

    for city in CITIES:
        fetch = PythonOperator(
            task_id=f"fetch_{city.lower()}",
            python_callable=fetch_weather,
            op_kwargs={"city": city},
            on_failure_callback=alert_on_failure,
        )

        validate = PythonOperator(
            task_id=f"validate_{city.lower()}",
            python_callable=validate_weather,
            op_kwargs={"city": city},
            on_failure_callback=alert_on_failure,
        )

        fetch >> validate

        fetch_tasks.append(fetch)
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