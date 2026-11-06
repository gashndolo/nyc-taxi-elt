import logging
import os
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

log = logging.getLogger(__name__)

PROJECT = "/opt/airflow/project"
PY = "/opt/airflow/pipeline-venv/bin/python"
DBT = "/opt/airflow/pipeline-venv/bin/dbt"
DBT_DIR = f"{PROJECT}/dbt_project"

# Same pattern as ingestion/config.py (duplicated: the DAG file can't import
# project code, which needs environment variables at import time)
TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

# Templated by Airflow: the month this run is responsible for
YEAR = "{{ data_interval_start.year }}"
MONTH = "{{ data_interval_start.month }}"


def notify_failure(context):
    ti = context["task_instance"]
    msg = (f"Airflow task failed: {ti.dag_id}.{ti.task_id} "
           f"(run {context['run_id']}, attempt {ti.try_number})")
    log.error(msg)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        try:
            # Slack-style payload; for Discord, change "text" to "content"
            requests.post(url, json={"text": msg}, timeout=10)
        except requests.RequestException:
            log.exception("Could not send alert")


def tlc_file_published(year, month) -> bool:
    url = TLC_URL.format(year=int(year), month=int(month))
    r = requests.head(url, timeout=30, allow_redirects=True)
    log.info("HEAD %s -> %s", url, r.status_code)
    return r.status_code == 200


with DAG(
    dag_id="nyc_taxi_elt",
    description="Monthly NYC taxi + weather: GCS -> BigQuery -> dbt",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("@monthly", timezone="UTC"),
    catchup=False,  # history is loaded deliberately with a backfill
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    tags=["nyc-taxi", "elt"],
) as dag:

    wait_for_tlc = PythonSensor(
        task_id="wait_for_tlc",
        python_callable=tlc_file_published,
        op_kwargs={"year": YEAR, "month": MONTH},
        mode="reschedule",
        poke_interval=12 * 3600,
        timeout=60 * 24 * 3600,
    )

    extract_to_gcs = BashOperator(
        task_id="extract_to_gcs",
        bash_command=(f"cd {PROJECT} && {PY} -m ingestion.run "
                      f"--year {YEAR} --month {MONTH} --skip-load"),
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=(f"cd {PROJECT} && {PY} -m ingestion.run "
                      f"--year {YEAR} --month {MONTH} --load-only"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(f"cd {DBT_DIR} && {DBT} deps --profiles-dir . "
                      f"&& {DBT} seed --profiles-dir . "
                      f"&& {DBT} run --profiles-dir ."),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT} test --profiles-dir .",
    )

    wait_for_tlc >> extract_to_gcs >> load_to_bigquery >> dbt_run >> dbt_test