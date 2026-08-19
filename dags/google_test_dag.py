"""Тестовый DAG: каждые 5 минут запрашивает google.com и логирует HTTP-статус."""

import logging
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def fetch_google():
    with urllib.request.urlopen("https://www.google.com", timeout=30) as response:
        status = response.status
    logger.info("HTTP-статус google.com: %s", status)
    return status


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="google_test",
    description="Каждые 5 минут запрашивает google.com и логирует HTTP-статус",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "network"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_google",
        python_callable=fetch_google,
    )
