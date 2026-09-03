"""DAG DLP_TOKEN_SDK_TEST: временный отладочный DAG (будет удалён после отладки).

Цель — получить runtime IAM-токен воркера Managed Airflow и вывести его целиком
в лог таска, чтобы скопировать из логов Airflow и проверить токен локально
(например, в вызовах DLP API). Никаких вызовов DLP API сам DAG не делает —
только логирование токена и его длины.

Токен получается в рантайме на воркере от сервисного аккаунта, привязанного
к кластеру Managed Airflow (документация: managed-airflow/operations/
get-iam-token): yandexcloud.SDK()._channels._token_requester.get_token() —
ровно тот же паттерн, что в dlp_mart_demo_dag.py. Пакет yandexcloud
предустановлен на воркерах Managed Airflow. Статических токенов в коде нет;
полный вывод токена в лог — осознанное временное отладочное решение
в preprod-sandbox. После отладки DAG удалить.
"""

import logging
from datetime import datetime, timedelta

import yandexcloud
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def get_iam_token():
    """IAM-токен сервисного аккаунта кластера Managed Airflow (runtime)."""
    sdk = yandexcloud.SDK()
    return sdk._channels._token_requester.get_token()


def log_iam_token():
    token = get_iam_token()
    logger.info("IAM token: %s", token)
    logger.info("IAM token length: %s", len(token))
    return len(token)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DLP_TOKEN_SDK_TEST",
    description="Тестовый DAG: получает runtime IAM-токен воркера и пишет его в лог (отладка, потом удалить)",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "dlp", "test"],
) as dag:
    log_iam_token_task = PythonOperator(
        task_id="log_iam_token",
        python_callable=log_iam_token,
    )
