"""DAG DLP_MART_DEMO: ежедневный запуск сохранённого SQL-запроса DataLens Platform.

Раз в сутки (cron "0 0 * * *", 00:00 UTC) выполняет сохранённый SQL-запрос DLP
ecgnn7kizpbax (построение витрины; параметров у запуска нет) через лежащий
рядом SDK-модуль dlp_sdk (RPC runSqlQuery, окружение preprod, org
yc.organization-manager.sandbox). Таск логирует статус выполнения
(executed_query.status) и события-строки результата, в XCom уходит число строк.

IAM-токен получается в рантайме на воркере Managed Airflow от сервисного
аккаунта, привязанного к кластеру (документация: managed-airflow/operations/
get-iam-token): yandexcloud.SDK()._channels._token_requester.get_token() —
и передаётся в dlp_sdk.run_sql_query явным параметром iam_token. Пакет
yandexcloud предустановлен на воркерах Managed Airflow. Статических токенов
в коде нет.
"""

import logging
from datetime import datetime, timedelta

import yandexcloud
from airflow import DAG
from airflow.operators.python import PythonOperator

import dlp_sdk

logger = logging.getLogger(__name__)

SQL_QUERY_ID = "ecgnn7kizpbax"


def get_iam_token():
    """IAM-токен сервисного аккаунта кластера Managed Airflow (runtime)."""
    sdk = yandexcloud.SDK()
    return sdk._channels._token_requester.get_token()


def run_sql_query():
    response = dlp_sdk.run_sql_query(
        SQL_QUERY_ID,
        iam_token=get_iam_token(),
    )
    executed_query = response.get("executed_query") or response.get("executedQuery") or {}
    logger.info("status: %s", executed_query.get("status"))
    rows = 0
    for result in response.get("results") or []:
        for event in result.get("events") or []:
            if event.get("event") == "row":
                rows += 1
                logger.info("row: %s", event)
    return rows


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DLP_MART_DEMO",
    description="Раз в сутки выполняет сохранённый SQL-запрос DLP (витрина, preprod) через dlp_sdk",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "dlp", "preprod"],
) as dag:
    run_sql_query_task = PythonOperator(
        task_id="run_sql_query",
        python_callable=run_sql_query,
    )
