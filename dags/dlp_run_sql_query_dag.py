"""DAG-расписание для сохранённого SQL-запроса DataLens Platform (DLP).

Каждые 5 минут выполняет сохранённый SQL-запрос 1wi4zefzkq2ck (preprod:
SELECT * FROM "dlback-test-catalog-10".system.iceberg_tables
WHERE table_name = {{tableName}} LIMIT 100) с параметром tableName=shops
через лежащий рядом SDK-модуль dlp_sdk (RPC runSqlQuery, окружение preprod,
org yc.organization-manager.sandbox). Таск логирует статус выполнения
(executed_query.status) и события-строки результата, в XCom уходит число строк.

IAM-токен сервисного аккаунта airflowsa записан ниже явной константой IAM_TOKEN
и передаётся в dlp_sdk.run_sql_query явным параметром iam_token — ВРЕМЕННОЕ
решение по решению пользователя: токен живёт не более 12 часов, после чего его
надо перевыпускать и обновлять здесь (в будущем заменим на получение в рантайме).
ВАЖНО: пока у airflowsa нет лицензии DLP, API отвечает 401/403 — это ожидаемое
поведение, а не ошибка DAG.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import dlp_sdk

logger = logging.getLogger(__name__)

SQL_QUERY_ID = "1wi4zefzkq2ck"
TABLE_NAME = "shops"
IAM_TOKEN = "t1.9eudmZ2RzJXOx5iKl8rGlomLlp6My-3rnZmdlJqSj56cioyUjsrGi5CWlcnl9Pd2S2gr-e8tDy2L3fT3NnplK_nvLQ8ti83n9eudmZ2dm5GekIzMysnNmIuXjsmPmu_8zef1652ZnZ2UmcqOz82XzpWUm5PPk4zH7_3F652ZnZ2bkZ6QjMzKyc2Yi5eOyY-a.iEv8R8IbuX_PqfS54EGnABOnaTS5i7MPufCEG04-_Q-MkGLzwL20XDruNYgyo914sK03yiNpAG7XoUWMXCk0AA"


def run_sql_query():
    response = dlp_sdk.run_sql_query(
        SQL_QUERY_ID,
        {"tableName": TABLE_NAME},
        iam_token=IAM_TOKEN,
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
    dag_id="dlp_run_sql_query",
    description="Каждые 5 минут выполняет сохранённый SQL-запрос DLP (preprod) через dlp_sdk",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "dlp", "preprod"],
) as dag:
    run_sql_query_task = PythonOperator(
        task_id="run_sql_query",
        python_callable=run_sql_query,
    )
