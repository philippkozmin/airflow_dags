"""DAG-расписание для сохранённого SQL-запроса DataLens Platform (DLP).

Каждые 5 минут выполняет сохранённый SQL-запрос 1wi4zefzkq2ck (preprod:
SELECT * FROM "dlback-test-catalog-10".system.iceberg_tables
WHERE table_name = {{tableName}} LIMIT 100) через лежащий рядом SDK-модуль
dlp_sdk (RPC runSqlQuery, окружение preprod, org yc.organization-manager.sandbox)
с параметром tableName=shops. Таск логирует статус выполнения
(executed_query.status) и события-строки результата, а в XCom уходит число
строк ответа (dlp_sdk.count_result_rows).

IAM-токен сервисного аккаунта airflowsa получается в рантайме вызовом
dlp_sdk.get_iam_token() (yc CLI + authorized key) и передаётся в
dlp_sdk.run_sql_query явным параметром iam_token — временное решение,
отмеченное пользователем.

ВАЖНО: токен сервисного аккаунта airflowsa пока не лицензирован в DLP,
поэтому до выдачи лицензии таск будет падать с 401/403 от API — это
ожидаемое поведение, а не ошибка DAG. Authorized key лежит вне репозитория:
/Users/philippkozmin/github/dlp-demo/authorized_key.json. Секреты в коде,
конфигурацию DAG и логи не пишутся.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import dlp_sdk

logger = logging.getLogger(__name__)

SQL_QUERY_ID = "1wi4zefzkq2ck"
TABLE_NAME = "shops"


def run_sql_query():
    iam_token = dlp_sdk.get_iam_token()
    response = dlp_sdk.run_sql_query(
        SQL_QUERY_ID,
        {"tableName": TABLE_NAME},
        iam_token=iam_token,
    )
    executed_query = response.get("executed_query") or response.get("executedQuery") or {}
    logger.info("status: %s", executed_query.get("status"))
    for result in response.get("results") or []:
        for event in result.get("events") or []:
            if event.get("event") == "row":
                logger.info("row: %s", event)
    return dlp_sdk.count_result_rows(response)


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
