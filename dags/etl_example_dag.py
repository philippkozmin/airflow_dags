"""DAG etl_example: запуск сохранённого SQL-запроса DataLens Platform (DLP).

Каждые 5 минут выполняет сохранённый SQL-запрос 0wuiid112l4uj (preprod,
перезагрузка партиции marts.mart_orders) с параметром launch_date — датой
запуска (логической датой) DAG в текстовом формате dd-mm-yyyy (%d-%m-%Y) —
через лежащий рядом SDK-модуль dlp_sdk (RPC runSqlQuery, окружение preprod,
org yc.organization-manager.sandbox).

Запрос заведомо возвращает от Trino целевую (ожидаемую) ошибку
DB.INVALID_QUERY / SYNTAX_ERROR («mismatched input ';'» на первом стейтменте
DROP TABLE ...). Для этого DAG такая ошибка — УСПЕХ таска: логируются код
ошибки, db_message и query_id. Провалом таска считаются только
транспортные/HTTP-ошибки (включая 401/403), ошибка валидации RPC, ответ вовсе
без results/executed_query или ошибка, не похожая на целевую.

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
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

import dlp_sdk

logger = logging.getLogger(__name__)

SQL_QUERY_ID = "0wuiid112l4uj"

# Целевая (ожидаемая) ошибка запроса: Trino SYNTAX_ERROR на первом стейтменте.
TARGET_ERROR_CODE = "DB.INVALID_QUERY"
TARGET_DB_MESSAGE_MARKER = "mismatched input ';'"
TARGET_ERROR_NAME = "SYNTAX_ERROR"


def get_iam_token():
    """IAM-токен сервисного аккаунта кластера Managed Airflow (runtime)."""
    sdk = yandexcloud.SDK()
    return sdk._channels._token_requester.get_token()


def _is_target_error(error):
    """Похожа ли ошибка результата на целевую (DB.INVALID_QUERY / SYNTAX_ERROR)."""
    if error.get("code") == TARGET_ERROR_CODE:
        return True
    details = error.get("details") or {}
    debug = error.get("debug") or {}
    db_messages = (error.get("db_message"), details.get("db_message"), debug.get("db_message"))
    if any(message and TARGET_DB_MESSAGE_MARKER in message for message in db_messages):
        return True
    return details.get("error_name") == TARGET_ERROR_NAME


def run_sql_query(**context):
    launch_date = (context.get("logical_date") or context["execution_date"]).strftime("%d-%m-%Y")
    logger.info("launch_date: %s", launch_date)
    response = dlp_sdk.run_sql_query(
        SQL_QUERY_ID,
        {"launch_date": launch_date},
        iam_token=get_iam_token(),
    )
    executed_query = response.get("executed_query") or response.get("executedQuery") or {}
    results = response.get("results") or []
    errors = [result["error"] for result in results if result.get("error")]

    target_errors = [error for error in errors if _is_target_error(error)]
    if target_errors:
        # Целевая (ожидаемая) ошибка — таск завершается успехом.
        for error in target_errors:
            details = error.get("details") or {}
            db_message = details.get("db_message") or (error.get("debug") or {}).get("db_message")
            logger.info(
                "target (expected) error: code=%s db_message=%s query_id=%s",
                error.get("code"),
                db_message,
                details.get("query_id"),
            )
        return {
            "launch_date": launch_date,
            "target_error": True,
            "error_code": target_errors[0].get("code"),
            "query_id": (target_errors[0].get("details") or {}).get("query_id"),
            "executed_query_id": executed_query.get("id"),
        }

    if not results and not executed_query:
        raise AirflowException(f"неожиданный ответ DLP API без results/executed_query: {response!r}")

    if errors:
        # Ошибка есть, но не похожа на целевую — это провал таска.
        raise AirflowException(f"запрос завершился нецелевой ошибкой: {errors!r}")

    # Ошибок нет: запрос выполнился успешно (не как ожидалось) — предупреждаем.
    logger.warning(
        "запрос выполнился без ожидаемой целевой ошибки %s (executed_query.status=%s)",
        TARGET_ERROR_CODE,
        executed_query.get("status"),
    )
    rows = 0
    for result in results:
        for event in result.get("events") or []:
            if event.get("event") == "row":
                rows += 1
    return {"launch_date": launch_date, "target_error": False, "rows": rows}


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_example",
    description=(
        "Каждые 5 минут выполняет сохранённый SQL-запрос DLP (preprod) "
        "с параметром launch_date в формате dd-mm-yyyy"
    ),
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "dlp", "preprod", "etl"],
) as dag:
    run_sql_query_task = PythonOperator(
        task_id="run_sql_query",
        python_callable=run_sql_query,
    )
