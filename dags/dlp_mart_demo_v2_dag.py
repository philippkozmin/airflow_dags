"""DAG DLP_MART_DEMO_v2: временная отладочная копия DLP_MART_DEMO.

Отличие от DLP_MART_DEMO ровно одно: вместо IAM-токена, получаемого в рантайме
на воркере (yandexcloud.SDK()._channels._token_requester.get_token()),
подставлен статический IAM-токен сервисного аккаунта dlp-airflow-profile,
проверенный локально (запрос ecgnn7kizpbax через api.preprod.datalens.tech
вернул status=success). Цель — проверить со стороны Airflow вызов с этим
токеном (dlp_sdk сейчас ходит в preprod через порт :20197).

ВНИМАНИЕ: токен зашит в код и живёт не дольше 12 часов; после отладки этот
DAG удалить (токен останется в истории git — это разовая отладка в preprod
sandbox, не прод-секрет).
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import dlp_sdk

logger = logging.getLogger(__name__)

SQL_QUERY_ID = "ecgnn7kizpbax"
IAM_TOKEN = (
    "t1.9eudmZ2Vy5GQncadx5zNlJCVl8mTmu3rnZmdlJqSj56cioyUjsrGi5CWlcnl8_cVNxsr-e8pEig2_"
    "d3z91VlGCv57ykSKDb9zef1652ZnZKRjsvOmI6KnY7MiZSPx52Z7_zN5_XrnZmdnZSZyo7PzZfOlZSbk8-"
    "TjMfv_cXrnZmdkpGOy86YjoqdjsyJlI_HnZk."
    "huYeMAepdp75rSPajIiLqdZk2zqJ5HpkA82RMsYH4f0NhcJn9U913uAbkzQY3qhtjCZYv3VxiAzYpWJoN6akAA"
)


def run_sql_query():
    response = dlp_sdk.run_sql_query(
        SQL_QUERY_ID,
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
    dag_id="DLP_MART_DEMO_v2",
    description="Отладочная копия DLP_MART_DEMO со статическим токеном (временный, удалить после теста)",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "dlp", "preprod", "test"],
) as dag:
    run_sql_query_task = PythonOperator(
        task_id="run_sql_query",
        python_callable=run_sql_query,
    )
