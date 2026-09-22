"""Hourly refresh of scale2026-restcatalog demo layers through saved DLP SQL.

SQL sources: scripts/dlp_dag_example/*.sql in this repository.
Worker authentication: https://yandex.cloud/ru/docs/managed-airflow/operations/get-iam-token
The existing dlp_sdk_preprod uses the worker-network preprod endpoint on port 20197.
"""

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

import dlp_sdk_preprod as dlp_sdk

LOGGER = logging.getLogger(__name__)
ORG_ID = "aatjshkh6qiphjpq10tv"
QUERY_IDS = {
    "source_to_ods": "24b8g34xz81il",
    "ods_to_dds": "46dai9usofw2n",
    "dds_to_marts": "uw3082o0uhbqd",
}


def run_layer(sql_query_id):
    # Get the worker service-account credential only at execution time.
    import yandexcloud

    sdk = yandexcloud.SDK()
    token = sdk._channels._token_requester.get_token()
    response = dlp_sdk.run_sql_query(
        sql_query_id,
        iam_token=token,
        org_id=ORG_ID,
        environment="preprod",
        timeout=300,
    )
    execution = response.get("executed_query") or response.get("executedQuery") or response
    results = response.get("results") or execution.get("results") or []
    status = execution.get("status")
    if status != "success" or not results:
        raise RuntimeError(f"DLP query {sql_query_id} did not complete successfully")
    for result in results:
        if result.get("error") or result.get("status") not in (None, "success"):
            raise RuntimeError(f"DLP query {sql_query_id} returned a failed statement")
        if any(event.get("event") == "error" for event in result.get("events", [])):
            raise RuntimeError(f"DLP query {sql_query_id} returned an error event")
    LOGGER.info("DLP query %s completed successfully", sql_query_id)


with DAG(
    dag_id="dlp_dag_example",
    description="Hourly demo orders refresh: source -> ods -> dds -> marts",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 9, 22, tz="Europe/Moscow"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=45),
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["dlp", "preprod", "demo"],
) as dag:
    source_to_ods = PythonOperator(
        task_id="source_to_ods",
        python_callable=run_layer,
        op_kwargs={"sql_query_id": QUERY_IDS["source_to_ods"]},
        do_xcom_push=False,
    )
    ods_to_dds = PythonOperator(
        task_id="ods_to_dds",
        python_callable=run_layer,
        op_kwargs={"sql_query_id": QUERY_IDS["ods_to_dds"]},
        do_xcom_push=False,
    )
    dds_to_marts = PythonOperator(
        task_id="dds_to_marts",
        python_callable=run_layer,
        op_kwargs={"sql_query_id": QUERY_IDS["dds_to_marts"]},
        do_xcom_push=False,
    )
    source_to_ods >> ods_to_dds >> dds_to_marts
