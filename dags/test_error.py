"""Тестовый DAG для проверки отображения ошибок импорта в интерфейсе Airflow."""

import non_existent_module_xyz  # noqa: F401

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="test_error",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    EmptyOperator(task_id="noop")
