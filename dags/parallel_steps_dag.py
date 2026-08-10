"""Тестовый DAG с production-подобным графом для проверки Airflow UI."""

import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DATA_SOURCES = ("customers", "orders", "payments", "products")
QUALITY_CHECKS = ("completeness", "freshness", "consistency")
STEP_DURATION_SECONDS = 3


def print_hello(step_name):
    print(f"hello from {step_name}")
    time.sleep(STEP_DURATION_SECONDS)


with DAG(
    dag_id="parallel_steps",
    description="Production-подобный UI smoke-test с параллельными группами",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["example", "parallel", "ui-test"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_inputs = PythonOperator(
        task_id="validate_inputs",
        python_callable=print_hello,
        op_args=["input validation"],
    )

    with TaskGroup(group_id="extract") as extract:
        for source in DATA_SOURCES:
            PythonOperator(
                task_id=f"extract_{source}",
                python_callable=print_hello,
                op_args=[f"extract {source}"],
            )

    with TaskGroup(group_id="transform") as transform:
        for source in DATA_SOURCES:
            PythonOperator(
                task_id=f"transform_{source}",
                python_callable=print_hello,
                op_args=[f"transform {source}"],
            )

    aggregate_metrics = PythonOperator(
        task_id="aggregate_metrics",
        python_callable=print_hello,
        op_args=["metrics aggregation"],
    )

    with TaskGroup(group_id="quality") as quality:
        for check in QUALITY_CHECKS:
            PythonOperator(
                task_id=f"check_{check}",
                python_callable=print_hello,
                op_args=[f"quality check {check}"],
            )

    publish_dataset = PythonOperator(
        task_id="publish_dataset",
        python_callable=print_hello,
        op_args=["dataset publication"],
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=print_hello,
        op_args=["success notification"],
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_inputs
        >> extract
        >> transform
        >> aggregate_metrics
        >> quality
        >> publish_dataset
        >> notify_success
        >> finish
    )
