"""Простой DAG: запускается раз в 5 минут и печатает 'hello world'."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("hello world")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world",
    description="Печатает 'hello world' каждые 5 минут",
    default_args=default_args,
    schedule="*/6 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )
