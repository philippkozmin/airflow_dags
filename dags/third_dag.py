"""Третий DAG: запускается раз в 5 минут и печатает 'hello world' в трёх тасках."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello_1():
    print("hello world 1")


def print_hello_2():
    print("hello world 2")


def print_hello_3():
    print("hello world 3")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="third_dag",
    description="Печатает 'hello world' 1, 2, 3 каждые 5 минут",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    hello_task_1 = PythonOperator(
        task_id="print_hello_1",
        python_callable=print_hello_1,
    )

    hello_task_2 = PythonOperator(
        task_id="print_hello_2",
        python_callable=print_hello_2,
    )

    hello_task_3 = PythonOperator(
        task_id="print_hello_3",
        python_callable=print_hello_3,
    )

    # Таски выполняются последовательно
    hello_task_1 >> hello_task_2 >> hello_task_3
