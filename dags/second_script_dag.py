"""Второй DAG: запускается раз в 7 минут и печатает приветствие."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("hello world. I am second script")
    # Возвращаемое значение автоматически кладётся в XCom (ключ "return_value")
    status = "success"
    return status


def print_second_part(ti):
    print("hello world. I am second part of second script")
    # Забираем статус первого оператора из XCom
    status = ti.xcom_pull(task_ids="print_hello")
    print(f"status from print_hello: {status}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="second_script",
    description="Печатает приветствие каждые 7 минут",
    default_args=default_args,
    schedule="*/7 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    second_part_task = PythonOperator(
        task_id="print_second_part",
        python_callable=print_second_part,
    )

    # Второй оператор запускается только при успешном выполнении первого
    hello_task >> second_part_task
