"""Ежечасный DAG: PySpark hello-world через Spark Connect на Managed Spark.

Каждый час DAG сам поднимает временный Spark Connect job на кластере
c9qnmr0ifrus5ks50jup, дожидается статуса RUNNING и появления top-level `connect_url`,
запускает скрипт hello_world_spark_worker.py (печатает "Hello world from Spark worker"
и df.show()) и затем ГАРАНТИРОВАННО гасит job (trigger_rule="all_done"), чтобы не
оставлять висящих задач.

Скрипт сам job не создаёт/не гасит — он лишь читает две переменные окружения:
  SPARK_CONNECT_URL — sc://…:443 работающего Spark Connect job;
  IAM_TOKEN         — свежий Yandex Cloud IAM токен.
Поэтому жизненным циклом job управляет этот DAG.

Предположения об окружении Airflow worker (см. также scheduler_result.md):
  * `yc` CLI установлен и аутентифицирован на worker (тот же профиль, что и на хосте,
    где собирался DAG);
  * доступен скрипт получения токена GET_IAM_TOKEN_SH;
  * доступен venv с PySpark 4.0.0 на CPython 3.13 по пути SPARK_VENV_PYTHON
    (кластер Spark 4.0.0 требует совпадения minor-версии Python драйвера);
  * целевой скрипт доставлен в этот же репозиторий (scripts/hello_world_spark_worker.py)
    и резолвится относительно DAG-файла — отдельная выкладка скрипта не нужна.
Остальные пути (venv, скрипт токена) вынесены в константы ниже — при переносе на
другой worker правьте их здесь.
Токен НИКОГДА не хардкодится и не пишется в конфиг — он получается в рантайме.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# --- Пути и идентификаторы (правьте под окружение worker) --------------------
CLUSTER_ID = "c9qnmr0ifrus5ks50jup"
# Целевой скрипт ДОСТАВЛЕН в этот же репозиторий (airflow_dags/scripts/) и
# резолвится относительно расположения самого DAG-файла. Поэтому путь валиден на
# Airflow worker при любом месте выкладки репозитория — не абсолютный путь чужого
# репозитория ouroboros. Каталог scripts/ лежит вне dags-folder, поэтому Airflow
# не пытается импортировать его как DAG.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER_SCRIPT = os.path.join(_REPO_ROOT, "scripts", "hello_world_spark_worker.py")
SPARK_VENV_PYTHON = "/Users/philippkozmin/.spark-connect-venv/bin/python"
GET_IAM_TOKEN_SH = (
    "/Users/philippkozmin/github/datalensplatform-harness-claudecode/"
    "tools/get-iam-token/scripts/get_iam_token.sh"
)

default_args = {
    "owner": "airflow",
    # 0 повторов намеренно: каждый ретрай create_job поднял бы лишний Spark job.
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# Имя job детерминировано в пределах одного запуска DAG (ts_nodash стабилен между
# ретраями), поэтому poll/teardown находят его по имени без разбора async-операции.
JOB_NAME_EXPR = "hw-spark-worker-{{ ts_nodash | lower }}"

# 1) Создать Spark Connect job асинхронно; последней строкой stdout печатаем имя job
#    (уходит в XCom), диагностику yc отправляем в stderr.
CREATE_CMD = f"""
set -euo pipefail
JOB_NAME="{JOB_NAME_EXPR}"
echo "[create] creating spark-connect job name=$JOB_NAME cluster={CLUSTER_ID}" 1>&2
yc managed-spark job create-spark-connect \
    --cluster-id "{CLUSTER_ID}" \
    --name "$JOB_NAME" \
    --async --format json 1>&2
echo "$JOB_NAME"
"""

# 2) Опросить job до status==RUNNING И наличия top-level connect_url (~3 мин).
#    Последней строкой stdout печатаем connect_url (уходит в XCom).
POLL_CMD = f"""
set -euo pipefail
JOB_NAME="{{{{ ti.xcom_pull(task_ids='create_job') }}}}"
if [ -z "$JOB_NAME" ]; then echo "[poll] empty job name from XCom" 1>&2; exit 1; fi
CONNECT_URL=""
for i in $(seq 1 60); do
    JSON="$(yc managed-spark job get --name "$JOB_NAME" --cluster-id "{CLUSTER_ID}" --format json)"
    STATUS="$(printf '%s' "$JSON" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("status",""))')"
    echo "[poll] attempt=$i status=$STATUS" 1>&2
    case "$STATUS" in
        RUNNING)
            CONNECT_URL="$(printf '%s' "$JSON" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("connect_url",""))')"
            if [ -n "$CONNECT_URL" ]; then break; fi
            ;;
        ERROR|DONE|CANCELLED|CANCELLING)
            echo "[poll] terminal status $STATUS before connect_url appeared" 1>&2
            exit 1
            ;;
    esac
    sleep 10
done
if [ -z "$CONNECT_URL" ]; then echo "[poll] timed out waiting for RUNNING + connect_url" 1>&2; exit 1; fi
echo "[poll] RUNNING; connect_url=$CONNECT_URL" 1>&2
echo "$CONNECT_URL"
"""

# 3) Запустить скрипт с SPARK_CONNECT_URL (из XCom) и свежим IAM_TOKEN (рантайм).
RUN_CMD = f"""
set -euo pipefail
export SPARK_CONNECT_URL="{{{{ ti.xcom_pull(task_ids='poll_job') }}}}"
if [ -z "$SPARK_CONNECT_URL" ]; then echo "[run] empty SPARK_CONNECT_URL from XCom" 1>&2; exit 1; fi
export IAM_TOKEN="$({GET_IAM_TOKEN_SH})"
echo "[run] launching {WORKER_SCRIPT} via {SPARK_VENV_PYTHON}" 1>&2
"{SPARK_VENV_PYTHON}" "{WORKER_SCRIPT}"
"""

# 4) Всегда гасим job (all_done), чтобы не оставлять висящих Spark-задач.
TEARDOWN_CMD = f"""
set -euo pipefail
JOB_NAME="{{{{ ti.xcom_pull(task_ids='create_job') }}}}"
if [ -n "$JOB_NAME" ]; then
    echo "[teardown] cancelling job $JOB_NAME" 1>&2
    yc managed-spark job cancel --name "$JOB_NAME" --cluster-id "{CLUSTER_ID}" 1>&2 \
        || echo "[teardown] cancel failed or job already gone" 1>&2
else
    echo "[teardown] no job name in XCom; nothing to cancel" 1>&2
fi
echo "teardown-done"
"""

with DAG(
    dag_id="hello_world_spark_worker",
    description="Ежечасно запускает PySpark hello-world через временный Spark Connect job",
    default_args=default_args,
    schedule="@hourly",  # эквивалент cron "0 * * * *"
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # не накладывать запуски друг на друга (старт job ~3 мин)
    tags=["spark", "spark-connect", "managed-spark"],
) as dag:
    create_job = BashOperator(
        task_id="create_job",
        bash_command=CREATE_CMD,
        execution_timeout=timedelta(minutes=5),
    )

    poll_job = BashOperator(
        task_id="poll_job",
        bash_command=POLL_CMD,
        execution_timeout=timedelta(minutes=12),
    )

    run_script = BashOperator(
        task_id="run_script",
        bash_command=RUN_CMD,
        execution_timeout=timedelta(minutes=15),
    )

    teardown = BashOperator(
        task_id="teardown",
        bash_command=TEARDOWN_CMD,
        trigger_rule="all_done",  # гасим job даже если предыдущие шаги упали
        execution_timeout=timedelta(minutes=5),
    )

    create_job >> poll_job >> run_script >> teardown
