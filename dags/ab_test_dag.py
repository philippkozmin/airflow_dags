"""DAG AB_test: ежедневный A/B-расчёт на Spark (DLP preprod, Spark Connect).

Раз в сутки (cron "0 0 * * *", 00:00 UTC) считает A/B-тест по таблице
`dlback-test-catalog-10`.marts.mart_orders_ab_test (группы A/B, бутстрап
1000 итераций на executor'ах) на кластере Spark_Philipp_test_with_rest
(DLP id b6pce1s7bu1gj3iakvl1) через эфемерную SparkConnect-джобу, результат
пишется CTAS в `dlback-test-catalog-10`.marts.orders_ab_result.

Таски (последовательная логика, PythonOperator):
  create_session   — IAM-токен → DLP RPC createSparkJob (имя ab-test-daily,
                      каталог bu6cinhpkq0jsb0p1aop аттачится к джобе и становится
                      defaultCatalog); в лог пишется createdBy операции и джобы —
                      id DLP-пользователя, от имени которого выполняются запросы
                      (сам токен в лог не пишется); далее поллинг
                     listSparkJobs до connectUrl (таймаут ~10 мин, повторы
                     сетевых вызовов); jobId+connectUrl уходят в XCom. Перед
                     созданием гасит висящие джобы с тем же именем
                     (cancelSparkJob);
  run_ab_analysis  — subprocess с python воркера: scripts/
                     orders_ab_test_analysis.py (env CONNECT_URL / IAM_TOKEN /
                     GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=scripts/certs/
                     yc_internal_root.pem); скрипт подключается по connectUrl,
                     считает бутстрап (applyInPandas + numpy, при
                     несовместимости версии Python — stdlib-fallback),
                     делает CTAS + read-back и печатает AB_RESULT_JSON=...;
                     метрики логируются и уходят в XCom;
  teardown_session — trigger_rule=all_done, ЛИСТОВОЙ таск: cancelSparkJob
                      джобы из XCom с подтверждением статуса + пропагация
                      падений: если любая другая таска запускаа зафейлилась,
                      teardown роняет и себя => DagRun получает статус error
                      (иначе успешный leaf на all_done «прощал» бы сбои и даг
                      становился SUCCESS);

RPC-вызовы к DLP API (preprod: https://api.preprod.datalens.tech:20197 —
порт 20197 обязателен из сети воркера Managed Airflow; org
yc.organization-manager.sandbox) делает stdlib urllib.request, как в dlp_sdk;
requests/pyspark на этапе оркестрации не используются. IAM-токен получается
в рантайме на воркере от сервисного аккаунта кластера
(yandexcloud.SDK()._channels._token_requester.get_token(), пакет yandexcloud
предустановлен). Статических токенов в коде нет.
"""

import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

import yandexcloud
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CLUSTER_ID = "b6pce1s7bu1gj3iakvl1"          # Spark_Philipp_test_with_rest
CATALOG_ID = "bu6cinhpkq0jsb0p1aop"           # dlback-test-catalog-10
JOB_NAME = "ab-test-daily"                    # [a-z][-a-z0-9]{1,62}[a-z0-9]
BOOTSTRAP_ITERS = int(os.getenv("AB_BOOTSTRAP_ITERS", "1000"))

# Base DLP API (preprod): из сети воркера Managed Airflow API доступен на порту
# 20197 — он первичен (https://api.preprod.datalens.tech:20197). Base 443 —
# запасной (работает извне, например с ноутбука). Рабочий base запоминается
# после первого успешного вызова. Override: AB_DLP_API_BASE.
API_BASE_FALLBACK = ["https://api.preprod.datalens.tech:20197", "https://api.preprod.datalens.tech"]
ORG_ID = "yc.organization-manager.sandbox"

SESSION_WAIT_SEC = 600   # ~10 мин на подъём SparkConnect-джобы
CANCEL_WAIT_SEC = 120    # ожидание погашения висящей джобы с тем же именем
POLL_INTERVAL_SEC = 15
RPC_TIMEOUT_SEC = 60
RPC_RETRIES = 3
TERMINAL_STATUSES = {"DONE", "ERROR", "CANCELLED", "FINISHED"}

# Пути внутри репо: DAG лежит в <repo>/dags/, скрипт и PEM — в <repo>/scripts/.
# На воркере репо может быть смонтирован и корнем в dags-папку — поэтому
# кандидаты покрывают обе раскладки (абсолютные пути YC-воркера добавлены
# на случай нестандартного cwd).
_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT_CANDIDATES = [
    os.path.join(_HERE, "..", "scripts", "orders_ab_test_analysis.py"),
    os.path.join(_HERE, "scripts", "orders_ab_test_analysis.py"),
    "/opt/airflow/dags/scripts/orders_ab_test_analysis.py",
    "/opt/airflow/dags/dags/scripts/orders_ab_test_analysis.py",
]
_PEM_CANDIDATES = [
    os.path.join(_HERE, "..", "scripts", "certs", "yc_internal_root.pem"),
    os.path.join(_HERE, "scripts", "certs", "yc_internal_root.pem"),
    "/opt/airflow/dags/scripts/certs/yc_internal_root.pem",
    "/opt/airflow/dags/dags/scripts/certs/yc_internal_root.pem",
]


def get_iam_token():
    """IAM-токен сервисного аккаунта кластера Managed Airflow (runtime)."""
    sdk = yandexcloud.SDK()
    return sdk._channels._token_requester.get_token()


_working_api_base = None  # запоминаем base, ответивший первым (в рамках процесса воркера)


def _api_bases():
    override = os.getenv("AB_DLP_API_BASE")
    if override:
        return [override]
    return [_working_api_base] if _working_api_base else API_BASE_FALLBACK


def dlp_rpc(method, payload, iam_token, timeout=RPC_TIMEOUT_SEC):
    """POST {base}/rpc/{method} (stdlib urllib, как dlp_sdk). Кандидаты base
    пробуются по кругу, сетевые ошибки/5xx ретраятся; 4xx — fail-fast.
    Диагностика ошибок — в лог."""
    global _working_api_base
    last_error = None
    for attempt in range(1, RPC_RETRIES + 1):
        for base in _api_bases():
            url = f"{base}/rpc/{method}"
            request = urllib.request.Request(
                url,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "content-type": "application/json",
                    "x-dl-org-id": ORG_ID,
                    "x-dl-api-version": "3",
                    "authorization": f"Bearer {iam_token}",
                },
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    _working_api_base = base
                    return json.loads(response.read())
            except urllib.error.HTTPError as exc:
                body = ""
                try:
                    body = exc.read().decode(errors="replace")
                except Exception:
                    pass
                if exc.code < 500:
                    raise RuntimeError(f"RPC {method} [{base}]: HTTP {exc.code}: {body[:2000]}") from None
                last_error = f"[{base}] HTTP {exc.code}: {body[:500]}"
                logger.warning("RPC %s attempt %d/%d @ %s failed: %s",
                               method, attempt, RPC_RETRIES, base, last_error)
            except urllib.error.URLError as exc:
                last_error = f"[{base}] {exc!r}"
                logger.warning("RPC %s attempt %d/%d @ %s failed: %r",
                               method, attempt, RPC_RETRIES, base, exc)
        time.sleep(min(5 * attempt, 15))
    raise RuntimeError(f"RPC {method} failed after {RPC_RETRIES} attempts: {last_error}")


def _list_jobs(iam_token):
    """listSparkJobs кластера: список живых и завершённых джоб."""
    response = dlp_rpc("listSparkJobs", {"clusterId": CLUSTER_ID, "pageSize": 100}, iam_token)
    return response.get("jobs") or []


def _live_jobs(jobs):
    """Джобы с именем JOB_NAME, ещё не дошедшие до терминального статуса."""
    return [j for j in jobs if j.get("name") == JOB_NAME and j.get("status") not in TERMINAL_STATUSES]


def _wait_jobs_gone(iam_token, jobs0, wait_sec, what):
    """Ждать, пока перечисленные джобы не станут терминальными (cancel —
    асинхронная операция)."""
    deadline = time.time() + wait_sec
    pending_ids = {j.get("id") for j in jobs0}
    while time.time() < deadline and pending_ids:
        jobs = [j for j in _list_jobs(iam_token) if j.get("id") in pending_ids]
        pending_ids = {j.get("id") for j in _live_jobs(jobs)}
        if pending_ids:
            logger.info("%s: waiting for %s to terminate ...", what, sorted(pending_ids))
            time.sleep(POLL_INTERVAL_SEC)
    if pending_ids:
        logger.warning("%s: jobs %s still not terminal after %ss", what, sorted(pending_ids), wait_sec)


def create_session(**context):
    """Таск 1: создать SparkConnect-джобу и дождаться connectUrl."""
    iam_token = get_iam_token()

    # 0) погасить висящие джобы с тем же именем от прошлых запусков
    stale = _live_jobs(_list_jobs(iam_token))
    for job in stale:
        logger.info("cancelling stale job %s (status=%s)", job.get("id"), job.get("status"))
        dlp_rpc("cancelSparkJob", {"clusterId": CLUSTER_ID, "jobId": job["id"]}, iam_token)
    if stale:
        _wait_jobs_gone(iam_token, stale, CANCEL_WAIT_SEC, "stale cleanup")

    # 1) создать джобу (sparkConnectJob + REST-каталог → defaultCatalog)
    operation = dlp_rpc(
        "createSparkJob",
        {
            "clusterId": CLUSTER_ID,
            "name": JOB_NAME,
            "catalogs": [{"catalogId": CATALOG_ID}],
            "sparkConnectJob": {},
        },
        iam_token,
    )
    operation_id = operation.get("id") if isinstance(operation, dict) else None
    logger.info("createSparkJob: operation=%s done=%s createdBy=%s (DLP user id, от имени которого Airflow выполняет запросы)",
                operation_id, operation.get("done"), operation.get("createdBy"))

    # 2) поллинг операции до done, затем listSparkJobs до connectUrl
    deadline = time.time() + SESSION_WAIT_SEC
    while time.time() < deadline:
        if operation_id and not operation.get("done"):
            operation = dlp_rpc("getLakehouseOperation", {"operationId": operation_id}, iam_token)
            logger.info("operation %s: done=%s", operation_id, operation.get("done"))
            if operation.get("done") and operation.get("error"):
                raise RuntimeError(f"createSparkJob operation failed: {operation['error']}")
        for job in _live_jobs(_list_jobs(iam_token)):
            connect_url = job.get("connectUrl")
            if connect_url:
                logger.info("session ready: jobId=%s status=%s createdBy=%s connectUrl=%s",
                            job.get("id"), job.get("status"), job.get("createdBy"), connect_url)
                return {"jobId": job["id"], "connectUrl": connect_url}
        time.sleep(POLL_INTERVAL_SEC)
    raise TimeoutError(f"SparkConnect session {JOB_NAME!r} not ready in {SESSION_WAIT_SEC}s")


def _locate(candidates, what):
    for path in candidates:
        if os.path.isfile(path):
            return os.path.abspath(path)
    raise FileNotFoundError(f"{what} not found in any of: {candidates}")


def run_ab_analysis(**context):
    """Таск 2: прогнать A/B-расчёт в отдельном python-процессе воркера."""
    ti = context["ti"]
    session = ti.xcom_pull(task_ids="create_session")
    if not session:
        raise RuntimeError("no session in XCom from create_session")

    script = _locate(_SCRIPT_CANDIDATES, "orders_ab_test_analysis.py")
    pem = _locate(_PEM_CANDIDATES, "yc_internal_root.pem")
    logger.info("script=%s pem=%s connectUrl=%s", script, pem, session["connectUrl"])

    env = os.environ.copy()
    env.update({
        "IAM_TOKEN": get_iam_token(),
        "CONNECT_URL": session["connectUrl"],
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": pem,
        "BOOTSTRAP_ITERS": str(BOOTSTRAP_ITERS),
        "PYTHONUNBUFFERED": "1",
    })

    # python воркера (или AB_PYTHON для локальной отладки DAG-хелперов)
    python_bin = os.getenv("AB_PYTHON", sys.executable)
    result = None
    with subprocess.Popen(
        [python_bin, "-u", script],
        env=env,
        cwd=os.path.dirname(script),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            line = line.rstrip()
            logger.info("[ab-script] %s", line)
            if line.startswith("AB_RESULT_JSON="):
                try:
                    result = json.loads(line[len("AB_RESULT_JSON="):])
                except json.JSONDecodeError:
                    logger.warning("unparseable AB_RESULT_JSON line ignored")
        returncode = proc.wait()

    if returncode != 0:
        raise RuntimeError(f"orders_ab_test_analysis.py exited with code {returncode}")
    if not result:
        raise RuntimeError("script finished OK but printed no AB_RESULT_JSON")

    summary = {
        "mode": result.get("mode"),
        "iterations": result.get("iterations"),
        "result_table": result.get("result_table"),
        "metrics": [
            {
                "metric_name": row["metric_name"],
                "ab_group": row["ab_group"],
                "metric_value": row.get("metric_value"),
                "rel_diff_pct": row.get("rel_diff_pct"),
                "p_value": row.get("p_value"),
                "is_significant": row.get("is_significant"),
            }
            for row in result.get("rows", [])
            if row.get("ab_group") == "B"
        ],
    }
    logger.info("A/B result: mode=%s iterations=%s table=%s",
                summary["mode"], summary["iterations"], summary["result_table"])
    for row in summary["metrics"]:
        logger.info(
            "metric=%s B=%.4f rel_diff=%s%% p=%s significant=%s",
            row["metric_name"], row["metric_value"] or 0.0,
            row["rel_diff_pct"], row["p_value"], row["is_significant"],
        )
    return summary


def _fail_if_siblings_failed(ti):
    """Уронить leaf-таск (и вместе с ним весь DagRun → error), если любая другая
    таска этого запуска зафейлилась.

    Без этого успешный leaf-таск на trigger_rule=all_done «прощал» бы падения
    апстримов: Airflow выводит статус DagRun по листовым таскам, и teardown,
    отработавший cleanup, пометил бы запуск SUCCESS. Пропагация ошибки через
    лист гарантирует: любая упавшая таска => даг в статусе error."""
    from airflow.utils.state import TaskInstanceState

    failed = [
        t.task_id
        for t in ti.get_dagrun().get_task_instances()
        if t.task_id != ti.task_id and t.state == TaskInstanceState.FAILED
    ]
    if failed:
        raise AirflowException(
            "DAG AB_test завершён с ошибкой — упали таски: " + ", ".join(sorted(failed))
        )


def teardown_session(**context):
    """Таск 3 (all_done, листовой): погасить SparkConnect-джобу, подтвердить
    статус и пропагировать падение любой таски в статус дага (error)."""
    ti = context["ti"]
    try:
        session = ti.xcom_pull(task_ids="create_session")
        if not session:
            logger.info("no session in XCom — nothing to tear down")
            return
        job_id = session["jobId"]
        iam_token = get_iam_token()
        logger.info("cancelling SparkConnect job %s ...", job_id)
        operation = dlp_rpc("cancelSparkJob", {"clusterId": CLUSTER_ID, "jobId": job_id}, iam_token)
        logger.info("cancelSparkJob: operation=%s done=%s",
                    operation.get("id") if isinstance(operation, dict) else None,
                    operation.get("done") if isinstance(operation, dict) else None)

        # подтверждение: джоба должна стать терминальной (CANCELLED)
        _wait_jobs_gone(iam_token, [{"id": job_id}], CANCEL_WAIT_SEC, "teardown")
        for job in _list_jobs(iam_token):
            if job.get("id") == job_id:
                logger.info("teardown confirmed: job %s status=%s", job_id, job.get("status"))
                return
        logger.warning("teardown: job %s not found in listSparkJobs (treated as gone)", job_id)
    finally:
        # cleanup выполнен при любом исходе; теперь честно отчитываемся о сбоях
        _fail_if_siblings_failed(ti)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="AB_test",
    description="Раз в сутки считает A/B-тест на Spark (DLP preprod): бутстрап 1000 итераций, "
                "CTAS marts.orders_ab_result через эфемерную SparkConnect-джобу",
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["datalens", "spark", "ab"],
) as dag:
    create_session_task = PythonOperator(
        task_id="create_session",
        python_callable=create_session,
        execution_timeout=timedelta(minutes=15),
    )
    run_ab_analysis_task = PythonOperator(
        task_id="run_ab_analysis",
        python_callable=run_ab_analysis,
        retries=0,  # сессия после падения скрипта уже не гарантируется — cleaner перезапустить DAG целиком
        execution_timeout=timedelta(hours=2),
    )
    teardown_session_task = PythonOperator(
        task_id="teardown_session",
        python_callable=teardown_session,
        trigger_rule="all_done",  # гасим джобу при любом исходе расчёта
        execution_timeout=timedelta(minutes=10),
    )

    create_session_task >> run_ab_analysis_task >> teardown_session_task
