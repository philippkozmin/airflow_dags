"""SDK-модуль для DataLens Platform (DLP) RPC API.

Python-эквивалент MCP-инструмента ``run_sql_query``: выполняет сохранённый
SQL-запрос DLP через ``POST {base}/rpc/runSqlQuery`` (заголовки ``x-dl-org-id``,
``x-dl-api-version: 3``, ``authorization: Bearer <IAM token>``). IAM-токен
сервисного аккаунта выпускается из authorized key через yc CLI.

Модуль лежит в dags-folder и импортируется DAG'ами: объектов DAG внутри нет,
CLI-смок-тест выполняется только при прямом запуске файла. Только stdlib.
"""

import argparse
import json
import os
import subprocess
import urllib.error
import urllib.request
from typing import Any

DEFAULT_KEY_FILE = "/Users/philippkozmin/github/dlp-demo/authorized_key.json"

YC_CLI = "yc"
YC_ENDPOINT = "api.cloud-preprod.yandex.net:443"

DEFAULT_ORG_ID = "yc.organization-manager.sandbox"
DEFAULT_ENVIRONMENT = "preprod"
DEFAULT_TIMEOUT_S = 300.0

API_BASE_URLS = {
    "prod": "https://api.datalens.tech",
    "preprod": "https://api.preprod.datalens.tech",
}


def get_iam_token(key_file: str = DEFAULT_KEY_FILE) -> str:
    """Выпускает IAM-токен сервисного аккаунта из authorized key через yc CLI.

    Использует env-переменную ``YC_SERVICE_ACCOUNT_KEY_FILE`` (флаг
    ``--key-file`` у используемой версии yc отсутствует) и endpoint preprod.
    Возвращает токен без хвостового перевода строки; ошибки subprocess
    превращаются в исключение с текстом stderr.
    """
    env = dict(os.environ)
    env["YC_SERVICE_ACCOUNT_KEY_FILE"] = key_file
    command = [YC_CLI, "--endpoint", YC_ENDPOINT, "iam", "create-token"]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
    except OSError as exc:
        raise RuntimeError(f"не удалось запустить yc CLI ({YC_CLI}): {exc}") from exc
    stderr = completed.stderr.strip()
    if completed.returncode != 0:
        raise RuntimeError(
            f"yc iam create-token завершился с кодом {completed.returncode}: {stderr or '(пустой stderr)'}"
        )
    token = completed.stdout.strip()
    if not token:
        raise RuntimeError(f"yc iam create-token вернул пустой токен: {stderr or '(пустой вывод)'}")
    return token


def _normalize_params(params: dict[str, Any]) -> dict[str, Any]:
    """Проверяет, что значения params — str | int | float | bool | None."""
    if not isinstance(params, dict):
        raise TypeError("params должен быть dict: имя -> str | int | float | bool | None")
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        if value is None or isinstance(value, (bool, int, float, str)):
            normalized[key] = value
        else:
            raise TypeError(
                f"params[{key!r}]: ожидается str | int | float | bool | None, "
                f"получено {type(value).__name__}"
            )
    return normalized


def run_sql_query(
    sql_query_id: str,
    params: dict[str, Any] | None = None,
    *,
    iam_token: str,
    org_id: str = DEFAULT_ORG_ID,
    environment: str = DEFAULT_ENVIRONMENT,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict:
    """Выполняет сохранённый SQL-запрос DLP: ``POST {base}/rpc/runSqlQuery``.

    Окружение: ``preprod`` → https://api.preprod.datalens.tech (по умолчанию),
    ``prod`` → https://api.datalens.tech. Тело запроса — ``{"sqlQueryId": ...}``,
    при заданном ``params`` добавляется ``"params": {...}`` со значениями
    str | int | float | bool | None. Возвращает распарсенный JSON-ответ (dict);
    при не-2xx бросает исключение с кодом и телом ответа. Токен в исключения
    и логи не попадает.
    """
    base_url = API_BASE_URLS.get(environment)
    if base_url is None:
        raise ValueError(
            f"неизвестное окружение {environment!r}: ожидается одно из {', '.join(sorted(API_BASE_URLS))}"
        )
    if not iam_token:
        raise ValueError("iam_token обязателен")
    body: dict[str, Any] = {"sqlQueryId": sql_query_id}
    if params is not None:
        body["params"] = _normalize_params(params)
    url = f"{base_url}/rpc/runSqlQuery"
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "content-type": "application/json",
            "x-dl-org-id": org_id,
            "x-dl-api-version": "3",
            "authorization": f"Bearer {iam_token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace").strip()
        raise RuntimeError(
            f"HTTP {exc.code} {exc.reason} от {url}: {error_body or '(пустое тело ответа)'}"
        ) from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"запрос к {url} не удался: {exc.reason}") from None
    return json.loads(payload)


def count_result_rows(response: dict) -> int:
    """Считает события ``"event": "row"`` в ``response["results"][*]["events"]``."""
    rows = 0
    for result in response.get("results") or []:
        for event in result.get("events") or []:
            if event.get("event") == "row":
                rows += 1
    return rows


def _parse_cli_param(raw: str) -> tuple[str, Any]:
    """Разбирает аргумент CLI вида ``key=value`` в пару (имя, значение).

    Значение сначала пытается интерпретироваться как JSON (числа, true/false,
    null), при неудаче берётся как строка.
    """
    key, separator, value = raw.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError(f"ожидается формат key=value, получено: {raw!r}")
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return key, value
    if parsed is None or isinstance(parsed, (bool, int, float, str)):
        return key, parsed
    return key, value


def _main(argv: list[str] | None = None) -> int:
    """CLI-смок-тест: печатает статус из ``executedQuery.status`` и число строк."""
    parser = argparse.ArgumentParser(
        description="Смок-тест DLP SDK: выполняет сохранённый SQL-запрос через runSqlQuery.",
    )
    parser.add_argument("--sql-query-id", required=True, help="id сохранённого SQL-запроса DLP")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="параметр запроса (str|int|float|bool|null), можно повторять",
    )
    parser.add_argument(
        "--key-file",
        default=DEFAULT_KEY_FILE,
        help=f"путь к authorized key сервисного аккаунта (по умолчанию {DEFAULT_KEY_FILE})",
    )
    args = parser.parse_args(argv)
    params: dict[str, Any] = {}
    for raw in args.param:
        key, value = _parse_cli_param(raw)
        params[key] = value
    try:
        iam_token = get_iam_token(args.key_file)
        response = run_sql_query(args.sql_query_id, params or None, iam_token=iam_token)
    except (RuntimeError, ValueError, TypeError) as exc:
        print(f"error: {exc}", flush=True)
        return 1
    executed_query = response.get("executedQuery") or response.get("executed_query") or {}
    status = executed_query.get("status")
    print(f"status: {status}", flush=True)
    print(f"rows: {count_result_rows(response)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
