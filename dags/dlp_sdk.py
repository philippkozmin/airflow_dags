"""Тонкая обёртка над DLP RPC API: выполняет сохранённый SQL-запрос.
Делает POST {base}/rpc/runSqlQuery и возвращает ответ API.
"""

import json
import urllib.request

ORG_ID = "yc.organization-manager.sandbox"
API_BASE_URLS = {"prod": "https://api.datalens.tech", "preprod": "https://api.preprod.datalens.tech"}


def run_sql_query(sql_query_id, params=None, *, iam_token, org_id=ORG_ID, environment="preprod", timeout=300):
    """Выполняет сохранённый SQL-запрос: POST {base}/rpc/runSqlQuery."""
    if environment not in API_BASE_URLS:
        raise ValueError(f"неизвестное окружение {environment!r}: {', '.join(sorted(API_BASE_URLS))}")
    body = {"sqlQueryId": sql_query_id}
    if params is not None:
        body["params"] = params
    request = urllib.request.Request(
        f"{API_BASE_URLS[environment]}/rpc/runSqlQuery",
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
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace')}") from None
