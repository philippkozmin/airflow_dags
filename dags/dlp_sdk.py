"""Тонкая обёртка над DLP RPC API: выполняет сохранённый SQL-запрос.
Делает POST {base}/rpc/runSqlQuery и возвращает ответ API.

При ошибках логирует диагностику: HTTP-код, тело, заголовки ответа
(включая x-request-id — по нему DLP может найти запрос в своих логах),
резолвнутый IP хоста API. Если заголовков/тела с описанием ошибки нет,
скорее всего запрос не дошёл до DLP (LB/прокси/DNS по пути).
"""

import json
import logging
import socket
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

ORG_ID = "yc.organization-manager.sandbox"
API_BASE_URLS = {"prod": "https://api.datalens.tech", "preprod": "https://api.preprod.datalens.tech"}


def _resolve_host(url):
    """Вернуть (host, ip) из URL или (host, '<resolve error>') при неудаче."""
    host = urllib.request.urlparse(url).hostname
    try:
        return host, socket.gethostbyname(host)
    except OSError as exc:
        return host, f"resolve error: {exc}"


def _log_http_error(exc, url, body):
    """Логировать максимум диагностики по HTTPError/URLError."""
    host, ip = _resolve_host(url)
    logger.error("DLP request failed: url=%s host=%s ip=%s", url, host, ip)
    if isinstance(exc, urllib.error.HTTPError):
        headers = dict(exc.headers.items()) if exc.headers else {}
        response_body = ""
        try:
            response_body = exc.read().decode(errors="replace")
        except Exception:
            response_body = "<body read error>"
        logger.error(
            "HTTP %s: headers=%s body=%s",
            exc.code,
            json.dumps(headers, ensure_ascii=False),
            response_body or "<empty body>",
        )
        # Пустое тело и отсутствие requestId — признак, что ответ отдал не DLP,
        # а что-то по сетевому пути (LB/прокси): их сигнатуры не такие.
        request_id = (exc.headers or {}).get("x-request-id") if exc.headers else None
        if not response_body and not request_id:
            logger.error(
                "Пустой ответ без x-request-id: запрос, вероятно, не дошёл до DLP API "
                "(ответ от LB/прокси). Проверьте DNS/маршрут до %s (ip=%s) с воркера.",
                host,
                ip,
            )
    else:
        logger.error("Сетевая ошибка до HTTP-запроса: %r", exc)


def run_sql_query(sql_query_id, params=None, *, iam_token, org_id=ORG_ID, environment="preprod", timeout=300):
    """Выполняет сохранённый SQL-запрос: POST {base}/rpc/runSqlQuery."""
    if environment not in API_BASE_URLS:
        raise ValueError(f"неизвестное окружение {environment!r}: {', '.join(sorted(API_BASE_URLS))}")
    body = {"sqlQueryId": sql_query_id}
    if params is not None:
        body["params"] = params
    url = f"{API_BASE_URLS[environment]}/rpc/runSqlQuery"
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
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        _log_http_error(exc, url, body)
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode(errors='replace') if exc.fp else ''}") from None
    except urllib.error.URLError as exc:
        _log_http_error(exc, url, body)
        raise RuntimeError(f"URL error: {exc!r}") from None
