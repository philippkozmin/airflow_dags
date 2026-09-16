#!/usr/bin/env python3
"""
A/B-тест по таблице `dlback-test-catalog-10`.marts.mart_orders_ab_test (Spark Connect, DLP preprod).

Адаптация v8 под запуск с воркера Yandex Managed Airflow (DAG AB_test,
dags/ab_test_dag.py): скрипт сам НЕ создаёт и НЕ гасит SparkConnect-джобу —
оркестрация (createSparkJob → connectUrl → расчёт → cancelSparkJob) лежит на
DAG. Скрипт только подключается по готовому CONNECT_URL, считает, делает CTAS
результата `dlback-test-catalog-10`.marts.orders_ab_result, проверяет чтением
и выходит.

Параметры — ТОЛЬКО через env (секреты не хардкодятся):
  IAM_TOKEN         (обяз.)  IAM-токен; на воркере Managed Airflow DAG берёт
                             его у сервисного аккаунта кластера
                             (yandexcloud.SDK()._channels._token_requester
                             .get_token())
  CONNECT_URL       (обяз.)  sc://...:9443 от createSparkJob (ставит DAG);
                             для совместимости с ручным запуском принимается
                             и SPARK_CONNECT_URL
  BOOTSTRAP_ITERS   (опц.)   итераций бутстрапа на группу, дефолт 1000
  GRPC_DEFAULT_SSL_ROOTS_FILE_PATH (нужен gRPC-клиенту) — путь к PEM
                             публичного корня YC (scripts/certs/
                             yc_internal_root.pem в этом репо); ставит DAG

v8 (проверено 2026-09-16): бутстрап исполняется через
groupBy("ab_group").applyInPandas(...) с numpy непосредственно на executor'ах
кластера (pip: pandas/numpy/pyarrow). На клиент возвращаются только малые
агрегаты: наблюдаемые метрики и BOOTSTRAP_ITERS реплик на метрику (long-формат).

FALLBACK (graceful, зафиксирован по требованию): если applyInPandas падает
(например, минорная версия Python воркера Managed Airflow ≠ 3.10 воркеров
кластера → PYTHON_VERSION_MISMATCH, либо pandas/arrow недоступны на
executor'ах), скрипт НЕ падает, а пересчитывает бутстрап stdlib-вариантом
(логика v7): чистые random/fsum/set, БЕЗ numpy/pandas, UDF регистрируется
ТОЛЬКО через spark.udf.register (F.udf на функциях модуля несовместим с
Spark Connect), вычисление идёт на executor'ах чанками по 100 итераций на
один SQL-запрос — короткие gRPC-стримы (прокси Lakehouse обрывает долгие,
RST_STREAM). Seed детерминирован: SEED_BASE + crc32(группа) + номер чанка.
CTAS и read-back одинаковы для обоих путей.

Метрики по группам A (control) и B (target):
  - orders_cnt       — кол-во заказов (count order_id)
  - avg_order_value  — средний чек (avg order_amount)
  - uniq_customers   — кол-во уникальных клиентов (count distinct customer_id)
  - items_per_order  — ср. кол-во позиций на заказ (avg items_count)

Стат. значимость различий (metric_B - metric_A) — ТОЛЬКО бутстрап:
  ресемплинг заказов с возвращением внутри каждой группы.
  p_value  — односторонний тест в сторону улучшения B>A: доля реплик с diff<=0
             c +1-коррекцией (Phipson & Smyth): p = (1 + #{diff<=0}) / (N+1);
  ci_low/ci_high — перцентильный 95% CI разности metric_B - metric_A
             (перцентили считаются stdlib-линейной интерполяцией, как numpy);
  is_significant = p < 0.05.

Результат (long-формат) пишется CTAS в `dlback-test-catalog-10`.marts.orders_ab_result
и читается обратно для проверки. Последняя строка stdout:
  AB_RESULT_JSON={...}  — машиночитаемый итог (таблица, режим, метрики);
  его поднимает в XCom таск run_ab_analysis DAG'а AB_test.

Запуск (в рантайме всё ставит DAG; вручную — например):
  export IAM_TOKEN=$(yc --profile sandbox-preprod iam create-token)
  export CONNECT_URL='sc://connect-api-<...>.proxy.lakehouse.preprod.yandexcloud.net:9443'
  export GRPC_DEFAULT_SSL_ROOTS_FILE_PATH=scripts/certs/yc_internal_root.pem
  python -u scripts/orders_ab_test_analysis.py
"""

import json
import math
import os
import sys
import time
import zlib
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CATALOG = "dlback-test-catalog-10"
SOURCE_TABLE = f"`{CATALOG}`.marts.mart_orders_ab_test"
RESULT_TABLE = f"`{CATALOG}`.marts.orders_ab_result"

CONTROL_GROUP = "A"  # контроль
TARGET_GROUP = "B"   # целевая

N_BOOT = max(1, int(os.environ.get("BOOTSTRAP_ITERS", "1000")))  # итераций бутстрапа на группу
BOOT_CHUNK = 50      # итераций за один numpy-чанк (матрица BOOT_CHUNK x n индексов)
FALLBACK_CHUNK = 100  # итераций на один SQL-запрос в stdlib-fallback (короткие стримы)
ALPHA = 0.05
SEED_BASE = 20260916  # seed детерминированно зависит от группы → воспроизводимость

METRICS = ("orders_cnt", "avg_order_value", "uniq_customers", "items_per_order")

# Возвращаемый тип applyInPandas: long-формат (ab_group, metric_name, iteration,
# boot_value), 4 метрики x N_BOOT строк на группу — arrow-friendly, без массивов в ячейках.
# ВАЖНО: applyInPandas НЕ добавляет колонку группировки сам — возвращаем ab_group явно.
BOOT_RETURN_SCHEMA = StructType([
    StructField("ab_group", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("iteration", IntegerType(), False),
    StructField("boot_value", DoubleType(), False),
])

# Возвращаемый тип stdlib-fallback UDF: массив реплик одного чанка
# (avg_order_value, items_per_order, uniq_customers); orders_cnt = n группы.
FALLBACK_RETURN_TYPE = ArrayType(StructType([
    StructField("avg_order_value", DoubleType(), False),
    StructField("items_per_order", DoubleType(), False),
    StructField("uniq_customers", DoubleType(), False),
]))

# Схема итоговой таблицы (long-формат по брифу).
RESULT_SCHEMA = StructType([
    StructField("ab_group", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), False),
    StructField("control_value", DoubleType(), True),
    StructField("abs_diff", DoubleType(), True),
    StructField("rel_diff_pct", DoubleType(), True),
    StructField("p_value", DoubleType(), True),
    StructField("ci_low", DoubleType(), True),
    StructField("ci_high", DoubleType(), True),
    StructField("is_significant", BooleanType(), True),
    StructField("iterations", IntegerType(), False),
    StructField("computed_at", TimestampType(), False),
])


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def group_bootstrap_pandas(pdf):
    """applyInPandas-функция: исполняется на executor'ах кластера.

    Принимает pandas.DataFrame всех заказов одной группы (ab_group, order_id,
    customer_id, items_count, order_amount) и выполняет N_BOOT итераций
    ресемплинга заказов с возвращением средствами numpy. Возвращает long-DF
    бутстрап-реплик метрик (4 метрики x N_BOOT строк).
    """
    import numpy as np
    import pandas as pd

    group = str(pdf["ab_group"].iloc[0])
    n = len(pdf)
    rng = np.random.default_rng(SEED_BASE + zlib.crc32(group.encode("utf-8")))

    amounts = pdf["order_amount"].to_numpy(dtype="float64")
    items = pdf["items_count"].to_numpy(dtype="float64")
    cust_codes = pd.factorize(pdf["customer_id"])[0]  # ids -> компактные int-коды

    boot_aov = np.empty(N_BOOT, dtype="float64")
    boot_ipo = np.empty(N_BOOT, dtype="float64")
    boot_uniq = np.empty(N_BOOT, dtype="float64")
    pos = 0
    while pos < N_BOOT:
        ch = min(BOOT_CHUNK, N_BOOT - pos)
        idx = rng.integers(0, n, size=(ch, n))  # ресемплинг заказов с возвращением
        boot_aov[pos:pos + ch] = np.take(amounts, idx).mean(axis=1)
        boot_ipo[pos:pos + ch] = np.take(items, idx).mean(axis=1)
        for r in range(ch):  # uniq-клиенты реплики: np.unique по int-кодам
            boot_uniq[pos + r] = np.unique(np.take(cust_codes, idx[r])).size
        pos += ch
    boot_cnt = np.full(N_BOOT, float(n))  # count инвариантен к ресемплингу заказов

    reps = {
        "orders_cnt": boot_cnt,
        "avg_order_value": boot_aov,
        "uniq_customers": boot_uniq,
        "items_per_order": boot_ipo,
    }
    out = {"ab_group": [], "metric_name": [], "iteration": [], "boot_value": []}
    for metric in METRICS:
        out["ab_group"].extend([group] * N_BOOT)
        out["metric_name"].extend([metric] * N_BOOT)
        out["iteration"].extend(range(N_BOOT))
        out["boot_value"].extend(reps[metric].tolist())
    return pd.DataFrame(out)


def stdlib_bootstrap_udf(amounts, items, custs, seed, n_iter):
    """stdlib-fallback (v7-логика): бутстрап одной группы БЕЗ numpy/pandas.

    Исполняется на executor'ах; вызывается из SQL зарегистрированным UDF
    ab_boot_stdlib (регистрация ТОЛЬКО через spark.udf.register — F.udf на
    функциях модуля несовместим с Spark Connect). Аргументы — выровненные
    collect_list-ы колонок группы, seed чанка и размер чанка; возвращаются
    реплики (avg_order_value, items_per_order, uniq_customers) — по одной
    структуре на итерацию. random/fsum/set + int(random()*n) вместо
    randrange (быстрее, смещение пренебрежимо для бутстрапа).
    """
    rng = __import__("random").Random(seed)
    n = len(amounts)
    out = []
    for _ in range(n_iter):
        idx = [int(rng.random() * n) for _ in range(n)]  # ресемплинг с возвращением
        out.append((
            math.fsum(amounts[i] for i in idx) / n,
            math.fsum(items[i] for i in idx) / n,
            float(len({custs[i] for i in idx})),
        ))
    return out


def build_spark():
    """Spark Connect-сессия (+ gRPC keepalive опции канала и fast-fail политика:
    прокси Lakehouse обрывает долгие стримы, reattach не поддерживает — обрывы
    должны фейлиться быстро, а не висеть ~15 минут в ретраях)."""
    from pyspark.sql.connect.client.core import ChannelBuilder

    _orig_init = ChannelBuilder.__init__

    def _patched_init(self, url, channelOptions=None):
        keepalive = [
            ("grpc.keepalive_time_ms", 20000),
            ("grpc.keepalive_timeout_ms", 15000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.max_pings_without_data", 0),
        ]
        _orig_init(self, url, (channelOptions or []) + keepalive)

    ChannelBuilder.__init__ = _patched_init

    connect_url = os.environ.get("CONNECT_URL") or os.environ["SPARK_CONNECT_URL"]
    spark = (
        SparkSession.builder.remote(
            f"{connect_url}/;use_ssl=true;token={os.environ['IAM_TOKEN']}"
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    try:  # fast-fail: обрывы стрима должны фейлиться за секунды, а не висеть 15 минут
        client = spark._client
        client._retry_policy.update(
            {"max_retries": 2, "initial_backoff": 200, "backoff_multiplier": 2.0, "max_backoff": 2000, "jitter": 200}
        )
    except Exception as e:  # приватная структура клиента изменилась — не критично
        log(f"      (client retry-policy tuning skipped: {e})")
    return spark


def build_source(spark):
    """Источник: только нужные колонки, decimal -> double; кэшируем."""
    src = (
        spark.table(SOURCE_TABLE)
        .select(
            F.col("ab_group").cast("string").alias("ab_group"),
            F.col("order_id"),
            F.col("customer_id"),
            F.col("items_count").cast("double").alias("items_count"),
            F.col("order_amount").cast("double").alias("order_amount"),
        )
        .cache()
    )
    return src


def run_bootstrap_pandas(spark, src):
    """Основной путь: numpy-бутстрап на executor'ах через applyInPandas."""
    boot = src.groupBy("ab_group").applyInPandas(group_bootstrap_pandas, schema=BOOT_RETURN_SCHEMA)
    # собираем малые агрегаты: 2 группы x 4 метрики x N_BOOT реплик (8k строк при 1000)
    boots = {}
    for r in boot.collect():
        boots.setdefault(r["ab_group"], {}).setdefault(r["metric_name"], {})[r["iteration"]] = r["boot_value"]
    return boots


def run_bootstrap_stdlib(spark, src, counts):
    """Fallback: stdlib-бутстрап (random/fsum/set) на executor'ах.

    UDF регистрируется через spark.udf.register (обязательный для Spark
    Connect способ), вызывается из SQL по группе: collect_list колонок группы
    + чанк итераций за один запрос (короткие gRPC-стримы — прокси Lakehouse
    обрывает долгие). orders_cnt инвариантен к ресемплингу → n группы.
    """
    spark.udf.register("ab_boot_stdlib", stdlib_bootstrap_udf, FALLBACK_RETURN_TYPE)
    src.createOrReplaceTempView("ab_src")

    boots = {
        g: {m: {} for m in METRICS}
        for g in (CONTROL_GROUP, TARGET_GROUP)
    }
    t0 = time.time()
    for g in (CONTROL_GROUP, TARGET_GROUP):
        n_group = counts[g]
        boots[g]["orders_cnt"] = {i: float(n_group) for i in range(N_BOOT)}
        pos, chunk_idx = 0, 0
        while pos < N_BOOT:
            ch = min(FALLBACK_CHUNK, N_BOOT - pos)
            seed = SEED_BASE + zlib.crc32(g.encode("utf-8")) + chunk_idx
            rows = spark.sql(
                f"SELECT ab_boot_stdlib(collect_list(order_amount), collect_list(items_count), "
                f"collect_list(customer_id), {seed}, {ch}) AS reps "
                f"FROM ab_src WHERE ab_group = '{g}'"
            ).collect()
            if not rows or rows[0]["reps"] is None:
                raise RuntimeError(f"stdlib bootstrap: empty result for group {g} chunk {chunk_idx}")
            for j, rep in enumerate(rows[0]["reps"]):
                boots[g]["avg_order_value"][pos + j] = float(rep["avg_order_value"])
                boots[g]["items_per_order"][pos + j] = float(rep["items_per_order"])
                boots[g]["uniq_customers"][pos + j] = float(rep["uniq_customers"])
            pos += ch
            chunk_idx += 1
            log(f"      stdlib bootstrap {g}: {pos}/{N_BOOT} iters ({time.time() - t0:.1f}s elapsed)")
    return boots


def percentile(sorted_vals, q):
    """Перцентиль линейной интерполяцией (stdlib-эквивалент numpy.percentile)."""
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = (q / 100.0) * (len(sorted_vals) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return float(sorted_vals[int(pos)])
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def boot_compare(boot_a, boot_b):
    """Расчёт на драйвере над МАЛЫМИ массивами (по N_BOOT значений на метрику):
    p-value односторонний (H1: B>A) и 95% CI разности. stdlib, без numpy."""
    diffs = sorted(b - a for a, b in zip(boot_a, boot_b))
    p = (1.0 + sum(1.0 for d in diffs if d <= 0.0)) / (N_BOOT + 1.0)
    return float(p), float(percentile(diffs, 2.5)), float(percentile(diffs, 97.5))


def main() -> int:
    for var in ("IAM_TOKEN",):
        if not os.environ.get(var):
            print(f"FATAL: env {var} is not set", file=sys.stderr)
            return 2
    if not (os.environ.get("CONNECT_URL") or os.environ.get("SPARK_CONNECT_URL")):
        print("FATAL: env CONNECT_URL (or SPARK_CONNECT_URL) is not set", file=sys.stderr)
        return 2

    connect_url = os.environ.get("CONNECT_URL") or os.environ["SPARK_CONNECT_URL"]
    log(f"[1/7] Connecting Spark Connect: {connect_url}")
    spark = build_spark()
    try:
        log(f"[2/7] Reading source: {SOURCE_TABLE}")
        src = build_source(spark)
        src.printSchema()
        total = src.count()
        log(f"      total rows: {total}")
        log("      rows per ab_group:")
        counts = {}
        for r in src.groupBy("ab_group").count().orderBy("ab_group").collect():
            counts[r["ab_group"]] = r["count"]
            log(f"        {r['ab_group']}: {r['count']}")

        # наблюдаемые метрики — короткий серверный запрос, заодно материализует кэш src
        agg = src.groupBy("ab_group").agg(
            F.count("order_id").alias("orders_cnt"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("uniq_customers"),
            F.avg("items_count").alias("items_per_order"),
        )
        observed = {r["ab_group"]: r for r in agg.collect()}
        if CONTROL_GROUP not in observed or TARGET_GROUP not in observed:
            log(f"FATAL: expected groups {CONTROL_GROUP}/{TARGET_GROUP}, got {sorted(observed)}")
            return 3
        for g in sorted(observed):
            r = observed[g]
            log(f"      observed {g}: orders_cnt={r['orders_cnt']} avg_order_value={r['avg_order_value']:.4f} "
                f"uniq_customers={r['uniq_customers']} items_per_order={r['items_per_order']:.4f}")

        log(f"[3/7] Server-side bootstrap via groupBy(ab_group).applyInPandas "
            f"(numpy on executors): {N_BOOT} iters/group, chunk={BOOT_CHUNK} ...")
        t0 = time.time()
        try:
            boots = run_bootstrap_pandas(spark, src)
            mode = "applyInPandas (numpy on executors)"
        except Exception as e:  # PYTHON_VERSION_MISMATCH / нет pandas-arrow на executor'ах
            log(f"      applyInPandas FAILED: {e!r}")
            log("      FALLBACK: stdlib-бутстрап (random/fsum/set через spark.udf.register, "
                f"чанки по {FALLBACK_CHUNK} итераций на SQL-запрос) ...")
            boots = run_bootstrap_stdlib(spark, src, counts)
            mode = "stdlib UDF fallback (v7 logic)"
        n_collected = sum(len(m) for g in boots.values() for m in g.values())
        log(f"      bootstrap done in {time.time() - t0:.1f}s (mode={mode}, rows collected: {n_collected})")
        for g in (CONTROL_GROUP, TARGET_GROUP):
            for m in METRICS:
                if len(boots.get(g, {}).get(m, {})) != N_BOOT:
                    log(f"FATAL: bootstrap incomplete: group {g} metric {m}: "
                        f"{len(boots.get(g, {}).get(m, {}))}/{N_BOOT}")
                    return 4

        log("[4/7] Driver-side summary (small aggregates only):")
        a, b = observed[CONTROL_GROUP], observed[TARGET_GROUP]
        records = []
        computed_at = datetime.now(timezone.utc)
        for metric in METRICS:
            va, vb = float(a[metric]), float(b[metric])
            p, ci_low, ci_high = boot_compare(
                [boots[CONTROL_GROUP][metric][i] for i in range(N_BOOT)],
                [boots[TARGET_GROUP][metric][i] for i in range(N_BOOT)],
            )
            abs_diff = vb - va
            rel_pct = (abs_diff / va * 100.0) if va != 0 else float("nan")
            sig = bool(p < ALPHA)
            # строка контрольной группы: только собственное значение метрики
            records.append((CONTROL_GROUP, metric, va, None, None, None, None, None, None, None, N_BOOT, computed_at))
            # строка целевой группы: значение + сравнение с контролем
            records.append((TARGET_GROUP, metric, vb, va, abs_diff, rel_pct, p, ci_low, ci_high, sig, N_BOOT, computed_at))
            log(
                f"      {metric:16s} A={va:.4f}  B={vb:.4f}  diff={abs_diff:+.4f} ({rel_pct:+.2f}%)  "
                f"p={p:.4f}  CI95=[{ci_low:.4f}; {ci_high:.4f}]  significant={sig}"
            )
        log("      NOTE: для orders_cnt бутстрап-распределение вырождено (count не меняется при"
            " ресемпплинге заказов) — p/CI для этой метрики тривиальны (0/1), интерпретировать осторожно.")

        log(f"[5/7] Result DataFrame + CTAS -> {RESULT_TABLE}")
        result_df = spark.createDataFrame(records, schema=RESULT_SCHEMA)
        result_df.createOrReplaceTempView("orders_ab_result_view")
        spark.sql(f"DROP TABLE IF EXISTS {RESULT_TABLE}")
        spark.sql(f"CREATE TABLE {RESULT_TABLE} AS SELECT * FROM orders_ab_result_view")
        log("      written.")

        log("[6/7] Read-back verification (SELECT * LIMIT 10):")
        back = spark.sql(f"SELECT * FROM {RESULT_TABLE} ORDER BY metric_name, ab_group LIMIT 10")
        back.show(20, truncate=False)
        back_rows = back.collect()
        if len(back_rows) != 2 * len(METRICS):
            log(f"FATAL: read-back expected {2 * len(METRICS)} rows, got {len(back_rows)}")
            return 5

        log("[7/7] DONE OK")
        # машиночитаемый итог для XCom DAG'а AB_test (последняя строка stdout)
        print("AB_RESULT_JSON=" + json.dumps({
            "mode": mode,
            "iterations": N_BOOT,
            "result_table": RESULT_TABLE,
            "rows": [
                {
                    "ab_group": r[0], "metric_name": r[1], "metric_value": r[2],
                    "control_value": r[3], "abs_diff": r[4], "rel_diff_pct": r[5],
                    "p_value": r[6], "ci_low": r[7], "ci_high": r[8],
                    "is_significant": r[9], "computed_at": computed_at.isoformat(),
                }
                for r in records
            ],
        }))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
