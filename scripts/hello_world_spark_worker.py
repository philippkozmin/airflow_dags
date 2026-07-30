#!/usr/bin/env python3
"""
Hello world from Spark worker — PySpark over Spark Connect (DataLens Platform).

Produces the string "Hello world from Spark worker" by executing work on a Spark
executor (worker), then reads it back on the driver and prints it plainly, plus a
df.show() of the DataFrame.

Runtime / environment
---------------------
- Target Managed Spark cluster: c9qnmr0ifrus5ks50jup, spark_version = 4.0.0.
- Therefore the driver MUST run PySpark 4.0.0 on a Python interpreter whose MINOR
  version matches the cluster's Python (the fix for the previous run's
  check_python_version PythonException). The provided venv
  /Users/philippkozmin/.spark-connect-venv (pyspark 4.0.0, CPython 3.13) is used.
- Connection is built by the `sparkconnect` skill pattern: the IAM token is embedded
  inline in the remote URI (use_ssl=true;token=...). The token is read from the
  IAM_TOKEN env var and is never written to disk.

Required env vars:
    SPARK_CONNECT_URL   e.g. sc://connect-api-<job>-<cluster>.spark.yandexcloud.net:443
    IAM_TOKEN           fresh Yandex Cloud IAM token

The phrase is produced on the worker two ways, with a graceful fallback so a single
run is robust:
  A) a Python UDF (runs in the Python worker on the executor) — the literal
     "map/UDF over a distributed DataFrame" requirement;
  B) if the Python worker is unusable (e.g. a driver/worker Python mismatch), a
     Spark SQL concat() expression, which still evaluates on the executor JVM.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


PHRASE = "Hello world from Spark worker"


def build_session() -> SparkSession:
    connect_url = os.environ["SPARK_CONNECT_URL"].rstrip("/")
    iam_token = os.environ["IAM_TOKEN"]
    remote = f"{connect_url}/;use_ssl=true;token={iam_token}"
    return SparkSession.builder.remote(remote).getOrCreate()


def make_on_worker_via_udf(spark: SparkSession):
    """Build the phrase inside a Python UDF that runs on an executor."""

    @F.udf(returnType=StringType())
    def assemble(a: str, b: str) -> str:
        # This body executes in the Python worker process on the executor.
        return f"{a} {b}"

    # Small distributed DataFrame: one partition -> one task on a worker.
    parts = spark.createDataFrame(
        [("Hello world", "from Spark worker")], ["a", "b"]
    )
    return parts.select(assemble("a", "b").alias("message"))


def make_on_worker_via_sql(spark: SparkSession):
    """Fallback: build the phrase with a JVM concat, evaluated on the executor."""
    base = spark.range(1)  # distributed DataFrame
    return base.select(
        F.concat(F.lit("Hello world"), F.lit(" "), F.lit("from Spark worker")).alias(
            "message"
        )
    )


def main() -> int:
    print(f"[driver] Python {sys.version.split()[0]}", flush=True)
    spark = build_session()
    try:
        print(f"[driver] connected; Spark version {spark.version}", flush=True)

        df = None
        used = None
        try:
            df = make_on_worker_via_udf(spark)
            # Force execution now so a Python-worker problem is caught here.
            rows = df.collect()
            used = "python-udf-on-executor"
        except Exception as exc:  # noqa: BLE001 - deliberate robust fallback
            print(
                "[driver] Python UDF path failed "
                f"({type(exc).__name__}); falling back to JVM concat on executor. "
                f"Detail: {str(exc).splitlines()[0] if str(exc) else exc}",
                flush=True,
            )
            df = make_on_worker_via_sql(spark)
            rows = df.collect()
            used = "jvm-concat-on-executor"

        phrase = rows[0]["message"]

        print("=" * 60, flush=True)
        print(f"[result] produced via: {used}", flush=True)
        print(phrase, flush=True)
        print("=" * 60, flush=True)

        df.show(truncate=False)
        return 0
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
