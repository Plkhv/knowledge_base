"""
metrics_dag.py
==============
DAG оркестрации расчёта показателей через Spark.

Порядок выполнения (соответствует оператору M из мат. модели):
  1. check_source_data    — проверяет что факты уже загружены
  2. submit_spark_metrics — запускает spark_metrics_job.py через docker exec jupyter
  3. validate_results     — проверяет что таблицы метрик не пустые
  4. notify_summary       — логирует итоговую статистику

Требования:
  - spark_metrics_job.py лежит в ./notebooks/ (монтируется в /home/jovyan/work/ на jupyter)
  - JAR-файлы в ./spark-jars/ (монтируется в /home/jovyan/jars/ на jupyter)
  - Docker socket проброшен в airflow: /var/run/docker.sock:/var/run/docker.sock
  - Все контейнеры в одной сети lakehouse-net
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
import logging
import subprocess
import json
import os
from typing import Dict, Any

# ── Конфигурация ──────────────────────────────────────────────────────────────

# Имя jupyter-контейнера (как в docker-compose.yml)
JUPYTER_CONTAINER = "jupyter"

# Пути внутри jupyter-контейнера
SPARK_SUBMIT_BIN  = "/usr/local/spark/bin/spark-submit"
JAVA_HOME         = "/usr/lib/jvm/java-17-openjdk-amd64"
SPARK_APP_PATH    = "/home/jovyan/work/spark_metrics_job.py"
SPARK_JARS_DIR    = "/home/jovyan/jars"
SPARK_JARS        = ",".join([
    f"{SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.4.0.jar",
    f"{SPARK_JARS_DIR}/iceberg-nessie-1.4.0.jar",
    f"{SPARK_JARS_DIR}/iceberg-aws-bundle-1.4.0.jar",
    f"{SPARK_JARS_DIR}/nessie-client-0.7.0.jar",
])

# Spark в local-режиме (spark-master контейнер не используется)
SPARK_MASTER = "local[*]"

TRINO_HOST   = "trino"
TRINO_PORT   = 8080
TRINO_SCHEMA = "mine"

# Таблицы-источники: должны быть заполнены до запуска расчётов
SOURCE_TABLES = [
    "incident_description",
    "air_analysis",
    "premise_parameters",
    "affected_areas",
]

# Таблицы-результаты: проверяем после расчётов
RESULT_TABLES = [
    "incident_metrics",
    "ventilation_metrics",
    "fire_metrics",
    "degassing_metrics",
    "dust_metrics",
    "incident_metrics_summary",
]

# ── Утилиты ───────────────────────────────────────────────────────────────────

def get_trino_conn():
    """Создаёт соединение с Trino."""
    from trino.dbapi import connect
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog="iceberg",
        schema=TRINO_SCHEMA,
        http_scheme="http",
        request_timeout=60.0,
    )


def count_table(table: str) -> int:
    """Возвращает количество строк в таблице через Trino."""
    conn   = get_trino_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM iceberg.{TRINO_SCHEMA}.{table}")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else 0


def run_in_jupyter(cmd: list, timeout: int = 600) -> subprocess.CompletedProcess:
    """
    Выполняет команду в jupyter-контейнере через docker exec.
    JAVA_HOME выставляется явно чтобы spark-submit нашёл JVM.
    """
    full_cmd = [
        "docker", "exec",
        "-e", f"JAVA_HOME={JAVA_HOME}",
        "-e", f"SPARK_MASTER={SPARK_MASTER}",
        "-e", "MINIO_ENDPOINT=http://minio:9000",
        "-e", "MINIO_ACCESS_KEY=admin",
        "-e", "MINIO_SECRET_KEY=password",
        JUPYTER_CONTAINER,
    ] + cmd

    logging.info(f"Running in {JUPYTER_CONTAINER}: {' '.join(cmd[:4])} ...")
    return subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# ── Задачи DAG ────────────────────────────────────────────────────────────────

@task
def check_source_data() -> Dict[str, int]:
    """
    Проверяет доступность Trino, jupyter-контейнера и наличие данных.
    """
    # Проверяем Trino
    try:
        conn = get_trino_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        logging.info("✅ Trino connection OK")
    except Exception as e:
        raise ConnectionError(
            f"Cannot reach Trino at {TRINO_HOST}:{TRINO_PORT}. "
            f"Check that airflow-standalone is in lakehouse-net. Error: {e}"
        )

    # Проверяем доступность jupyter через docker exec
    result = run_in_jupyter(["echo", "jupyter-ok"], timeout=15)
    if result.returncode != 0:
        raise RuntimeError(
            f"Cannot reach jupyter container via docker exec. "
            f"Check that /var/run/docker.sock is mounted in airflow-standalone. "
            f"stderr: {result.stderr}"
        )
    logging.info("✅ jupyter container reachable via docker exec")

    # Проверяем наличие spark-submit
    result = run_in_jupyter(["test", "-f", SPARK_SUBMIT_BIN], timeout=10)
    if result.returncode != 0:
        raise FileNotFoundError(
            f"spark-submit not found at {SPARK_SUBMIT_BIN} in jupyter container."
        )
    logging.info(f"✅ spark-submit found: {SPARK_SUBMIT_BIN}")

    # Проверяем наличие spark_metrics_job.py
    result = run_in_jupyter(["test", "-f", SPARK_APP_PATH], timeout=10)
    if result.returncode != 0:
        raise FileNotFoundError(
            f"Spark app not found at {SPARK_APP_PATH} in jupyter container. "
            f"Place spark_metrics_job.py in ./notebooks/ on the host."
        )
    logging.info(f"✅ Spark app found: {SPARK_APP_PATH}")

    # Считаем строки в таблицах-источниках
    counts       = {}
    empty_tables = []

    for table in SOURCE_TABLES:
        try:
            n = count_table(table)
        except Exception as e:
            logging.warning(f"  Cannot count {table}: {e}")
            n = 0
        counts[table] = n
        status = "✅" if n > 0 else "⚠️  EMPTY"
        logging.info(f"  {status} {table}: {n} rows")
        if n == 0:
            empty_tables.append(table)

    if empty_tables:
        raise AirflowSkipException(
            f"Source tables are empty or missing: {empty_tables}. "
            f"Run parallel_trino_loader first."
        )

    logging.info(f"✅ All source tables have data: {counts}")
    return counts


@task
def submit_spark_metrics(source_counts: Dict[str, int]) -> Dict[str, Any]:
    """
    Запускает spark_metrics_job.py через docker exec в jupyter-контейнере.
    Spark работает в local[*] режиме — отдельный кластер не нужен.
    """
    cmd = [
        SPARK_SUBMIT_BIN,
        "--master",  SPARK_MASTER,
        "--jars",    SPARK_JARS,
        "--conf",    "spark.executor.memory=1g",
        "--conf",    "spark.driver.memory=1g",
        "--conf",    "spark.executor.cores=2",
        "--conf",    "spark.sql.adaptive.enabled=true",
        "--conf",    "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf",    "spark.hadoop.fs.s3a.access.key=admin",
        "--conf",    "spark.hadoop.fs.s3a.secret.key=password",
        "--conf",    "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf",    "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf",    "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        SPARK_APP_PATH,
    ]

    logging.info(f"🚀 Submitting Spark job via docker exec {JUPYTER_CONTAINER}")
    logging.info(f"   App:    {SPARK_APP_PATH}")
    logging.info(f"   Master: {SPARK_MASTER}")

    result = run_in_jupyter(cmd, timeout=600)

    # Логируем вывод для диагностики
    if result.stdout:
        for line in result.stdout.splitlines()[-100:]:
            logging.info(f"[spark] {line}")
    if result.stderr:
        for line in result.stderr.splitlines()[-50:]:
            logging.warning(f"[spark-err] {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"spark-submit failed with code {result.returncode}. "
            f"Last stderr: {result.stderr[-1000:]}"
        )

    logging.info("✅ Spark job completed successfully")
    return {
        "returncode":   result.returncode,
        "stdout_lines": len(result.stdout.splitlines()),
    }


@task
def validate_results(spark_result: Dict[str, Any]) -> Dict[str, int]:
    """
    Проверяет что таблицы результатов заполнены после расчётов.
    """
    counts   = {}
    warnings = []

    for table in RESULT_TABLES:
        try:
            n = count_table(table)
        except Exception as e:
            logging.warning(f"  Cannot count {table}: {e}")
            n = 0
        counts[table] = n
        status = "✅" if n > 0 else "⚠️  EMPTY"
        logging.info(f"  {status} {table}: {n} rows")
        if n == 0:
            warnings.append(table)

    if warnings:
        logging.warning(
            f"Following metrics tables are empty after calculation: {warnings}. "
            f"Check if source data covers these metrics."
        )

    return counts


@task
def notify_summary(source_counts: Dict[str, int],
                   result_counts:  Dict[str, int]) -> str:
    """Логирует итоговую статистику прогона."""
    summary = {
        "timestamp":          datetime.now().isoformat(),
        "source_tables":      source_counts,
        "result_tables":      result_counts,
        "total_metrics_rows": sum(v for v in result_counts.values() if v > 0),
    }

    logging.info("=" * 60)
    logging.info("📊 METRICS CALCULATION SUMMARY")
    logging.info(f"   Source rows:  {sum(source_counts.values())}")
    logging.info(f"   Metrics rows: {summary['total_metrics_rows']}")
    logging.info("   Result tables:")
    for table, n in result_counts.items():
        status = "✅" if n > 0 else "⚠️"
        logging.info(f"     {status} {table}: {n} rows")
    logging.info("=" * 60)

    return json.dumps(summary, indent=2, ensure_ascii=False)


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":           "data_engineering",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=2),
}

with DAG(
    "metrics_calculator",
    default_args=default_args,
    description="Расчёт показателей M_inc / M_vent / M_fire через Spark (jupyter) → Iceberg",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "metrics", "iceberg"],
) as dag:

    source_counts = check_source_data()
    spark_result  = submit_spark_metrics(source_counts)
    result_counts = validate_results(spark_result)
    summary       = notify_summary(source_counts, result_counts)

    source_counts >> spark_result >> result_counts >> summary
