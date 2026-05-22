"""DAG: submit a Spark job via Spark Standalone REST API.

This DAG demonstrates how to run Spark workloads from Airflow without
`spark-submit` in the Airflow container.

Flow:
1) POST /v1/submissions/create to spark-master:6066
2) Poll /v1/submissions/status/<submissionId> until terminal state

The Spark application reads/writes MinIO via s3a:// using the shared Spark
configuration mounted into the Spark containers.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.decorators import task


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Spark REST HTTP {e.code}: {body}") from e


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="spark_rest_submit_minio_demo",
    default_args=default_args,
    description="Submit a MinIO->compute->MinIO Spark job via Spark REST",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "rest", "minio", "example"],
) as dag:

    @task
    def submit() -> str:
        spark_rest = "http://spark-master:6066"
        create_url = f"{spark_rest}/v1/submissions/create"

        # In standalone cluster mode, PythonRunner expects the Python script as the
        # first argument. We keep the script available via a bind-mount on all
        # Spark nodes (/opt/spark-apps), and use any existing jar as appResource.
        app_resource = "file:/opt/spark/jars/spark-core_2.13-4.1.1.jar"

        payload = {
            "action": "CreateSubmissionRequest",
            "appResource": app_resource,
            "clientSparkVersion": "4.1.1",
            "environmentVariables": {
                "AWS_ACCESS_KEY_ID": "admin",
                "AWS_SECRET_ACCESS_KEY": "password",
            },
            "mainClass": "org.apache.spark.deploy.PythonRunner",
            "appArgs": [
                "/opt/spark-apps/minio_rest_demo.py",
                "",
                "--input",
                "s3a://lakehouse/spark_rest_demo/input",
                "--output",
                "s3a://lakehouse/spark_rest_demo/output",
                "--rows",
                "1000",
            ],
            "sparkProperties": {
                "spark.master": "spark://spark-master:7077",
                "spark.submit.deployMode": "cluster",
                "spark.app.name": "airflow-rest-minio-demo",
                "spark.pyspark.python": "python3",
                "spark.pyspark.driver.python": "python3",
                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.access.key": "admin",
                "spark.hadoop.fs.s3a.secret.key": "password",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
        }

        resp = _http_json("POST", create_url, payload)
        submission_id = resp.get("submissionId")
        if not submission_id:
            raise RuntimeError(f"Unexpected create response: {resp}")

        return submission_id

    @task
    def wait_for_completion(submission_id: str) -> dict[str, Any]:
        spark_rest = "http://spark-master:6066"
        status_url = f"{spark_rest}/v1/submissions/status/{submission_id}"

        terminal = {"FINISHED", "ERROR", "KILLED", "FAILED"}
        timeout_seconds = 15 * 60
        poll_seconds = 5

        start = time.time()
        last = None
        while True:
            resp = _http_json("GET", status_url)
            last = resp

            driver_state = resp.get("driverState")
            if driver_state in terminal:
                if driver_state != "FINISHED":
                    raise RuntimeError(f"Spark job ended in state={driver_state}: {resp}")
                return resp

            if time.time() - start > timeout_seconds:
                raise TimeoutError(f"Timed out waiting for Spark job: {resp}")

            time.sleep(poll_seconds)

    submission_id = submit()
    wait_for_completion(submission_id)
