"""Example DAG: MinIO -> compute -> MinIO.

Goal
----
A minimal, reproducible template for data processing in Airflow:
1) Read objects from MinIO (S3-compatible)
2) Compute a simple metric
3) Write results back to MinIO

Why this example (in THIS repo)
------------------------------
In the current docker runtime, Airflow is running but Spark master/workers are
not started, so the most reliable example is a pure-Python job.
You can later swap the `compute_metrics()` task body for a Spark submission.

Configuration
-------------
Optionally set Airflow Variables (Admin -> Variables):
- MINIO_ENDPOINT:    default "http://minio:9000"
- MINIO_ACCESS_KEY:  default "admin"
- MINIO_SECRET_KEY:  default "password"
- MINIO_BUCKET:      default "lakehouse"
- MINIO_INPUT_PREFIX:  default "raw/" (what to read)
- MINIO_OUTPUT_PREFIX: default "processed/example/" (where to write results)

Notes
-----
- This DAG purposely avoids any repo-specific table schemas.
- It works with arbitrary files; for .json/.csv it tries to count records.
  Otherwise it falls back to counting lines.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


def _get_minio_config() -> MinioConfig:
    return MinioConfig(
        endpoint=Variable.get("MINIO_ENDPOINT", default_var="http://minio:9000"),
        access_key=Variable.get("MINIO_ACCESS_KEY", default_var="admin"),
        secret_key=Variable.get("MINIO_SECRET_KEY", default_var="password"),
        bucket=Variable.get("MINIO_BUCKET", default_var="lakehouse"),
    )


def _get_minio_client(cfg: MinioConfig):
    from minio import Minio

    endpoint = cfg.endpoint.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=False)


def _count_records_for_file(path: Path, object_name: str) -> dict[str, Any]:
    suffix = object_name.lower().rsplit(".", 1)[-1] if "." in object_name else ""

    if suffix == "json":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                return {"mode": "json", "records": len(data)}
            if isinstance(data, dict):
                return {"mode": "json", "records": 1}
        except Exception as e:
            return {"mode": "json", "error": str(e)}

    if suffix == "csv":
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                # Count data rows; assume header exists but don't enforce.
                rows = 0
                for _ in reader:
                    rows += 1
            # If there is a header row, this is "rows-1"; we keep "rows" as-is.
            return {"mode": "csv", "rows": rows}
        except Exception as e:
            return {"mode": "csv", "error": str(e)}

    # Fallback: count lines in text
    try:
        with open(path, "rb") as f:
            line_count = 0
            for _ in f:
                line_count += 1
        return {"mode": "lines", "lines": line_count}
    except Exception as e:
        return {"mode": "lines", "error": str(e)}


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="minio_minio_etl_example",
    default_args=default_args,
    description="Example: read from MinIO, compute simple metrics, write back to MinIO",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["minio", "example", "etl"],
) as dag:

    @task
    def pick_latest_object() -> dict[str, Any]:
        cfg = _get_minio_config()
        client = _get_minio_client(cfg)

        input_prefix = Variable.get("MINIO_INPUT_PREFIX", default_var="raw/")
        if input_prefix and not input_prefix.endswith("/"):
            input_prefix += "/"

        if not client.bucket_exists(cfg.bucket):
            raise AirflowSkipException(f"Bucket does not exist: {cfg.bucket}")

        latest = None
        for obj in client.list_objects(cfg.bucket, prefix=input_prefix, recursive=True):
            # Skip folders / markers
            if obj.is_dir:
                continue
            if latest is None or (obj.last_modified and obj.last_modified > latest.last_modified):
                latest = obj

        if latest is None:
            raise AirflowSkipException(f"No objects found under prefix: {cfg.bucket}/{input_prefix}")

        logging.info("Picked object: %s (size=%s)", latest.object_name, latest.size)
        return {
            "object_name": latest.object_name,
            "size": latest.size,
            "last_modified": latest.last_modified.isoformat() if latest.last_modified else None,
            "minio": asdict(cfg),
        }

    @task
    def compute_metrics(picked: dict[str, Any]) -> dict[str, Any]:
        cfg = MinioConfig(**picked["minio"])
        client = _get_minio_client(cfg)
        object_name = picked["object_name"]

        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td) / Path(object_name).name
            client.fget_object(cfg.bucket, object_name, str(local_path))

            metrics = _count_records_for_file(local_path, object_name)

        out = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "bucket": cfg.bucket,
            "input_object": object_name,
            "input_size": picked.get("size"),
            "input_last_modified": picked.get("last_modified"),
            "metrics": metrics,
        }

        logging.info("Metrics computed: %s", out["metrics"])
        return out

    @task
    def write_metrics_to_minio(result: dict[str, Any]) -> str:
        cfg = _get_minio_config()
        client = _get_minio_client(cfg)

        output_prefix = Variable.get("MINIO_OUTPUT_PREFIX", default_var="processed/example/")
        if output_prefix and not output_prefix.endswith("/"):
            output_prefix += "/"

        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        object_name = f"{output_prefix}{run_id}_metrics.json"

        with tempfile.TemporaryDirectory() as td:
            local_path = Path(td) / "metrics.json"
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            client.fput_object(cfg.bucket, object_name, str(local_path))

        logging.info("Wrote metrics to s3a://%s/%s", cfg.bucket, object_name)
        return f"s3a://{cfg.bucket}/{object_name}"

    picked = pick_latest_object()
    metrics = compute_metrics(picked)
    out_path = write_metrics_to_minio(metrics)

    picked >> metrics >> out_path
