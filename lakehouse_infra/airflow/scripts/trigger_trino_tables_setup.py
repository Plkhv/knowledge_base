#!/usr/bin/env python3
"""Trigger the Trino tables setup DAG when the infrastructure is ready."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from urllib import error, request

AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "trino_tables_setup")
TRINO_HEALTH_URL = os.getenv("TRINO_HEALTH_URL", "http://trino:8080/v1/info")
POLL_INTERVAL_SECONDS = int(os.getenv("TRIGGER_POLL_INTERVAL", "10"))
TIMEOUT_SECONDS = int(os.getenv("TRIGGER_TIMEOUT_SECONDS", "900"))


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True)


def airflow_db_ready() -> bool:
    result = run_command(["airflow", "db", "check"])
    return result.returncode == 0


def airflow_dag_loaded() -> bool:
    result = run_command(["airflow", "dags", "list"])
    return result.returncode == 0 and AIRFLOW_DAG_ID in result.stdout


def trino_ready() -> bool:
    try:
        with request.urlopen(TRINO_HEALTH_URL, timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def wait_until_ready() -> None:
    deadline = time.time() + TIMEOUT_SECONDS

    while time.time() < deadline:
        if airflow_db_ready() and airflow_dag_loaded() and trino_ready():
            return
        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Timed out waiting for Airflow and Trino readiness after {TIMEOUT_SECONDS}s"
    )


def trigger_dag() -> None:
    run_id = f"infra_startup__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    payload = {
        "source": "infra_startup",
        "triggered_by": "airflow_init_service",
        "dag_id": AIRFLOW_DAG_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    result = run_command(
        [
            "airflow",
            "dags",
            "trigger",
            AIRFLOW_DAG_ID,
            "--run-id",
            run_id,
            "--conf",
            json.dumps(payload, ensure_ascii=False),
        ]
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Airflow DAG trigger failed\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    print(result.stdout.strip() or f"Triggered {AIRFLOW_DAG_ID} with run_id={run_id}")


def main() -> int:
    try:
        print(f"Waiting for Airflow DAG '{AIRFLOW_DAG_ID}' and Trino to be ready...")
        wait_until_ready()
        print(f"Triggering DAG '{AIRFLOW_DAG_ID}'...")
        trigger_dag()
        print("Done.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
