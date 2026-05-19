"""Spark REST demo job: MinIO (S3A) -> compute -> MinIO.

This job is intentionally tiny and dependency-free (beyond PySpark).
It writes a small input dataset to MinIO via s3a://, reads it back,
computes a simple aggregation, and writes the result back to MinIO.

Designed to be submitted via Spark Standalone REST (/v1/submissions/create)
from Airflow.
"""

from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, max as spark_max


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input",
        default="s3a://lakehouse/spark_rest_demo/input",
        help="Input path in MinIO (s3a://bucket/prefix)",
    )
    p.add_argument(
        "--output",
        default="s3a://lakehouse/spark_rest_demo/output",
        help="Output path in MinIO (s3a://bucket/prefix)",
    )
    p.add_argument("--rows", type=int, default=1000, help="How many rows to generate")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.builder.appName("minio-rest-demo").getOrCreate()

    # 1) "Input" to MinIO
    df_in = spark.range(0, args.rows).withColumnRenamed("id", "n")
    df_in.write.mode("overwrite").parquet(args.input)

    # 2) Read from MinIO and compute
    df = spark.read.parquet(args.input)
    summary = df.agg(count("*").alias("row_count"), spark_max(col("n")).alias("max_n"))

    # 3) Output to MinIO
    out_df = summary.withColumn("processed_at_utc", current_timestamp())
    out_df.coalesce(1).write.mode("overwrite").json(args.output)

    print(
        "OK",
        {
            "input": args.input,
            "output": args.output,
            "rows": args.rows,
            "finished_at_utc": datetime.utcnow().isoformat() + "Z",
        },
    )

    spark.stop()


if __name__ == "__main__":
    main()
