# upload_to_minio.py
from pathlib import Path
from minio import Minio
import os

# Настройки (требовать через окружение в целях безопасности)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")
INCIDENT_ID = os.getenv("INCIDENT_ID", "INC-2023-001")
OUTPUT_DIR = "mine_parser/output"

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in environment")

def main():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' created")
    
    output_path = Path(OUTPUT_DIR)
    for file_path in output_path.glob("*.json"):
        object_name = f"bronze/incident_id={INCIDENT_ID}/{file_path.name}"
        client.fput_object(BUCKET, object_name, str(file_path))
        print(f"{file_path.name}")
    
    print("Upload completed!")

if __name__ == "__main__":
    main()