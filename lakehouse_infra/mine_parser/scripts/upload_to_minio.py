# upload_to_minio.py
from pathlib import Path
from minio import Minio

# Настройки
MINIO_ENDPOINT = "localhost:9000"  # MinIO на хосте
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET = "lakehouse"
INCIDENT_ID = "INC-2023-001"
OUTPUT_DIR = "mine_parser\output"

def main():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
        print(f"✅ Bucket '{BUCKET}' created")
    
    output_path = Path(OUTPUT_DIR)
    for file_path in output_path.glob("*.json"):
        object_name = f"bronze/incident_id={INCIDENT_ID}/{file_path.name}"
        client.fput_object(BUCKET, object_name, str(file_path))
        print(f"✅ {file_path.name}")
    
    print("✅ Upload completed!")

if __name__ == "__main__":
    main()