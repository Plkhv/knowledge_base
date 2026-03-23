import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # PostgreSQL (административная база)
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DATABASE = os.getenv("PG_DATABASE", "polaris")
    PG_USER = os.getenv("PG_USER", "polaris")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "polaris123")
    
    DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    
    # Trino (для запросов к Lakehouse)
    TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
    TRINO_PORT = os.getenv("TRINO_PORT", "8082")
    TRINO_CATALOG = os.getenv("TRINO_CATALOG", "memory")
    TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "lakehouse")
    
    TRINO_URL = f"http://{TRINO_HOST}:{TRINO_PORT}"
    
    # Приложение
    APP_NAME = "Lakehouse Admin Panel"
    APP_VERSION = "1.0.0"