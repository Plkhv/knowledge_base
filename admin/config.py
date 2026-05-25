import os
from pathlib import Path
from dotenv import load_dotenv


_CONFIG_DIR = Path(__file__).resolve().parent
_ENV_CANDIDATES = [
    _CONFIG_DIR / ".env",
    _CONFIG_DIR.parent / ".env",
    _CONFIG_DIR.parent / "lakehouse_infra" / ".env",
]

for _env_path in _ENV_CANDIDATES:
    if _env_path.exists():
        load_dotenv(dotenv_path=_env_path, override=False)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1].strip()
    return value or default

class Config:
    # Можно явно задать DATABASE_URL (например, для SQLite при разработке)
    DATABASE_URL = _env("DATABASE_URL")

    # PostgreSQL (административная база)
    PG_HOST = _env("PG_HOST", "localhost")
    PG_PORT = _env("PG_PORT", "5432")
    PG_DATABASE = _env("PG_DATABASE", "polaris")
    PG_USER = _env("PG_USER", "polaris")
    PG_PASSWORD = _env("PG_PASSWORD", "password")
    ADMIN_BOOTSTRAP_PASSWORD = _env("ADMIN_BOOTSTRAP_PASSWORD") or _env("AIRFLOW_ADMIN_PASSWORD")

    # Если DATABASE_URL не задан — собираем URL для Postgres из отдельных переменных
    if not DATABASE_URL and all([PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE]):
        DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    elif not DATABASE_URL:
        DATABASE_URL = None
    
    # Trino (для запросов к Lakehouse)
    TRINO_HOST = _env("TRINO_HOST", "localhost")
    TRINO_PORT = _env("TRINO_PORT", "8082")
    TRINO_CATALOG = _env("TRINO_CATALOG", "iceberg")
    TRINO_SCHEMA = _env("TRINO_SCHEMA", "mine")
    # Trino requires a user header (X-Trino-User). Default keeps backward compatibility.
    TRINO_USER = _env("TRINO_USER", "admin")
    
    TRINO_URL = f"http://{TRINO_HOST}:{TRINO_PORT}"
    
    # Приложение
    APP_NAME = "Lakehouse Admin Panel"
    APP_VERSION = "1.0.0"

