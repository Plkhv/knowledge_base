import os
from pathlib import Path
from dotenv import load_dotenv
from runtime_paths import find_repo_root


_REPO_ROOT = find_repo_root()
_CONFIG_DIR = _REPO_ROOT / "admin"
_ENV_CANDIDATES = [
    _CONFIG_DIR / ".env",
    _REPO_ROOT / ".env",
    _REPO_ROOT / "lakehouse_infra" / ".env",
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
    PG_PASSWORD = _env("PG_PASSWORD", None)
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

    # MinIO (для ссылок/предпросмотра медиа)
    MINIO_ENDPOINT = _env("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_PUBLIC_URL = _env("MINIO_PUBLIC_URL", MINIO_ENDPOINT)
    MINIO_ACCESS_KEY = _env("MINIO_ACCESS_KEY") or _env("MINIO_ROOT_USER", None)
    MINIO_SECRET_KEY = _env("MINIO_SECRET_KEY") or _env("MINIO_ROOT_PASSWORD", None)
    MINIO_PRESIGN_TTL_SECONDS = int(_env("MINIO_PRESIGN_TTL_SECONDS", "3600"))
    
    # Приложение
    APP_NAME = "Lakehouse Admin Panel"
    APP_VERSION = "1.0.0"

    @classmethod
    def validate_security(cls):
        import logging
        _logger = logging.getLogger(__name__)

        if not cls.PG_PASSWORD:
            _logger.warning("PG_PASSWORD is not set. Using an empty or missing DB password is insecure.")
        if not cls.MINIO_SECRET_KEY:
            _logger.warning("MINIO_SECRET_KEY is not set. MinIO access may be insecure if defaults are used.")
        if not cls.ADMIN_BOOTSTRAP_PASSWORD:
            _logger.warning("ADMIN_BOOTSTRAP_PASSWORD is not set. Bootstrap admin account may be unprotected.")

