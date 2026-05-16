from sqlalchemy import create_engine, inspect
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from config import Config
from db.models import Base
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Database:
    _instance = None
    _engine = None
    _session_factory = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        def init_engine(db_url: str):
            return create_engine(db_url, echo=False, pool_pre_ping=True)

        self._engine = init_engine(Config.DATABASE_URL)

        try:
            inspector = inspect(self._engine)
            existing_tables = inspector.get_table_names()
        except OperationalError as e:
            # Частая проблема: Postgres из docker-compose не запущен.
            # Чтобы GUI хотя бы стартовал и работала авторизация, используем локальную SQLite.
            logger.error(
                "Database connection failed for %s. Falling back to local SQLite. Error: %s",
                Config.DATABASE_URL,
                e,
            )

            db_path = Path(__file__).resolve().parents[1] / "admin_local.sqlite3"
            sqlite_url = f"sqlite:///{db_path.as_posix()}"
            self._engine = init_engine(sqlite_url)

            inspector = inspect(self._engine)
            existing_tables = inspector.get_table_names()

        if not existing_tables:
            logger.info("Creating database tables...")
            Base.metadata.create_all(self._engine)
            logger.info("Tables created successfully")
        else:
            logger.info("Tables already exist: %s", existing_tables)

            # Create any newly-added tables (does not alter existing columns).
            Base.metadata.create_all(self._engine)

        # Lightweight migrations for existing databases (no Alembic in this project)
        try:
            self._apply_lightweight_migrations()
        except Exception as e:
            logger.warning("Failed to apply lightweight migrations: %s", e)

        # User objects are returned from services after commit; keep attributes accessible
        # even after the session is closed.
        self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False)

    def _apply_lightweight_migrations(self) -> None:
        inspector = inspect(self._engine)
        if "users" in inspector.get_table_names():
            existing_cols = {c["name"] for c in inspector.get_columns("users")}
            if "allowed_incident_ids" not in existing_cols:
                with self._engine.begin() as conn:
                    conn.execute(text("ALTER TABLE users ADD COLUMN allowed_incident_ids TEXT"))
                logger.info("Migration applied: users.allowed_incident_ids")
    
    @property
    def session(self) -> Session:
        return self._session_factory()
    
    def close(self):
        if self._engine:
            self._engine.dispose()
    
    def get_engine(self):
        return self._engine