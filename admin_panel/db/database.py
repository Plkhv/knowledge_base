from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, Session
from config import Config
from db.models import Base
import logging

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
        self._engine = create_engine(
            Config.DATABASE_URL,
            echo=False,
            pool_pre_ping=True
        )
        
        # Проверяем, существуют ли таблицы
        inspector = inspect(self._engine)
        existing_tables = inspector.get_table_names()
        
        if not existing_tables:
            # Только если таблиц нет, создаём их
            logger.info("Creating database tables...")
            Base.metadata.create_all(self._engine)
            logger.info("Tables created successfully")
        else:
            logger.info(f"Tables already exist: {existing_tables}")
        
        self._session_factory = sessionmaker(bind=self._engine)
    
    @property
    def session(self) -> Session:
        return self._session_factory()
    
    def close(self):
        if self._engine:
            self._engine.dispose()
    
    def get_engine(self):
        return self._engine