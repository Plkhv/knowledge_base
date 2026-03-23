from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from config import Config
from db.models import Base

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
        self._session_factory = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)
    
    @property
    def session(self) -> Session:
        return self._session_factory()
    
    def close(self):
        if self._engine:
            self._engine.dispose()
    
    def get_engine(self):
        return self._engine