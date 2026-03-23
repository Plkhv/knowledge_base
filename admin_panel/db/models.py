from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class TableMetadata(Base):
    """Метаданные о таблицах в Lakehouse"""
    __tablename__ = "table_metadata"
    
    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), unique=True, nullable=False)
    catalog_name = Column(String(100), default="iceberg")
    schema_name = Column(String(100), default="lakehouse")
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)

class QueryHistory(Base):
    """История запросов"""
    __tablename__ = "query_history"
    
    id = Column(Integer, primary_key=True)
    query_text = Column(Text, nullable=False)
    table_name = Column(String(255))
    executed_at = Column(DateTime, default=datetime.utcnow)
    execution_time_ms = Column(Integer)
    row_count = Column(Integer)
    status = Column(String(50))  # success, error
    error_message = Column(Text)

class User(Base):
    """Пользователи системы (для будущего расширения)"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255))
    role = Column(String(50), default="viewer")  # admin, editor, viewer
    created_at = Column(DateTime, default=datetime.utcnow)