from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum

Base = declarative_base()

class UserRole(enum.Enum):
    ADMIN = "ADMIN"   
    EXPERT = "EXPERT" 
    VIEWER = "VIEWER" 

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    full_name = Column(String(200), nullable=True)
    role = Column(Enum(UserRole), default=UserRole.VIEWER)
    is_active = Column(Boolean, default=True)
    # Comma-separated list of incident identifiers the user is allowed to view.
    # Used for row-level filtering in Trino queries for EXPERT/VIEWER roles.
    allowed_incident_ids = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login = Column(DateTime)
    created_by = Column(Integer)  # ID пользователя, который создал

class TableMetadata(Base):
    __tablename__ = "table_metadata"
    
    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), unique=True, nullable=False)
    catalog_name = Column(String(100), default="memory")
    schema_name = Column(String(100), default="default")
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)

class QueryHistory(Base):
    __tablename__ = "query_history"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=True)  # ID пользователя, выполнившего запрос
    username = Column(String(100), nullable=True)  # Имя пользователя
    query_text = Column(Text, nullable=False)
    table_name = Column(String(255))
    executed_at = Column(DateTime, default=datetime.utcnow)
    execution_time_ms = Column(Integer)
    row_count = Column(Integer)
    status = Column(String(50))
    error_message = Column(Text)


class TableChangeLog(Base):
    __tablename__ = "table_change_log"

    id = Column(Integer, primary_key=True)
    table_name = Column(String(255), nullable=False)
    user_id = Column(Integer, nullable=True)
    username = Column(String(100), nullable=True)
    action = Column(String(50), nullable=False)  # SAVE/INSERT/UPDATE/DELETE/ROLLBACK
    executed_at = Column(DateTime, default=datetime.utcnow)
    snapshot_before = Column(Integer, nullable=True)
    snapshot_after = Column(Integer, nullable=True)
    details = Column(Text, nullable=True)