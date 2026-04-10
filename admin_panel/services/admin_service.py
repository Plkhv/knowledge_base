from db.database import Database
from db.models import TableMetadata, QueryHistory, User
from services.lakehouse_service import LakehouseService
from sqlalchemy import desc
import time
import logging

logger = logging.getLogger(__name__)

class AdminService:
    """Сервис для CRUD операций админ-панели"""
    
    def __init__(self):
        self.db = Database()
        self.lakehouse = LakehouseService()
    
    # ==================== Table Metadata CRUD ====================
    
    def get_all_tables_metadata(self):
        """Получить все метаданные таблиц"""
        session = self.db.session
        try:
            return session.query(TableMetadata).all()
        finally:
            session.close()
    
    def add_table_metadata(self, table_name: str, description: str = "", catalog: str = "iceberg", schema: str = "lakehouse"):
        """Добавить метаданные таблицы"""
        session = self.db.session
        try:
            existing = session.query(TableMetadata).filter_by(table_name=table_name).first()
            if existing:
                return False, "Table already exists"
            
            metadata = TableMetadata(
                table_name=table_name,
                catalog_name=catalog,
                schema_name=schema,
                description=description
            )
            session.add(metadata)
            session.commit()
            return True, metadata
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding table metadata: {e}")
            return False, str(e)
        finally:
            session.close()
    
    def update_table_metadata(self, table_id: int, **kwargs):
        """Обновить метаданные таблицы"""
        session = self.db.session
        try:
            metadata = session.query(TableMetadata).filter_by(id=table_id).first()
            if not metadata:
                return False, "Table not found"
            
            for key, value in kwargs.items():
                if hasattr(metadata, key):
                    setattr(metadata, key, value)
            
            session.commit()
            return True, metadata
        except Exception as e:
            session.rollback()
            return False, str(e)
        finally:
            session.close()
    
    def delete_table_metadata(self, table_id: int):
        """Удалить метаданные таблицы"""
        session = self.db.session
        try:
            metadata = session.query(TableMetadata).filter_by(id=table_id).first()
            if metadata:
                session.delete(metadata)
                session.commit()
                return True, "Deleted"
            return False, "Table not found"
        except Exception as e:
            session.rollback()
            return False, str(e)
        finally:
            session.close()
    
    # ==================== Query History ====================
    
    def log_query(self, query_text: str, table_name: str, execution_time_ms: int, 
                  row_count: int, status: str, error_message: str = None):
        """Логирование запроса"""
        session = self.db.session
        try:
            history = QueryHistory(
                query_text=query_text[:500],
                table_name=table_name[:255],
                execution_time_ms=execution_time_ms,
                row_count=row_count,
                status=status,
                error_message=error_message[:500] if error_message else None
            )
            session.add(history)
            session.commit()
        except Exception as e:
            logger.error(f"Error logging query: {e}")
            session.rollback()
        finally:
            session.close()
    
    def get_query_history(self, limit: int = 100):
        """Получить историю запросов"""
        session = self.db.session
        try:
            return session.query(QueryHistory).order_by(desc(QueryHistory.executed_at)).limit(limit).all()
        finally:
            session.close()
    
    def clear_history(self):
        """Очистить историю"""
        session = self.db.session
        try:
            session.query(QueryHistory).delete()
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
        finally:
            session.close()
    
    # ==================== Lakehouse Operations ====================
    
    def get_tables_from_lakehouse(self):
        """Получить список таблиц из Lakehouse"""
        try:
            return self.lakehouse.list_tables()
        except Exception as e:
            logger.error(f"Error getting tables from lakehouse: {e}")
            return []
    
    def preview_table(self, table_name: str, limit: int = 100):
        """Предпросмотр таблицы"""
        try:
            start_time = time.time()
            df = self.lakehouse.query_table(table_name, limit=limit)
            execution_time = int((time.time() - start_time) * 1000)
            
            self.log_query(
                query_text=f"SELECT * FROM {table_name} LIMIT {limit}",
                table_name=table_name,
                execution_time_ms=execution_time,
                row_count=len(df),
                status="success"
            )
            return df
        except Exception as e:
            self.log_query(
                query_text=f"SELECT * FROM {table_name} LIMIT {limit}",
                table_name=table_name,
                execution_time_ms=0,
                row_count=0,
                status="error",
                error_message=str(e)
            )
            raise
    
    def execute_custom_query(self, query: str):
        """Выполнить произвольный SQL запрос"""
        try:
            start_time = time.time()
            df = self.lakehouse.execute_query(query)
            execution_time = int((time.time() - start_time) * 1000)
            
            # Извлекаем имя таблицы из запроса (упрощённо)
            table_name = "custom"
            if "FROM" in query.upper():
                parts = query.upper().split("FROM")[1].split()[0]
                table_name = parts
            
            self.log_query(
                query_text=query,
                table_name=table_name,
                execution_time_ms=execution_time,
                row_count=len(df),
                status="success"
            )
            return df
        except Exception as e:
            self.log_query(
                query_text=query,
                table_name="custom",
                execution_time_ms=0,
                row_count=0,
                status="error",
                error_message=str(e)
            )
            raise