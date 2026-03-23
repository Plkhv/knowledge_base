import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from config import Config
import logging

logger = logging.getLogger(__name__)

class LakehouseService:
    """Сервис для работы с Lakehouse через Trino"""
    
    def __init__(self):
        self.conn_params = {
            "host": Config.TRINO_HOST,
            "port": Config.TRINO_PORT,
            "user": "admin",
            "catalog": Config.TRINO_CATALOG,
            "schema": Config.TRINO_SCHEMA,
        }
    
    def _get_connection(self):
        """Получить соединение с Trino"""
        return connect(**self.conn_params)
    
    def list_tables(self) -> list:
        """Получить список всех таблиц в Lakehouse"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                return tables
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> list:
        """Получить схему таблицы"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE {table_name}")
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            return []
    
    def query_table(self, table_name: str, limit: int = 100, offset: int = 0) -> pd.DataFrame:
        """Выполнить SELECT запрос к таблице"""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
            return pd.read_sql(query, self._get_connection())
        except Exception as e:
            logger.error(f"Error querying table {table_name}: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Выполнить произвольный SQL запрос"""
        try:
            return pd.read_sql(query, self._get_connection())
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def get_table_count(self, table_name: str) -> int:
        """Получить количество строк в таблице"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting count for {table_name}: {e}")
            return 0