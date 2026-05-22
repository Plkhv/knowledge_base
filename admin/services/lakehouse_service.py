import pandas as pd
from trino.dbapi import connect
from config import Config
import logging
import re

logger = logging.getLogger(__name__)

_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

class LakehouseService:
    def __init__(self):
        self.conn_params = {
            "host": Config.TRINO_HOST,
            "port": Config.TRINO_PORT,
            "user": Config.TRINO_USER,
            "catalog": Config.TRINO_CATALOG,
            "schema": Config.TRINO_SCHEMA,
        }
    
    def _get_connection(self):
        return connect(**self.conn_params)

    @staticmethod
    def _split_table_name(table_name: str) -> tuple[str, str]:
        """Return (schema, table) for a possibly-qualified name."""
        raw = str(table_name).strip()
        parts = raw.split(".")
        if len(parts) == 1:
            schema = Config.TRINO_SCHEMA
            base = parts[0]
        elif len(parts) == 2:
            schema, base = parts
        else:
            raise ValueError("Invalid table name")

        if not _SAFE_IDENT_RE.match(schema or ""):
            raise ValueError("Invalid schema name")
        if not _SAFE_IDENT_RE.match(base or ""):
            raise ValueError("Invalid table name")
        return schema, base

    @staticmethod
    def _iceberg_snapshots_table(schema: str, base_table: str) -> str:
        # Metadata tables require quoting because of '$'
        return f'"{schema}"."{base_table}$snapshots"'

    def list_snapshots(self, table_name: str, limit: int = 20) -> list[tuple[int, str, str]]:
        """Return latest Iceberg snapshots for a table: (snapshot_id, committed_at, operation)."""
        try:
            schema, base = self._split_table_name(table_name)
            meta = self._iceberg_snapshots_table(schema, base)
            q = (
                "SELECT snapshot_id, committed_at, operation "
                f"FROM {meta} "
                "ORDER BY committed_at DESC "
                f"LIMIT {int(limit)}"
            )
            with self._get_connection() as conn:
                cur = conn.cursor()
                cur.execute(q)
                rows = cur.fetchall() or []
            out: list[tuple[int, str, str]] = []
            for sid, committed_at, op in rows:
                out.append((int(sid), str(committed_at), str(op) if op is not None else ""))
            return out
        except Exception as e:
            logger.warning("Could not list snapshots for %s: %s", table_name, e)
            return []

    def get_latest_snapshot_id(self, table_name: str) -> int | None:
        snaps = self.list_snapshots(table_name, limit=1)
        if not snaps:
            return None
        return snaps[0][0]

    def rollback_to_snapshot(self, table_name: str, snapshot_id: int) -> None:
        """Rollback an Iceberg table to a given snapshot id via Trino procedure."""
        schema, base = self._split_table_name(table_name)
        catalog = Config.TRINO_CATALOG
        if not _SAFE_IDENT_RE.match(catalog or ""):
            raise ValueError("Invalid catalog name")

        stmt = f"CALL {catalog}.system.rollback_to_snapshot('{schema}', '{base}', {int(snapshot_id)})"
        self.execute_statement(stmt)
    
    def list_tables(self) -> list:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> list:
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE {table_name}")
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting schema: {e}")
            return []
    
    def query_table(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return pd.read_sql(query, self._get_connection())
        except Exception as e:
            logger.error(f"Error querying table: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            return pd.read_sql(query, self._get_connection())
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    def execute_statement(self, statement: str) -> None:
        """Выполнить SQL-оператор без ожидания табличного результата (INSERT/UPDATE/DELETE)."""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(statement)
                # Для некоторых драйверов требуется дочитать результат
                try:
                    cursor.fetchall()
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Error executing statement: {e}")
            raise