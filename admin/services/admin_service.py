# services/admin_service.py
from sqlalchemy import create_engine, text, or_
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import NoSuchModuleError
from db.database import Database
from db.models import TableMetadata, QueryHistory, User, UserRole, TableChangeLog
from config import Config
import pandas as pd
import logging
import bcrypt
from datetime import datetime

from services.lakehouse_service import LakehouseService
import re
import numbers
from typing import Optional

logger = logging.getLogger(__name__)

_SAFE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_INCIDENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")

class AdminService:
    def __init__(self):
        self.db = Database()
        self.lakehouse = LakehouseService()
        self.trino_engine = None
        try:
            self.trino_engine = create_engine(
                f"trino://{Config.TRINO_USER}@{Config.TRINO_HOST}:{Config.TRINO_PORT}/{Config.TRINO_CATALOG}/{Config.TRINO_SCHEMA}",
                connect_args={"http_scheme": "http"}
            )
        except NoSuchModuleError as e:
            # SQLAlchemy dialect for Trino may be missing; fallback to trino.dbapi in LakehouseService.
            logger.warning("Trino SQLAlchemy dialect is not available; falling back to trino.dbapi. Error: %s", e)
        self._init_default_admin()

    @staticmethod
    def _normalize_role(role) -> UserRole:
        if isinstance(role, UserRole):
            return role
        if role is None:
            return UserRole.VIEWER

        role_str = str(role).strip()
        if not role_str:
            return UserRole.VIEWER

        role_upper = role_str.upper()
        # Support UI legacy values like "expert" / "viewer"
        if role_upper in {"ADMIN", "EXPERT", "VIEWER"}:
            return UserRole(role_upper)
        if role_upper in {"АДМИН", "ADMINISTRATOR"}:
            return UserRole.ADMIN
        if role_upper in {"EXPERT", "ЭКСПЕРТ"}:
            return UserRole.EXPERT
        if role_upper in {"VIEWER", "ОБЗОР", "НАБЛЮДАТЕЛЬ"}:
            return UserRole.VIEWER

        # Last resort: try enum constructor (will raise ValueError)
        return UserRole(role_str)
    
    def _init_default_admin(self):
        """Создание default администратора при первом запуске"""
        session = self.db.session
        try:
            existing = session.query(User).filter_by(username="admin").first()
            if not existing:
                hashed = bcrypt.hashpw("admin123".encode('utf-8'), bcrypt.gensalt())
                admin = User(
                    username="admin",
                    password_hash=hashed.decode('utf-8'),
                    full_name="System Administrator",
                    role=UserRole.ADMIN,
                    created_by=0
                )
                session.add(admin)
                session.commit()
                logger.info("Default admin user created: admin / admin123")
        except Exception as e:
            logger.error(f"Error creating default admin: {e}")
        finally:
            session.close()
    
    # ==================== Аутентификация ====================
    
    def authenticate(self, username: str, password: str):
        """Аутентификация пользователя"""
        session = self.db.session
        try:
            user = session.query(User).filter_by(username=username, is_active=True).first()
            if user and bcrypt.checkpw(password.encode('utf-8'), user.password_hash.encode('utf-8')):
                # Обновляем время последнего входа
                user.last_login = datetime.utcnow()
                session.commit()
                return user
            return None
        finally:
            session.close()
    
    def get_current_user(self, user_id: int):
        """Получение пользователя по ID"""
        session = self.db.session
        try:
            return session.query(User).filter_by(id=user_id).first()
        finally:
            session.close()
    
    # ==================== CRUD пользователей (только для ADMIN) ====================
    
    def get_all_users(self):
        """Получить всех пользователей"""
        session = self.db.session
        try:
            return session.query(User).all()
        finally:
            session.close()
    
    def create_user(
        self,
        username: str,
        password: str,
        full_name: str,
        role: str,
        created_by: int,
        allowed_incident_ids: Optional[str] = None,
    ) -> tuple:
        """Создать нового пользователя."""
        session = self.db.session
        try:
            existing = session.query(User).filter_by(username=username).first()
            if existing:
                return False, "Имя пользователя уже существует"

            hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            user = User(
                username=username,
                password_hash=hashed.decode('utf-8'),
                full_name=full_name,
                role=self._normalize_role(role),
                created_by=created_by,
                allowed_incident_ids=self._normalize_allowed_incident_ids(allowed_incident_ids),
            )
            session.add(user)
            session.commit()
            return True, user.id
        except Exception as e:
            session.rollback()
            return False, str(e)
        finally:
            session.close()
    
    def update_user(self, user_id: int, **kwargs):
        """Обновить пользователя"""
        session = self.db.session
        try:
            user = session.query(User).filter_by(id=user_id).first()
            if not user:
                return False, "Пользователь не найден"
            
            for key, value in kwargs.items():
                if key == 'password' and value:
                    user.password_hash = bcrypt.hashpw(value.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                elif key == 'role' and value:
                    user.role = self._normalize_role(value)
                elif key == 'full_name':
                    user.full_name = value
                elif key == 'is_active':
                    user.is_active = value
                elif key == 'allowed_incident_ids':
                    user.allowed_incident_ids = self._normalize_allowed_incident_ids(value)
            
            session.commit()
            return True, user
        except Exception as e:
            session.rollback()
            return False, str(e)
        finally:
            session.close()

    @staticmethod
    def _parse_incident_ids(value: Optional[str]) -> list[str]:
        if value is None:
            return []
        raw = str(value).strip()
        if not raw:
            return []
        # allow comma/semicolon/whitespace/newline separated
        parts = re.split(r"[\s,;]+", raw)
        incident_ids: list[str] = []
        seen: set[str] = set()
        for p in parts:
            p = p.strip()
            if not p:
                continue
            if not _SAFE_INCIDENT_RE.match(p):
                raise ValueError(f"Некорректный идентификатор инцидента: {p}")
            if p not in seen:
                seen.add(p)
                incident_ids.append(p)
        return incident_ids

    def _normalize_allowed_incident_ids(self, value: Optional[str]) -> Optional[str]:
        try:
            ids = self._parse_incident_ids(value)
        except Exception:
            # Keep original error surface for UI
            raise
        if not ids:
            return None
        return ",".join(ids)

    def _get_allowed_incident_ids_for_user(self, current_user) -> list[str]:
        allowed_raw = getattr(current_user, "allowed_incident_ids", None)
        return self._parse_incident_ids(allowed_raw)
    
    def delete_user(self, user_id: int):
        """Удалить пользователя"""
        session = self.db.session
        try:
            user = session.query(User).filter_by(id=user_id).first()
            if user and user.username != 'admin':  # Нельзя удалить главного админа
                session.delete(user)
                session.commit()
                return True, "Пользователь удалён"
            return False, "Нельзя удалить администратора"
        except Exception as e:
            session.rollback()
            return False, str(e)
        finally:
            session.close()
    
    # ==================== Проверка прав доступа ====================
    
    def can_read(self, user_role: UserRole) -> bool:
        """Проверка права на чтение"""
        return True  # Все роли могут читать
    
    def can_write(self, user_role: UserRole) -> bool:
        """Проверка права на запись (CREATE, UPDATE, DELETE)"""
        return user_role in [UserRole.ADMIN, UserRole.EXPERT]
    
    def can_manage_users(self, user_role: UserRole) -> bool:
        """Проверка права на управление пользователями"""
        return user_role == UserRole.ADMIN
    
    # ==================== Логирование запросов с user_id ====================
    
    def log_query(self, user_id: int, username: str, query_text: str, table_name: str, 
                  execution_time_ms: int, row_count: int, status: str, error_message: str = None):
        """Логирование запроса с указанием пользователя"""
        session = self.db.session
        try:
            history = QueryHistory(
                user_id=user_id,
                username=username,
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
    
    def get_query_history(self, limit: int = 100, user_id: int = None):
        """Получить историю запросов (все или только для конкретного пользователя)"""
        session = self.db.session
        try:
            query = session.query(QueryHistory)
            if user_id:
                query = query.filter_by(user_id=user_id)
            return query.order_by(QueryHistory.executed_at.desc()).limit(limit).all()
        finally:
            session.close()

    # ==================== Iceberg snapshots / rollback ====================

    def log_table_change(
        self,
        table_name: str,
        action: str,
        current_user=None,
        snapshot_before: int | None = None,
        snapshot_after: int | None = None,
        details: str | None = None,
    ) -> None:
        session = self.db.session
        try:
            entry = TableChangeLog(
                table_name=str(table_name),
                user_id=getattr(current_user, "id", None) if current_user is not None else None,
                username=getattr(current_user, "username", None) if current_user is not None else None,
                action=str(action),
                snapshot_before=snapshot_before,
                snapshot_after=snapshot_after,
                details=details,
            )
            session.add(entry)
            session.commit()
        except Exception as e:
            logger.warning("Error logging table change: %s", e)
            session.rollback()
        finally:
            session.close()

    def get_table_change_points(self, table_name: str, limit: int = 50) -> list[TableChangeLog]:
        """Return user-facing rollback points for a table.

        We intentionally include only entries that have snapshot_after set.
        """
        session = self.db.session
        try:
            return (
                session.query(TableChangeLog)
                .filter(TableChangeLog.table_name == str(table_name))
                .filter(TableChangeLog.action.in_(["SAVE", "INSERT", "DELETE", "ROLLBACK"]))
                .filter(
                    or_(
                        TableChangeLog.snapshot_after.isnot(None),
                        TableChangeLog.snapshot_before.isnot(None),
                    )
                )
                .order_by(TableChangeLog.executed_at.desc())
                .limit(int(limit))
                .all()
            )
        finally:
            session.close()

    def get_table_snapshots(self, table_name: str, limit: int = 20) -> list[tuple[int, str, str]]:
        return self.lakehouse.list_snapshots(table_name, limit=limit)

    def rollback_table_to_snapshot(self, table_name: str, snapshot_id: int, current_user) -> None:
        if current_user is None or current_user.role != UserRole.ADMIN:
            raise PermissionError("Откат доступен только администратору")

        snap_before = None
        try:
            snap_before = self.lakehouse.get_latest_snapshot_id(table_name)
        except Exception:
            pass

        self.lakehouse.rollback_to_snapshot(table_name, snapshot_id)

        snap_after = None
        try:
            snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
        except Exception:
            pass

        self.log_table_change(
            table_name=table_name,
            action="ROLLBACK",
            current_user=current_user,
            snapshot_before=snap_before,
            snapshot_after=snap_after,
            details=f"rollback_to_snapshot={int(snapshot_id)}",
        )
    
    # ==================== CRUD для таблиц Lakehouse (с проверкой прав) ====================
    
    def get_table_data(
        self,
        table_name: str,
        limit: int = 100,
        user_role: UserRole = None,
        current_user=None,
    ) -> pd.DataFrame:
        """Получить данные таблицы (с проверкой прав на чтение).

        Для ролей EXPERT/VIEWER применяется фильтр по incident_id согласно users.allowed_incident_ids.
        """
        if not self.can_read(user_role):
            raise PermissionError("У вас нет прав на просмотр данных")

        query = f"SELECT * FROM {table_name}"
        role = self._normalize_role(user_role) if user_role is not None else None
        if role in {UserRole.EXPERT, UserRole.VIEWER}:
            if current_user is None:
                raise PermissionError("Не удалось определить пользователя для фильтрации по инцидентам")
            cols = [c.lower() for c in self.get_table_columns(table_name)]
            # Apply incident-based row-level filter only when the table has incident_id.
            # Otherwise, treat it as a global/reference table and allow viewing all rows.
            if "incident_id" in cols:
                allowed_ids = self._get_allowed_incident_ids_for_user(current_user)
                if not allowed_ids:
                    raise PermissionError("Для пользователя не назначены доступные инциденты")

                ids_sql = ",".join([f"'{i}'" for i in allowed_ids])
                query += f" WHERE incident_id IN ({ids_sql})"

        query += f" LIMIT {int(limit)}"
        try:
            if self.trino_engine is not None:
                return pd.read_sql(query, self.trino_engine)
            return self.lakehouse.execute_query(query)
        except Exception as e:
            logger.error(f"Error getting table data: {e}")
            raise
    
    def get_table_columns(self, table_name: str) -> list:
        try:
            if self.trino_engine is not None:
                result = pd.read_sql(f"SHOW COLUMNS FROM {table_name}", self.trino_engine)
                return result['Column'].tolist()

            described = self.lakehouse.get_table_schema(table_name)
            # Trino DESCRIBE returns tuples like (Column, Type, Extra, Comment)
            return [row[0] for row in described if row and row[0]]
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
    
    def insert_row(
        self,
        table_name: str,
        values: dict,
        user_role: UserRole = None,
        current_user=None,
        log_change: bool = True,
    ) -> bool:
        """Вставить новую строку (с проверкой прав на запись)"""
        if not self.can_write(user_role):
            raise PermissionError("У вас нет прав на добавление данных")

        def is_safe_table(name: str) -> bool:
            parts = str(name).split(".")
            return all(_SAFE_IDENT_RE.match(p or "") for p in parts)

        def is_safe_col(name: str) -> bool:
            return bool(_SAFE_IDENT_RE.match(str(name)))

        def quote(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, numbers.Number):
                return str(v)
            s = str(v)
            s = s.replace("'", "''")
            return f"'{s}'"
        
        try:
            snap_before = None
            if log_change:
                try:
                    snap_before = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass

            if not is_safe_table(table_name):
                raise ValueError("Invalid table name")
            if not all(is_safe_col(k) for k in values.keys()):
                raise ValueError("Invalid column name")

            columns = ", ".join(values.keys())

            if self.trino_engine is not None:
                placeholders = ", ".join([f":{k}" for k in values.keys()])
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                with self.trino_engine.connect() as conn:
                    conn.execute(text(query), values)
                    conn.commit()
                    snap_after = None
                    if log_change:
                        try:
                            snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                        except Exception:
                            pass
                    if log_change:
                        cols = ",".join(sorted([str(k) for k in values.keys()]))
                        self.log_table_change(
                            table_name=table_name,
                            action="INSERT",
                            current_user=current_user,
                            snapshot_before=snap_before,
                            snapshot_after=snap_after,
                            details=f"cols={cols}" if cols else None,
                        )
                    return True

            # Fallback: execute via trino.dbapi
            values_sql = ", ".join(quote(v) for v in values.values())
            statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values_sql})"
            self.lakehouse.execute_statement(statement)
            snap_after = None
            if log_change:
                try:
                    snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass
            if log_change:
                cols = ",".join(sorted([str(k) for k in values.keys()]))
                self.log_table_change(
                    table_name=table_name,
                    action="INSERT",
                    current_user=current_user,
                    snapshot_before=snap_before,
                    snapshot_after=snap_after,
                    details=f"cols={cols}" if cols else None,
                )
            return True
        except Exception as e:
            logger.error(f"Error inserting row: {e}")
            return False
    
    def update_row(
        self,
        table_name: str,
        primary_key: str,
        primary_key_value,
        updates: dict,
        user_role: UserRole = None,
        current_user=None,
        log_change: bool = True,
    ) -> bool:
        """Обновить строку (с проверкой прав на запись)"""
        if not self.can_write(user_role):
            raise PermissionError("У вас нет прав на редактирование данных")

        def is_safe_table(name: str) -> bool:
            parts = str(name).split(".")
            return all(_SAFE_IDENT_RE.match(p or "") for p in parts)

        def is_safe_col(name: str) -> bool:
            return bool(_SAFE_IDENT_RE.match(str(name)))

        def quote(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, numbers.Number):
                return str(v)
            s = str(v)
            s = s.replace("'", "''")
            return f"'{s}'"
        
        try:
            snap_before = None
            if log_change:
                try:
                    snap_before = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass

            if not is_safe_table(table_name):
                raise ValueError("Invalid table name")
            if not is_safe_col(primary_key):
                raise ValueError("Invalid primary key")
            if not all(is_safe_col(k) for k in updates.keys()):
                raise ValueError("Invalid column name")

            if self.trino_engine is not None:
                set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
                params = {**updates, "pk_value": primary_key_value}
                query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = :pk_value"
                with self.trino_engine.connect() as conn:
                    conn.execute(text(query), params)
                    conn.commit()
                    snap_after = None
                    if log_change:
                        try:
                            snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                        except Exception:
                            pass
                    if log_change:
                        cols = ",".join(sorted([str(k) for k in updates.keys()]))
                        pkv = str(primary_key_value)
                        if len(pkv) > 120:
                            pkv = pkv[:117] + "..."
                        self.log_table_change(
                            table_name=table_name,
                            action="UPDATE",
                            current_user=current_user,
                            snapshot_before=snap_before,
                            snapshot_after=snap_after,
                            details=f"pk={primary_key}={pkv}; cols={cols}" if cols else f"pk={primary_key}={pkv}",
                        )
                    return True

            set_clause = ", ".join([f"{k} = {quote(v)}" for k, v in updates.items()])
            statement = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = {quote(primary_key_value)}"
            self.lakehouse.execute_statement(statement)
            snap_after = None
            if log_change:
                try:
                    snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass
            if log_change:
                cols = ",".join(sorted([str(k) for k in updates.keys()]))
                pkv = str(primary_key_value)
                if len(pkv) > 120:
                    pkv = pkv[:117] + "..."
                self.log_table_change(
                    table_name=table_name,
                    action="UPDATE",
                    current_user=current_user,
                    snapshot_before=snap_before,
                    snapshot_after=snap_after,
                    details=f"pk={primary_key}={pkv}; cols={cols}" if cols else f"pk={primary_key}={pkv}",
                )
            return True
        except Exception as e:
            logger.error(f"Error updating row: {e}")
            return False
    
    def delete_row(
        self,
        table_name: str,
        primary_key: str,
        primary_key_value,
        user_role: UserRole = None,
        current_user=None,
        log_change: bool = True,
    ) -> bool:
        """Удалить строку (с проверкой прав на запись)"""
        if not self.can_write(user_role):
            raise PermissionError("У вас нет прав на удаление данных")

        def is_safe_table(name: str) -> bool:
            parts = str(name).split(".")
            return all(_SAFE_IDENT_RE.match(p or "") for p in parts)

        def is_safe_col(name: str) -> bool:
            return bool(_SAFE_IDENT_RE.match(str(name)))

        def quote(v) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, numbers.Number):
                return str(v)
            s = str(v)
            s = s.replace("'", "''")
            return f"'{s}'"
        
        try:
            snap_before = None
            if log_change:
                try:
                    snap_before = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass

            if not is_safe_table(table_name):
                raise ValueError("Invalid table name")
            if not is_safe_col(primary_key):
                raise ValueError("Invalid primary key")

            if self.trino_engine is not None:
                query = f"DELETE FROM {table_name} WHERE {primary_key} = :pk_value"
                with self.trino_engine.connect() as conn:
                    conn.execute(text(query), {"pk_value": primary_key_value})
                    conn.commit()
                    snap_after = None
                    if log_change:
                        try:
                            snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                        except Exception:
                            pass
                    if log_change:
                        pkv = str(primary_key_value)
                        if len(pkv) > 120:
                            pkv = pkv[:117] + "..."
                        self.log_table_change(
                            table_name=table_name,
                            action="DELETE",
                            current_user=current_user,
                            snapshot_before=snap_before,
                            snapshot_after=snap_after,
                            details=f"pk={primary_key}={pkv}",
                        )
                    return True

            statement = f"DELETE FROM {table_name} WHERE {primary_key} = {quote(primary_key_value)}"
            self.lakehouse.execute_statement(statement)
            snap_after = None
            if log_change:
                try:
                    snap_after = self.lakehouse.get_latest_snapshot_id(table_name)
                except Exception:
                    pass
            if log_change:
                pkv = str(primary_key_value)
                if len(pkv) > 120:
                    pkv = pkv[:117] + "..."
                self.log_table_change(
                    table_name=table_name,
                    action="DELETE",
                    current_user=current_user,
                    snapshot_before=snap_before,
                    snapshot_after=snap_after,
                    details=f"pk={primary_key}={pkv}",
                )
            return True
        except Exception as e:
            logger.error(f"Error deleting row: {e}")
            return False
    
    # ==================== Table Metadata ====================
    
    def get_all_tables_metadata(self):
        session = self.db.session
        try:
            return session.query(TableMetadata).all()
        finally:
            session.close()
    
    def add_table_metadata(self, table_name: str, description: str = "", 
                          catalog: str = "memory", schema: str = "default"):
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
            return False, str(e)
        finally:
            session.close()
    
    def update_table_metadata(self, table_id: int, **kwargs):
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