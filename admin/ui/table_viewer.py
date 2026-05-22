# ui/table_viewer.py
from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QLabel,
    QSpinBox,
    QHeaderView,
    QMessageBox,
    QInputDialog,
)
from PyQt6.QtCore import QThread, pyqtSignal
import pandas as pd
import time
from db.models import UserRole

class LoadTableThread(QThread):
    finished = pyqtSignal(pd.DataFrame, int)
    error = pyqtSignal(str)
    
    def __init__(self, service, table_name, user_role, current_user, limit=100):
        super().__init__()
        self.service = service
        self.table_name = table_name
        self.user_role = user_role
        self.current_user = current_user
        self.limit = limit
    
    def run(self):
        try:
            start_time = time.time()
            df = self.service.get_table_data(
                self.table_name,
                limit=self.limit,
                user_role=self.user_role,
                current_user=self.current_user,
            )
            exec_time = int((time.time() - start_time) * 1000)
            self.finished.emit(df, exec_time)
        except Exception as e:
            self.error.emit(str(e))

class TableViewerWidget(QWidget):
    data_modified = pyqtSignal()
    
    def __init__(self, table_name: str, admin_service, current_user, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.admin_service = admin_service
        self.current_user = current_user
        self.df = None
        self._original_df = None
        self._pending_updates: dict[int, dict[str, str]] = {}
        self.columns = []
        self.primary_key = None
        self.can_write = admin_service.can_write(current_user.role)
        self.setup_ui()
        self.load_data()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        toolbar = QHBoxLayout()
        
        self.refresh_btn = QPushButton("Обновить")
        self.refresh_btn.clicked.connect(self.load_data)
        toolbar.addWidget(self.refresh_btn)
        
        self.add_btn = QPushButton("➕ Добавить запись")
        self.add_btn.clicked.connect(self.add_record)
        self.add_btn.setEnabled(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.add_btn)
        
        self.delete_btn = QPushButton("🗑️ Удалить запись")
        self.delete_btn.clicked.connect(self.delete_record)
        self.delete_btn.setEnabled(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.delete_btn)
        
        toolbar.addWidget(QLabel("Лимит:"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(10, 10000)
        self.limit_spin.setValue(100)
        self.limit_spin.valueChanged.connect(self.load_data)
        toolbar.addWidget(self.limit_spin)
        
        self.save_btn = QPushButton("💾 Сохранить изменения")
        self.save_btn.clicked.connect(self.save_changes)
        self.save_btn.setEnabled(False)
        self.save_btn.setVisible(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.save_btn)

        self.rollback_btn = QPushButton("⏪ Откат")
        self.rollback_btn.clicked.connect(self.rollback_changes)
        self.rollback_btn.setVisible(getattr(self.current_user, "role", None) == UserRole.ADMIN)
        toolbar.addWidget(self.rollback_btn)
        
        toolbar.addStretch()
        
        self.status_label = QLabel("Готов")
        toolbar.addWidget(self.status_label)
        
        layout.addLayout(toolbar)
        
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        
        if not self.can_write:
            self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        
        self.table.itemChanged.connect(self.on_item_changed)
        layout.addWidget(self.table)

    def rollback_changes(self):
        if getattr(self.current_user, "role", None) != UserRole.ADMIN:
            QMessageBox.warning(self, "Недостаточно прав", "Откат доступен только администратору")
            return

        points = self.admin_service.get_table_change_points(self.table_name, limit=30)
        if not points:
            QMessageBox.information(self, "Откат", "Нет точек сохранения для отката (попробуйте сначала внести изменения и нажать 'Сохранить')")
            return

        by_id = {int(p.id): p for p in points}
        items = []
        for p in points:
            ts = str(getattr(p, "executed_at", ""))
            user = getattr(p, "username", None) or "?"
            action = getattr(p, "action", None) or "CHANGE"
            snap_after = getattr(p, "snapshot_after", None)
            snap_before = getattr(p, "snapshot_before", None)
            details = (getattr(p, "details", None) or "").replace("\n", " ").strip()
            if len(details) > 120:
                details = details[:117] + "..."
            items.append(
                f"{p.id} | {ts} | {user} | {action} | after={snap_after} before={snap_before} | {details}"
            )
        selected, ok = QInputDialog.getItem(
            self,
            "Откат таблицы",
            "Выберите точку сохранения:",
            items,
            0,
            False,
        )
        if not ok or not selected:
            return

        point_id = int(str(selected).split("|")[0].strip())
        point = by_id.get(point_id)
        if point is None:
            QMessageBox.critical(self, "Ошибка", "Не удалось определить snapshot для выбранной точки")
            return

        snapshot_id_raw = getattr(point, "snapshot_after", None)
        if snapshot_id_raw is None:
            snapshot_id_raw = getattr(point, "snapshot_before", None)
        if snapshot_id_raw is None:
            QMessageBox.critical(self, "Ошибка", "Для выбранной точки отсутствует snapshot_id")
            return

        snapshot_id = int(snapshot_id_raw)

        reply = QMessageBox.question(
            self,
            "Подтверждение",
            f"Откатить таблицу {self.table_name} к точке #{point_id} (snapshot {snapshot_id})?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        try:
            self.admin_service.rollback_table_to_snapshot(self.table_name, snapshot_id, self.current_user)
            QMessageBox.information(self, "Откат", "Откат выполнен")
            self.load_data()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
    
    def add_record(self):
        from ui.add_record_dialog import AddRecordDialog
        columns = self.admin_service.get_table_columns(self.table_name)
        
        dialog = AddRecordDialog(self.table_name, columns, self.admin_service, self.current_user, self)
        if dialog.exec():
            self.load_data()
    
    def delete_record(self):
        if not self.can_write:
            QMessageBox.warning(self, "Недостаточно прав", "У вас нет прав на удаление данных")
            return

        if self.df is None or self.df.empty:
            QMessageBox.information(self, "Удаление", "Нет данных для удаления")
            return

        if not self.primary_key:
            QMessageBox.critical(self, "Ошибка", "Не удалось определить первичный ключ для удаления")
            return

        selected = self.table.selectionModel().selectedRows()
        if not selected:
            QMessageBox.information(self, "Удаление", "Выберите строку(и) для удаления")
            return

        pk_values = []
        for model_index in selected:
            row = model_index.row()
            try:
                pk_values.append(self.df.iloc[row][self.primary_key])
            except Exception:
                continue

        if not pk_values:
            QMessageBox.critical(self, "Ошибка", "Не удалось получить значения ключа")
            return

        from ui.delete_dialog import DeleteDialog

        dialog = DeleteDialog(
            self.table_name,
            self.primary_key,
            pk_values,
            self.admin_service,
            self.current_user,
            self,
        )
        if dialog.exec():
            self.load_data()
    
    def save_changes(self):
        if not self.can_write:
            QMessageBox.warning(self, "Недостаточно прав", "У вас нет прав на редактирование данных")
            return

        if not self._pending_updates:
            return

        if self.primary_key is None:
            QMessageBox.critical(self, "Ошибка", "Не удалось определить первичный ключ")
            return

        if self._original_df is None:
            QMessageBox.critical(self, "Ошибка", "Нет исходных данных для сравнения")
            return

        try:
            snap_before = None
            try:
                snap_before = self.admin_service.lakehouse.get_latest_snapshot_id(self.table_name)
            except Exception:
                pass

            changed_rows = 0
            changed_cols: set[str] = set()
            pk_values: list[str] = []

            for row_idx, updates in self._pending_updates.items():
                if not updates:
                    continue

                # Не обновляем поле ключа
                updates = {k: v for k, v in updates.items() if k != self.primary_key}
                if not updates:
                    continue

                pk_value = self._original_df.iloc[row_idx][self.primary_key]
                # pandas/numpy scalar -> native python
                if hasattr(pk_value, "item"):
                    try:
                        pk_value = pk_value.item()
                    except Exception:
                        pass
                pk_values.append(str(pk_value))
                ok = self.admin_service.update_row(
                    self.table_name,
                    self.primary_key,
                    pk_value,
                    updates,
                    user_role=self.current_user.role,
                    current_user=self.current_user,
                    log_change=False,
                )
                if not ok:
                    raise RuntimeError("Не удалось сохранить изменения")

                changed_rows += 1
                changed_cols.update([str(k) for k in updates.keys()])

            snap_after = None
            try:
                snap_after = self.admin_service.lakehouse.get_latest_snapshot_id(self.table_name)
            except Exception:
                pass

            cols_text = ",".join(sorted(changed_cols))
            pk_text = ",".join(pk_values[:10])
            if len(pk_values) > 10:
                pk_text += f" (+{len(pk_values) - 10})"
            details = f"rows={changed_rows}; cols={cols_text}; pk={self.primary_key}; pk_values={pk_text}".strip()
            self.admin_service.log_table_change(
                table_name=self.table_name,
                action="SAVE",
                current_user=self.current_user,
                snapshot_before=snap_before,
                snapshot_after=snap_after,
                details=details,
            )

            self._pending_updates.clear()
            self.save_btn.setEnabled(False)
            self.data_modified.emit()
            self.load_data()
        except PermissionError as e:
            QMessageBox.critical(self, "Недостаточно прав", str(e))
        except Exception as e:
            QMessageBox.critical(self, "Ошибка сохранения", str(e))

    def on_item_changed(self, item: QTableWidgetItem):
        if not self.can_write:
            return
        if self.df is None or self._original_df is None:
            return

        row = item.row()
        col = item.column()
        if row < 0 or col < 0:
            return

        if col >= len(self.columns):
            return

        col_name = self.columns[col]
        new_value = item.text()

        if row not in self._pending_updates:
            self._pending_updates[row] = {}
        self._pending_updates[row][col_name] = new_value
        self.save_btn.setEnabled(True)

    def load_data(self):
        self.status_label.setText("Загрузка...")
        self.refresh_btn.setEnabled(False)
        self.add_btn.setEnabled(False)
        self.delete_btn.setEnabled(False)

        self.loader = LoadTableThread(
            self.admin_service,
            self.table_name,
            self.current_user.role,
            self.current_user,
            self.limit_spin.value(),
        )
        self.loader.finished.connect(self.display_data)
        self.loader.error.connect(self.show_error)
        self.loader.start()

    def display_data(self, df: pd.DataFrame, exec_time_ms: int):
        self.refresh_btn.setEnabled(True)
        self.add_btn.setEnabled(self.can_write)
        self.delete_btn.setEnabled(self.can_write)

        self.df = df
        self._original_df = df.copy(deep=True)
        self._pending_updates.clear()
        self.save_btn.setEnabled(False)

        self.columns = df.columns.tolist()
        self.primary_key = "id" if "id" in self.columns else (self.columns[0] if self.columns else None)

        self.status_label.setText(f"Строк: {len(df)} | {exec_time_ms} мс")

        self.table.blockSignals(True)
        try:
            self.table.clear()
            self.table.setRowCount(len(df))
            self.table.setColumnCount(len(self.columns))
            self.table.setHorizontalHeaderLabels(self.columns)

            for i, row in df.iterrows():
                for j, col_name in enumerate(self.columns):
                    value = row[col_name]
                    self.table.setItem(i, j, QTableWidgetItem("" if value is None else str(value)))

            self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        finally:
            self.table.blockSignals(False)

    def show_error(self, error_msg: str):
        self.status_label.setText("Ошибка загрузки")
        self.refresh_btn.setEnabled(True)
        self.add_btn.setEnabled(self.can_write)
        self.delete_btn.setEnabled(self.can_write)
        QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные:\n{error_msg}")

    def refresh(self):
        self.load_data()