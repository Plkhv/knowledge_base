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
    QDialog,
    QScrollArea,
    QFileDialog,
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QUrl
from PyQt6.QtGui import QDesktopServices, QPixmap, QFont
import pandas as pd
import time
from pathlib import Path
from urllib.request import urlopen
from db.models import UserRole


class ImagePreviewDialog(QDialog):
    def __init__(self, pixmap: QPixmap, title: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(1000, 700)
        self._base_pixmap = pixmap
        self._zoom = 1.0

        layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        zoom_out_btn = QPushButton("-")
        zoom_out_btn.clicked.connect(self.zoom_out)
        controls.addWidget(zoom_out_btn)

        zoom_in_btn = QPushButton("+")
        zoom_in_btn.clicked.connect(self.zoom_in)
        controls.addWidget(zoom_in_btn)

        fit_btn = QPushButton("Сброс масштаба")
        fit_btn.clicked.connect(self.reset_zoom)
        controls.addWidget(fit_btn)
        controls.addStretch()
        layout.addLayout(controls)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.image_label = QLabel()
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scroll.setWidget(self.image_label)
        layout.addWidget(self.scroll)

        self.apply_zoom()

    def apply_zoom(self):
        if self._base_pixmap.isNull():
            return
        w = max(1, int(self._base_pixmap.width() * self._zoom))
        h = max(1, int(self._base_pixmap.height() * self._zoom))
        scaled = self._base_pixmap.scaled(
            w,
            h,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.image_label.setPixmap(scaled)

    def zoom_in(self):
        self._zoom = min(5.0, self._zoom * 1.25)
        self.apply_zoom()

    def zoom_out(self):
        self._zoom = max(0.2, self._zoom / 1.25)
        self.apply_zoom()

    def reset_zoom(self):
        self._zoom = 1.0
        self.apply_zoom()

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

    MEDIA_COLUMNS = {
        "graphic_reestr": {"link", "source_file"},
        "audio_record": {"link", "source_file"},
    }
    DOWNLOAD_COLUMN_KEY = "__download__"
    IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp", ".tif", ".tiff", ".svg"}
    AUDIO_EXTENSIONS = {".mp3", ".wav", ".ogg", ".flac", ".aac", ".m4a"}
    
    def __init__(self, table_name: str, admin_service, current_user, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.admin_service = admin_service
        self.current_user = current_user
        self.df = None
        self._original_df = None
        self._pending_updates: dict[int, dict[str, str]] = {}
        self.columns = []
        self.display_columns = []
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
        
        self.add_btn = QPushButton("Добавить запись")
        self.add_btn.clicked.connect(self.add_record)
        self.add_btn.setEnabled(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.add_btn)
        
        self.delete_btn = QPushButton("Удалить запись")
        self.delete_btn.clicked.connect(self.delete_record)
        self.delete_btn.setEnabled(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.delete_btn)
        
        toolbar.addWidget(QLabel("Лимит:"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(10, 10000)
        self.limit_spin.setValue(100)
        self.limit_spin.valueChanged.connect(self.load_data)
        toolbar.addWidget(self.limit_spin)
        
        self.save_btn = QPushButton("Сохранить изменения")
        self.save_btn.clicked.connect(self.save_changes)
        self.save_btn.setEnabled(False)
        self.save_btn.setVisible(self.can_write)  # Только если есть права на запись
        toolbar.addWidget(self.save_btn)

        self.rollback_btn = QPushButton("Вернуть изменения")
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
        self.table.cellClicked.connect(self.on_cell_double_clicked)
        layout.addWidget(self.table)

    @staticmethod
    def _guess_media_type(path: str) -> str | None:
        raw = str(path or "").strip().lower()
        if not raw:
            return None
        dot_idx = raw.rfind(".")
        ext = raw[dot_idx:] if dot_idx != -1 else ""
        if ext in TableViewerWidget.IMAGE_EXTENSIONS:
            return "image"
        if ext in TableViewerWidget.AUDIO_EXTENSIONS:
            return "audio"
        return None

    def _is_media_cell(self, table_name: str, column_name: str, value: str) -> bool:
        table = str(table_name or "").lower()
        col = str(column_name or "").lower()
        raw = str(value or "").strip()
        if not raw:
            return False
        if table not in self.MEDIA_COLUMNS:
            return False
        if col not in self.MEDIA_COLUMNS[table]:
            return False
        return raw.startswith(("s3a://", "s3://", "http://", "https://"))

    def _get_media_source_path(self, row: pd.Series) -> str:
        for key in ("source_file", "link"):
            value = row.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    @staticmethod
    def _format_snapshot_time(value) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = text.replace("T", " ")
        if "+" in text:
            text = text.split("+")[0]
        if "." in text:
            text = text.split(".")[0]
        return text

    def _build_download_url(self, source_path: str) -> str | None:
        return self.admin_service.lakehouse.get_presigned_download_url(source_path, download=True)

    def _build_preview_url(self, source_path: str) -> str | None:
        return self.admin_service.lakehouse.get_presigned_download_url(source_path, download=False)

    @staticmethod
    def _make_link_item(text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        font = QFont(item.font())
        font.setUnderline(True)
        item.setFont(font)
        item.setForeground(Qt.GlobalColor.blue)
        item.setToolTip("Двойной клик: открыть файл")
        return item

    def on_cell_double_clicked(self, row: int, column: int):
        if row < 0 or column < 0 or self.df is None:
            return
        if column >= len(self.display_columns):
            return

        col_name = self.display_columns[column]
        if col_name == self.DOWNLOAD_COLUMN_KEY:
            self.download_media(row)
            return

        try:
            value = self.df.iloc[row][col_name]
        except Exception:
            return

        raw = "" if value is None else str(value).strip()
        if not self._is_media_cell(self.table_name, col_name, raw):
            return

        media_type = self._guess_media_type(raw)
        url = self._build_preview_url(raw)
        if not url:
            QMessageBox.warning(self, "Медиа", "Не удалось сформировать ссылку на файл")
            return

        if media_type == "image":
            self.show_image_preview(url, raw)
            return

        if media_type == "audio":
            QDesktopServices.openUrl(QUrl(url))
            self.status_label.setText("Открыта ссылка на аудио")
            return

        QDesktopServices.openUrl(QUrl(url))
        self.status_label.setText("Открыта ссылка на файл")

    def download_media(self, row: int):
        if self.df is None or row < 0 or row >= len(self.df):
            return

        source_path = self._get_media_source_path(self.df.iloc[row])
        if not source_path:
            QMessageBox.warning(self, "Скачивание", "Не удалось определить исходный файл")
            return

        url = self._build_download_url(source_path)
        if not url:
            QMessageBox.warning(self, "Скачивание", "Не удалось сформировать ссылку для скачивания")
            return

        suggested_name = Path(source_path.replace("\\", "/")).name or "downloaded_file"
        target_path, _ = QFileDialog.getSaveFileName(
            self,
            "Скачать файл",
            suggested_name,
            "All Files (*)",
        )
        if not target_path:
            return

        try:
            # Stream download with a reasonable size cap to avoid DoS or disk exhaustion
            max_bytes = 200 * 1024 * 1024  # 200 MB
            total = 0
            with urlopen(url, timeout=60) as resp:
                # Respect Content-Length header when present
                try:
                    content_length = int(resp.getheader("Content-Length") or 0)
                except Exception:
                    content_length = 0
                if content_length and content_length > max_bytes:
                    raise RuntimeError("Файл слишком большой для загрузки")

                with open(target_path, "wb") as out:
                    while True:
                        chunk = resp.read(8192)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise RuntimeError("Файл превышает допустимый размер во время загрузки")
                        out.write(chunk)

            self.status_label.setText(f"Файл скачан: {Path(target_path).name}")
        except Exception as e:
            QMessageBox.warning(self, "Скачивание", f"Не удалось скачать файл:\n{e}")

    def show_image_preview(self, url: str, source_path: str):
        try:
            with urlopen(url, timeout=25) as resp:
                # Quick content-type check to avoid loading non-image data
                content_type = ""
                try:
                    content_type = (resp.getheader("Content-Type") or "").lower()
                except Exception:
                    content_type = ""
                if not content_type.startswith("image/"):
                    raise RuntimeError(f"Ожидалось изображение, получен Content-Type: {content_type}")

                # Limit preview size to avoid excessive memory usage
                max_preview_bytes = 10 * 1024 * 1024  # 10 MB
                data = resp.read(max_preview_bytes + 1)
                if len(data) > max_preview_bytes:
                    raise RuntimeError("Изображение слишком велико для предпросмотра")

            pixmap = QPixmap()
            if not pixmap.loadFromData(data):
                raise RuntimeError("Не удалось декодировать изображение")

            dialog = ImagePreviewDialog(pixmap, f"Предпросмотр: {source_path}", self)
            dialog.exec()
        except Exception as e:
            QMessageBox.warning(
                self,
                "Предпросмотр недоступен",
                f"Не удалось открыть изображение в панели.\nОткроем ссылку в браузере.\n\n{e}",
            )
            QDesktopServices.openUrl(QUrl(url))

    def rollback_changes(self):
        if getattr(self.current_user, "role", None) != UserRole.ADMIN:
            QMessageBox.warning(self, "Недостаточно прав", "Откат доступен только администратору")
            return

        latest_snapshot = self.admin_service.lakehouse.get_latest_snapshot_id(self.table_name)
        previous_snapshot = self.admin_service.lakehouse.get_previous_snapshot_id(self.table_name)
        snapshots = self.admin_service.lakehouse.list_snapshots(self.table_name, limit=30)

        if previous_snapshot is not None:
            snapshot_id = int(previous_snapshot)
            snapshot_labels = {int(sid): str(committed_at) for sid, committed_at, _ in snapshots}
            latest_label = self._format_snapshot_time(snapshot_labels.get(int(latest_snapshot), latest_snapshot))
            target_label = self._format_snapshot_time(snapshot_labels.get(snapshot_id, snapshot_id))
            message_text = (
                f"Откатить таблицу {self.table_name} к предыдущей версии?\n\n"
                f"Текущая версия: {latest_label}\n"
                f"Вернуться к версии: {target_label}"
            )
        else:
            points = self.admin_service.get_table_change_points(self.table_name, limit=30)
            if not points:
                QMessageBox.information(
                    self,
                    "Откат",
                    "Нет доступной предыдущей версии для отката. Сначала сохраните хотя бы одно изменение.",
                )
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
                items.append(f"{p.id} | {ts} | {user} | {action} | {details}")
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
            snapshot_label = self._format_snapshot_time(getattr(point, "executed_at", None) or snapshot_id)
            message_text = f"Откатить таблицу {self.table_name} к версии от {snapshot_label}?"

        reply = QMessageBox.question(
            self,
            "Подтверждение",
            message_text,
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
        self.display_columns = list(self.columns)
        if self.table_name in self.MEDIA_COLUMNS and ("link" in self.columns or "source_file" in self.columns):
            self.display_columns.append(self.DOWNLOAD_COLUMN_KEY)
        self.primary_key = "id" if "id" in self.columns else (self.columns[0] if self.columns else None)

        self.status_label.setText(f"Строк: {len(df)} | {exec_time_ms} мс")

        self.table.blockSignals(True)
        try:
            self.table.clear()
            self.table.setRowCount(len(df))
            self.table.setColumnCount(len(self.display_columns))
            self.table.setHorizontalHeaderLabels([
                "Скачать" if col == self.DOWNLOAD_COLUMN_KEY else col
                for col in self.display_columns
            ])

            for i, row in df.iterrows():
                for j, col_name in enumerate(self.display_columns):
                    if col_name == self.DOWNLOAD_COLUMN_KEY:
                        source_path = self._get_media_source_path(row)
                        item = self._make_link_item("Скачать")
                        item.setData(Qt.ItemDataRole.UserRole, source_path)
                        item.setToolTip("Двойной клик: скачать файл")
                        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    else:
                        value = row[col_name]
                        text_value = "" if value is None else str(value)
                        if self._is_media_cell(self.table_name, col_name, text_value) and col_name == "link":
                            media_type = self._guess_media_type(text_value)
                            label = "Посмотреть"
                            if media_type == "audio":
                                label = "Прослушать"
                            item = self._make_link_item(label)
                            item.setData(Qt.ItemDataRole.UserRole, text_value)
                            item.setToolTip("Двойной клик: открыть файл")
                            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                        else:
                            item = QTableWidgetItem(text_value)
                    self.table.setItem(i, j, item)

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