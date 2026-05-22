from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, 
    QPushButton, QDialogButtonBox, QMessageBox
)
from PyQt6.QtCore import Qt

class AddRecordDialog(QDialog):
    def __init__(self, table_name: str, columns: list, admin_service, current_user, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.columns = columns
        self.admin_service = admin_service
        self.current_user = current_user
        self.inputs = {}
        self.setup_ui()
    
    def setup_ui(self):
        self.setWindowTitle(f"Добавление записи в {self.table_name}")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayout(self)
        
        form_layout = QFormLayout()
        
        # Исключаем автогенерируемые поля
        skip_fields = ['id', 'created_at', 'updated_at']
        
        for col in self.columns:
            if col.lower() in skip_fields:
                continue
            
            line_edit = QLineEdit()
            line_edit.setPlaceholderText(f"Введите значение для {col}")
            self.inputs[col] = line_edit
            form_layout.addRow(f"{col}:", line_edit)
        
        layout.addLayout(form_layout)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.on_submit)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
    
    def on_submit(self):
        values = {}
        for col, widget in self.inputs.items():
            text = widget.text().strip()
            if not text:
                QMessageBox.warning(self, "Внимание", f"Поле {col} не может быть пустым")
                return
            values[col] = text

        try:
            success = self.admin_service.insert_row(
                self.table_name,
                values,
                user_role=self.current_user.role,
                current_user=self.current_user,
            )
        except PermissionError as e:
            QMessageBox.critical(self, "Недостаточно прав", str(e))
            return
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
            return

        if success:
            QMessageBox.information(self, "Успех", "Запись успешно добавлена")
            self.accept()
        else:
            QMessageBox.critical(self, "Ошибка", "Не удалось добавить запись")