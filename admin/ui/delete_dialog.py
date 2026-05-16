from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QComboBox, QPushButton, QMessageBox
)

class DeleteDialog(QDialog):
    def __init__(self, table_name: str, primary_key: str, pk_values: list, admin_service, current_user, parent=None):
        super().__init__(parent)
        self.table_name = table_name
        self.primary_key = primary_key
        self.pk_values = pk_values
        self.admin_service = admin_service
        self.current_user = current_user
        self.setup_ui()
    
    def setup_ui(self):
        self.setWindowTitle(f"Удаление записи из {self.table_name}")
        self.setMinimumWidth(350)
        
        layout = QVBoxLayout(self)
        
        layout.addWidget(QLabel(f"Выберите {self.primary_key} для удаления:"))
        
        self.combo = QComboBox()
        for value in self.pk_values:
            self.combo.addItem(str(value), value)
        layout.addWidget(self.combo)
        
        btn_layout = QHBoxLayout()
        
        self.delete_btn = QPushButton("Удалить")
        self.delete_btn.clicked.connect(self.on_delete)
        btn_layout.addWidget(self.delete_btn)
        
        self.cancel_btn = QPushButton("Отмена")
        self.cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.cancel_btn)
        
        layout.addLayout(btn_layout)
    
    def on_delete(self):
        pk_value = self.combo.currentData()
        if pk_value is None:
            pk_value = self.combo.currentText()
        
        reply = QMessageBox.question(
            self, "Подтверждение",
            f"Удалить запись с {self.primary_key} = {pk_value}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        try:
            success = self.admin_service.delete_row(
                self.table_name,
                self.primary_key,
                pk_value,
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
            QMessageBox.information(self, "Успех", "Запись успешно удалена")
            self.accept()
        else:
            QMessageBox.critical(self, "Ошибка", "Не удалось удалить запись")