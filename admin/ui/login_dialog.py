# ui/login_dialog.py
from PyQt6.QtWidgets import QDialog, QVBoxLayout, QFormLayout, QLineEdit, QPushButton, QMessageBox, QLabel
from PyQt6.QtCore import Qt

class LoginDialog(QDialog):
    def __init__(self, admin_service, parent=None):
        super().__init__(parent)
        self.admin_service = admin_service
        self.user = None
        self.setup_ui()
        print("LoginDialog: инициализация завершена")
    
    def setup_ui(self):
        self.setWindowTitle("Авторизация")
        self.setFixedSize(350, 200)
        # Важно: не затираем базовые флаги (Dialog/Window), иначе окно может не отображаться.
        self.setWindowFlags(self.windowFlags() | Qt.WindowType.WindowCloseButtonHint)
        
        layout = QVBoxLayout(self)
        
        form = QFormLayout()
        
        self.username_edit = QLineEdit()
        self.username_edit.setPlaceholderText("Введите имя пользователя")
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_edit.setPlaceholderText("Введите пароль")
        
        form.addRow("Имя пользователя:", self.username_edit)
        form.addRow("Пароль:", self.password_edit)
        layout.addLayout(form)
        
        self.login_btn = QPushButton("Войти")
        self.login_btn.clicked.connect(self.authenticate)
        layout.addWidget(self.login_btn)
        
        # Подсказка для первого входа
        hint = QLabel("Подсказка: admin / admin123")
        hint.setStyleSheet("color: gray; font-size: 10px;")
        hint.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(hint)
    
    def authenticate(self):
        username = self.username_edit.text().strip()
        password = self.password_edit.text()
        
        if not username or not password:
            QMessageBox.warning(self, "Ошибка", "Введите имя пользователя и пароль")
            return
        
        user = self.admin_service.authenticate(username, password)
        if user:
            self.user = user
            self.accept()
        else:
            QMessageBox.critical(self, "Ошибка", "Неверное имя пользователя или пароль")