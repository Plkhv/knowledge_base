# ui/main_window.py
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QPushButton, QTreeWidget, QTreeWidgetItem,
    QSplitter, QTextEdit, QMessageBox, QLabel,
    QHeaderView, QInputDialog, QDialog, QStatusBar, QLineEdit,
    QFormLayout, QDialogButtonBox, QComboBox, QSizePolicy
)
from PyQt6.QtCore import Qt
from services.admin_service import AdminService
from services.lakehouse_service import LakehouseService
from ui.table_viewer import TableViewerWidget
from ui.login_dialog import LoginDialog
from db.models import UserRole
import time

class LakehouseAdminPanel(QMainWindow):
    def __init__(self):
        super().__init__()
        self.admin_service = AdminService()
        self.lakehouse_service = LakehouseService()
        self.current_user = None
        self.setup_ui()
        self.show_login()
    
    def show_login(self):
        dialog = LoginDialog(self.admin_service, self)
        if dialog.exec():
            self.current_user = dialog.user
            self.setup_ui_for_role()
            self.load_tables()
        else:
            self.close()
    
    def setup_ui_for_role(self):
        is_admin = self.current_user.role == UserRole.ADMIN
        self.user_label.setText(f"👤 {self.current_user.full_name} ({self.current_user.role.value})")

        # Raw SQL execution can bypass row-level filters; keep it admin-only.
        if hasattr(self, "sql_btn"):
            self.sql_btn.setEnabled(is_admin)

        if hasattr(self, "users_btn"):
            self.users_btn.setVisible(is_admin)
            self.users_btn.setEnabled(is_admin)
        
        if is_admin:
            self.add_user_management_tab()
        
        role_text = {
            UserRole.ADMIN: "Администратор",
            UserRole.EXPERT: "Эксперт",
            UserRole.VIEWER: "Наблюдатель"
        }.get(self.current_user.role, "Неизвестно")
        
        self.statusBar().showMessage(f"Пользователь: {self.current_user.full_name} ({role_text})", 5000)
    
    def add_user_management_tab(self):
        self.user_management_tab_index = self.tab_widget.addTab(QWidget(), "👥 Пользователи")
        self.load_user_management()
    
    def load_user_management(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        btn_layout = QHBoxLayout()
        add_btn = QPushButton("➕ Добавить пользователя")
        add_btn.clicked.connect(self.add_user)
        refresh_btn = QPushButton("🔄 Обновить")
        refresh_btn.clicked.connect(lambda: self.load_user_management())
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(refresh_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
        self.user_table = QTableWidget()
        self.user_table.setColumnCount(7)
        self.user_table.setHorizontalHeaderLabels(["ID", "Имя пользователя", "ФИО", "Роль", "Активен", "Инциденты", "Действия"])
        self.user_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        users = self.admin_service.get_all_users()
        self.user_table.setRowCount(len(users))
        
        for i, user in enumerate(users):
            self.user_table.setItem(i, 0, QTableWidgetItem(str(user.id)))
            self.user_table.setItem(i, 1, QTableWidgetItem(user.username))
            self.user_table.setItem(i, 2, QTableWidgetItem(user.full_name or ""))
            self.user_table.setItem(i, 3, QTableWidgetItem(user.role.value))
            self.user_table.setItem(i, 4, QTableWidgetItem("Да" if user.is_active else "Нет"))
            self.user_table.setItem(i, 5, QTableWidgetItem(getattr(user, "allowed_incident_ids", "") or ""))
            
            actions_widget = QWidget()
            actions_layout = QHBoxLayout(actions_widget)
            actions_layout.setContentsMargins(0, 0, 0, 0)
            
            edit_btn = QPushButton("✏️")
            edit_btn.setFixedWidth(30)
            edit_btn.clicked.connect(lambda checked, u=user: self.edit_user(u))
            
            reset_btn = QPushButton("🔑")
            reset_btn.setFixedWidth(30)
            reset_btn.clicked.connect(lambda checked, u=user: self.reset_password(u))
            
            delete_btn = QPushButton("🗑️")
            delete_btn.setFixedWidth(30)
            delete_btn.clicked.connect(lambda checked, u=user: self.delete_user(u))
            
            actions_layout.addWidget(edit_btn)
            actions_layout.addWidget(reset_btn)
            if user.username != 'admin':
                actions_layout.addWidget(delete_btn)
            
            self.user_table.setCellWidget(i, 6, actions_widget)
        
        layout.addWidget(self.user_table)
        
        if hasattr(self, 'user_management_tab_index'):
            self.tab_widget.widget(self.user_management_tab_index).deleteLater()
            self.tab_widget.insertTab(self.user_management_tab_index, widget, "👥 Пользователи")
            self.tab_widget.setCurrentIndex(self.user_management_tab_index)
    
    def add_user(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Добавление пользователя")
        dialog.setMinimumWidth(400)
        
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        
        username_edit = QLineEdit()
        fullname_edit = QLineEdit()
        password_edit = QLineEdit()
        password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        confirm_edit = QLineEdit()
        confirm_edit.setEchoMode(QLineEdit.EchoMode.Password)
        role_combo = QComboBox()
        role_combo.addItems([UserRole.EXPERT.value, UserRole.VIEWER.value])

        incidents_edit = QLineEdit()
        incidents_edit.setPlaceholderText("Напр.: INC-001,INC-002")
        
        form.addRow("Имя пользователя:", username_edit)
        form.addRow("ФИО:", fullname_edit)
        form.addRow("Пароль:", password_edit)
        form.addRow("Подтверждение:", confirm_edit)
        form.addRow("Роль:", role_combo)
        form.addRow("Инциденты (ID через запятую):", incidents_edit)
        layout.addLayout(form)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(lambda: self._on_add_user(
            username_edit.text(), fullname_edit.text(), 
            password_edit.text(), confirm_edit.text(), 
            role_combo.currentText(), incidents_edit.text(), dialog
        ))
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        dialog.exec()
    
    def _on_add_user(self, username, fullname, password, confirm, role, incidents, dialog):
        if not username or not password:
            QMessageBox.warning(self, "Ошибка", "Имя пользователя и пароль обязательны")
            return
        if password != confirm:
            QMessageBox.warning(self, "Ошибка", "Пароли не совпадают")
            return
        
        success, result = self.admin_service.create_user(
            username, password, fullname, role, self.current_user.id, allowed_incident_ids=incidents
        )
        if success:
            QMessageBox.information(self, "Успех", f"Пользователь {username} добавлен")
            dialog.accept()
            self.load_user_management()
        else:
            QMessageBox.warning(self, "Ошибка", result)
    
    def edit_user(self, user):
        dialog = QDialog(self)
        dialog.setWindowTitle(f"Редактирование: {user.username}")
        dialog.setMinimumWidth(400)
        
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        
        fullname_edit = QLineEdit(user.full_name or "")
        role_combo = QComboBox()
        role_combo.addItems([UserRole.EXPERT.value, UserRole.VIEWER.value])
        role_combo.setCurrentText(user.role.value)

        incidents_edit = QLineEdit(getattr(user, "allowed_incident_ids", "") or "")
        incidents_edit.setPlaceholderText("Напр.: INC-001,INC-002")
        active_check = QComboBox()
        active_check.addItems(["Да", "Нет"])
        active_check.setCurrentText("Да" if user.is_active else "Нет")
        
        form.addRow("ФИО:", fullname_edit)
        form.addRow("Роль:", role_combo)
        form.addRow("Активен:", active_check)
        form.addRow("Инциденты (ID через запятую):", incidents_edit)
        layout.addLayout(form)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(lambda: self._on_edit_user(
            user.id, fullname_edit.text(), role_combo.currentText(), 
            active_check.currentText() == "Да", incidents_edit.text(), dialog
        ))
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        dialog.exec()
    
    def _on_edit_user(self, user_id, fullname, role, is_active, incidents, dialog):
        success, result = self.admin_service.update_user(
            user_id, full_name=fullname, role=role, is_active=is_active, allowed_incident_ids=incidents
        )
        if success:
            QMessageBox.information(self, "Успех", "Данные пользователя обновлены")
            dialog.accept()
            self.load_user_management()
        else:
            QMessageBox.warning(self, "Ошибка", result)
    
    def reset_password(self, user):
        password, ok = QInputDialog.getText(
            self, "Сброс пароля", 
            f"Введите новый пароль для {user.username}:",
            QLineEdit.EchoMode.Password
        )
        if ok and password:
            confirm, ok2 = QInputDialog.getText(
                self, "Подтверждение", "Повторите пароль:",
                QLineEdit.EchoMode.Password
            )
            if ok2 and password == confirm:
                success, result = self.admin_service.update_user(user.id, password=password)
                if success:
                    QMessageBox.information(self, "Успех", "Пароль изменён")
                else:
                    QMessageBox.warning(self, "Ошибка", result)
            else:
                QMessageBox.warning(self, "Ошибка", "Пароли не совпадают")
    
    def delete_user(self, user):
        reply = QMessageBox.question(
            self, "Подтверждение",
            f"Удалить пользователя {user.username}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            success, result = self.admin_service.delete_user(user.id)
            if success:
                QMessageBox.information(self, "Успех", result)
                self.load_user_management()
            else:
                QMessageBox.warning(self, "Ошибка", result)
    
    def setup_ui(self):
        self.setWindowTitle("Lakehouse Admin Panel")
        self.setGeometry(100, 100, 1400, 800)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(4, 4, 4, 4)
        
        # Верхняя панель (фиксируем высоту, чтобы не съедала место у таблиц)
        self.top_panel = QWidget()
        self.top_panel.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        top_layout = QVBoxLayout(self.top_panel)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(4)

        toolbar_layout = QHBoxLayout()
        toolbar_layout.setContentsMargins(0, 0, 0, 0)
        toolbar_layout.setSpacing(6)
        
        self.refresh_btn = QPushButton("🔄 Обновить таблицы")
        self.refresh_btn.clicked.connect(self.load_tables)
        toolbar_layout.addWidget(self.refresh_btn)

        self.users_btn = QPushButton("👥 Пользователи")
        self.users_btn.clicked.connect(self.open_user_management)
        self.users_btn.setVisible(False)
        toolbar_layout.addWidget(self.users_btn)
        
        self.sql_btn = QPushButton("📝 Выполнить SQL")
        self.sql_btn.clicked.connect(self.show_sql_dialog)
        toolbar_layout.addWidget(self.sql_btn)
        
        self.history_btn = QPushButton("📜 История запросов")
        self.history_btn.clicked.connect(self.show_history)
        toolbar_layout.addWidget(self.history_btn)
        
        self.add_metadata_btn = QPushButton("➕ Добавить метаданные")
        self.add_metadata_btn.clicked.connect(self.add_table_metadata)
        toolbar_layout.addWidget(self.add_metadata_btn)
        
        self.logout_btn = QPushButton("🚪 Выйти")
        self.logout_btn.clicked.connect(self.logout)
        toolbar_layout.addWidget(self.logout_btn)
        
        toolbar_layout.addStretch()
        
        self.user_label = QLabel()
        toolbar_layout.addWidget(self.user_label)

        top_layout.addLayout(toolbar_layout)

        # Разделитель
        line = QWidget()
        line.setFixedHeight(1)
        line.setStyleSheet("background-color: #d0d0d0;")
        top_layout.addWidget(line)

        main_layout.addWidget(self.top_panel)
        
        # Основная область
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Левая панель
        left_panel = QWidget()
        left_panel.setFixedWidth(250)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        tree_header = QLabel("📁 Таблицы Lakehouse")
        tree_header.setFixedHeight(32)
        tree_header.setStyleSheet("font-weight: bold; padding: 8px;")
        left_layout.addWidget(tree_header)
        
        self.tree = QTreeWidget()
        self.tree.setHeaderLabel("Таблицы")
        self.tree.itemDoubleClicked.connect(self.on_table_clicked)
        left_layout.addWidget(self.tree)
        
        splitter.addWidget(left_panel)
        
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        splitter.addWidget(self.tab_widget)
        
        splitter.setSizes([250, 1100])
        main_layout.addWidget(splitter, 1)

        self._update_top_panel_height()
        
        # Нижняя строка статуса
        status_bar = QStatusBar()
        status_bar.setFixedHeight(24)
        self.setStatusBar(status_bar)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._update_top_panel_height()

    def _update_top_panel_height(self):
        # Keep the buttons area compact; cap at ~20% of the window height.
        # This avoids situations where the top layout eats too much vertical space.
        if not hasattr(self, "top_panel"):
            return
        max_h = max(64, int(self.height() * 0.2))
        self.top_panel.setMaximumHeight(max_h)

    def open_user_management(self):
        if not self.current_user or self.current_user.role != UserRole.ADMIN:
            QMessageBox.warning(self, "Недостаточно прав", "Доступно только администратору")
            return

        # Tab can be closed by the user; recover it by searching by title.
        target_title = "👥 Пользователи"
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == target_title:
                self.user_management_tab_index = i
                self.tab_widget.setCurrentIndex(i)
                self.load_user_management()
                return

        self.add_user_management_tab()
        self.tab_widget.setCurrentIndex(self.user_management_tab_index)
    
    def load_tables(self):
        if not self.current_user:
            return
        
        try:
            tables = self.lakehouse_service.list_tables()
            self.tree.clear()
            
            for table in tables:
                item = QTreeWidgetItem([table])
                self.tree.addTopLevelItem(item)
            
            self.statusBar().showMessage(f"Загружено {len(tables)} таблиц", 2000)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))
    
    def on_table_clicked(self, item, column):
        table_name = item.text(0)
        self.open_table_in_tab(table_name)
    
    def open_table_in_tab(self, table_name: str):
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == table_name:
                self.tab_widget.setCurrentIndex(i)
                return
        
        viewer = TableViewerWidget(table_name, self.admin_service, self.current_user)
        self.tab_widget.addTab(viewer, table_name)
        self.tab_widget.setCurrentWidget(viewer)
    
    def refresh_table_tab(self, table_name: str):
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == table_name:
                widget = self.tab_widget.widget(i)
                if hasattr(widget, 'refresh'):
                    widget.refresh()
                break
    
    def close_tab(self, index):
        self.tab_widget.removeTab(index)
    
    def show_sql_dialog(self):
        dialog = QDialog(self)
        dialog.setWindowTitle("Выполнить SQL запрос")
        dialog.setMinimumSize(600, 400)
        
        layout = QVBoxLayout(dialog)
        
        layout.addWidget(QLabel("SQL запрос:"))
        sql_edit = QTextEdit()
        sql_edit.setPlaceholderText("SELECT * FROM incidents LIMIT 10")
        layout.addWidget(sql_edit)
        
        btn_layout = QHBoxLayout()
        execute_btn = QPushButton("▶️ Выполнить")
        cancel_btn = QPushButton("Отмена")
        btn_layout.addWidget(execute_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
        
        def on_execute():
            query = sql_edit.toPlainText()
            if not query.strip():
                return
            
            try:
                start_time = time.time()
                df = self.lakehouse_service.execute_query(query)
                exec_time = int((time.time() - start_time) * 1000)
                
                self.admin_service.log_query(
                    user_id=self.current_user.id,
                    username=self.current_user.username,
                    query_text=query,
                    table_name="custom",
                    execution_time_ms=exec_time,
                    row_count=len(df),
                    status="success"
                )
                
                from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
                
                result_widget = QWidget()
                result_layout = QVBoxLayout(result_widget)
                
                table = QTableWidget()
                table.setRowCount(len(df))
                table.setColumnCount(len(df.columns))
                table.setHorizontalHeaderLabels(df.columns.tolist())
                
                for i, row in df.iterrows():
                    for j, value in enumerate(row):
                        table.setItem(i, j, QTableWidgetItem(str(value)))
                
                result_layout.addWidget(table)
                self.tab_widget.addTab(result_widget, "SQL Result")
                self.tab_widget.setCurrentWidget(result_widget)
                
                dialog.accept()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))
                self.admin_service.log_query(
                    user_id=self.current_user.id,
                    username=self.current_user.username,
                    query_text=query,
                    table_name="custom",
                    execution_time_ms=0,
                    row_count=0,
                    status="error",
                    error_message=str(e)
                )
        
        execute_btn.clicked.connect(on_execute)
        cancel_btn.clicked.connect(dialog.reject)
        
        dialog.exec()
    
    def show_history(self):
        history = self.admin_service.get_query_history(user_id=self.current_user.id)
        
        if not history:
            QMessageBox.information(self, "История", "История запросов пуста")
            return
        
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
        table = QTableWidget()
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(["Время", "Таблица", "Строк", "Время (мс)", "Статус"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        table.setRowCount(len(history))
        for i, h in enumerate(history):
            table.setItem(i, 0, QTableWidgetItem(str(h.executed_at)[:19]))
            table.setItem(i, 1, QTableWidgetItem(h.table_name or ""))
            table.setItem(i, 2, QTableWidgetItem(str(h.row_count or 0)))
            table.setItem(i, 3, QTableWidgetItem(str(h.execution_time_ms or 0)))
            table.setItem(i, 4, QTableWidgetItem(h.status or ""))
        
        layout.addWidget(table)
        self.tab_widget.addTab(widget, "История запросов")
        self.tab_widget.setCurrentWidget(widget)
    
    def add_table_metadata(self):
        table_name, ok = QInputDialog.getText(self, "Добавить метаданные", "Имя таблицы:")
        if not ok or not table_name:
            return
        
        description, ok = QInputDialog.getText(self, "Добавить метаданные", "Описание таблицы:")
        if not ok:
            description = ""
        
        success, result = self.admin_service.add_table_metadata(table_name, description)
        if success:
            QMessageBox.information(self, "Успех", f"Метаданные для таблицы '{table_name}' добавлены")
        else:
            QMessageBox.warning(self, "Ошибка", result)
    
    def logout(self):
        reply = QMessageBox.question(self, "Выход", "Вы уверены, что хотите выйти?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.current_user = None
            self.show_login()
            self.load_tables()
            # Очищаем вкладки
            while self.tab_widget.count() > 0:
                self.tab_widget.removeTab(0)