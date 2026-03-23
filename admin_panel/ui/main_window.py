import sys
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QTabWidget, QPushButton, QTableWidget, QTableWidgetItem,
    QTreeWidget, QTreeWidgetItem, QSplitter, QTextEdit,
    QMessageBox, QLineEdit, QLabel, QHeaderView, QInputDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QAction

from services.admin_service import AdminService
from ui.table_viewer import TableViewerWidget
from ui.data_editor import DataEditorWidget
import pandas as pd

class LakehouseAdminPanel(QMainWindow):
    """Главное окно админ-панели"""
    
    def __init__(self):
        super().__init__()
        self.admin_service = AdminService()
        self.setup_ui()
        self.load_tables()
    
    def setup_ui(self):
        self.setWindowTitle("Lakehouse Admin Panel")
        self.setGeometry(100, 100, 1400, 800)
        
        # Центральный виджет
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Основной layout
        main_layout = QVBoxLayout(central_widget)
        
        # Верхняя панель с кнопками
        toolbar_layout = QHBoxLayout()
        
        self.refresh_btn = QPushButton("Обновить таблицы")
        self.refresh_btn.clicked.connect(self.load_tables)
        toolbar_layout.addWidget(self.refresh_btn)
        
        self.sql_btn = QPushButton("Выполнить SQL")
        self.sql_btn.clicked.connect(self.show_sql_dialog)
        toolbar_layout.addWidget(self.sql_btn)
        
        self.history_btn = QPushButton("История запросов")
        self.history_btn.clicked.connect(self.show_history)
        toolbar_layout.addWidget(self.history_btn)
        
        self.add_metadata_btn = QPushButton("Добавить метаданные")
        self.add_metadata_btn.clicked.connect(self.add_table_metadata)
        toolbar_layout.addWidget(self.add_metadata_btn)
        
        toolbar_layout.addStretch()
        
        # Статус
        self.status_label = QLabel("Готов")
        toolbar_layout.addWidget(self.status_label)
        
        main_layout.addLayout(toolbar_layout)
        
        # Разделитель (дерево таблиц + содержимое)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Левая панель: дерево таблиц
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.tree = QTreeWidget()
        self.tree.setHeaderLabel("Таблицы Lakehouse")
        self.tree.itemDoubleClicked.connect(self.on_table_clicked)
        left_layout.addWidget(self.tree)
        
        splitter.addWidget(left_panel)
        
        # Правая панель: вкладки с таблицами
        self.tab_widget = QTabWidget()
        splitter.addWidget(self.tab_widget)
        
        splitter.setSizes([300, 1100])
        main_layout.addWidget(splitter)
    
    def load_tables(self):
        """Загрузка списка таблиц из Lakehouse"""
        self.status_label.setText("Загрузка таблиц...")
        try:
            tables = self.admin_service.get_tables_from_lakehouse()
            self.tree.clear()
            
            # Группировка по схемам (упрощённо)
            for table in tables:
                item = QTreeWidgetItem([table])
                self.tree.addTopLevelItem(item)
            
            self.status_label.setText(f"Загружено {len(tables)} таблиц")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить таблицы:\n{str(e)}")
            self.status_label.setText("Ошибка загрузки")
    
    def on_table_clicked(self, item, column):
        """Обработчик клика по таблице"""
        table_name = item.text(0)
        self.open_table_in_tab(table_name)
    
    def open_table_in_tab(self, table_name: str):
        """Открыть таблицу в новой вкладке"""
        # Проверяем, не открыта ли уже
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == table_name:
                self.tab_widget.setCurrentIndex(i)
                return
        
        # Создаём виджет для просмотра таблицы
        viewer = TableViewerWidget(table_name, self.admin_service)
        viewer.data_modified.connect(lambda: self.refresh_table_tab(table_name))
        
        self.tab_widget.addTab(viewer, table_name)
        self.tab_widget.setCurrentWidget(viewer)
    
    def refresh_table_tab(self, table_name: str):
        """Обновить вкладку с таблицей"""
        for i in range(self.tab_widget.count()):
            if self.tab_widget.tabText(i) == table_name:
                widget = self.tab_widget.widget(i)
                if hasattr(widget, 'refresh'):
                    widget.refresh()
                break
    
    def show_sql_dialog(self):
        """Диалог для выполнения произвольного SQL"""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QDialogButtonBox
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Выполнить SQL запрос")
        dialog.setMinimumSize(600, 400)
        
        layout = QVBoxLayout(dialog)
        
        sql_edit = QTextEdit()
        sql_edit.setPlaceholderText("Введите SQL запрос...\nПример: SELECT * FROM table_name LIMIT 10")
        layout.addWidget(sql_edit)
        
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(lambda: self.execute_sql(sql_edit.toPlainText(), dialog))
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)
        
        dialog.exec()
    
    def execute_sql(self, query: str, dialog):
        """Выполнение SQL запроса"""
        if not query.strip():
            return
        
        try:
            df = self.admin_service.execute_custom_query(query)
            
            # Открываем результат в новой вкладке
            result_widget = TableViewerWidget("Результат запроса", self.admin_service, df)
            self.tab_widget.addTab(result_widget, "SQL Result")
            self.tab_widget.setCurrentWidget(result_widget)
            
            dialog.accept()
            self.status_label.setText(f"Запрос выполнен, строк: {len(df)}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка запроса", str(e))
    
    def show_history(self):
        """Показать историю запросов"""
        history = self.admin_service.get_query_history()
        
        if not history:
            QMessageBox.information(self, "История", "История запросов пуста")
            return
        
        # Открываем историю в новой вкладке
        self.show_history_window(history)
    
    def show_history_window(self, history):
        """Окно с историей запросов"""
        from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem, QHeaderView
        
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
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
        """Добавить метаданные для таблицы"""
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