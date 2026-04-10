from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
    QTableWidgetItem, QPushButton, QLabel, QSpinBox,
    QHeaderView, QMessageBox
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
import pandas as pd

class LoadTableThread(QThread):
    """Поток для загрузки таблицы"""
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)
    
    def __init__(self, service, table_name, limit=100):
        super().__init__()
        self.service = service
        self.table_name = table_name
        self.limit = limit
    
    def run(self):
        try:
            df = self.service.preview_table(self.table_name, limit=self.limit)
            self.finished.emit(df)
        except Exception as e:
            self.error.emit(str(e))

class TableViewerWidget(QWidget):
    """Виджет для просмотра таблицы"""
    
    data_modified = pyqtSignal()
    
    def __init__(self, table_name: str, admin_service, dataframe: pd.DataFrame = None):
        super().__init__()
        self.table_name = table_name
        self.admin_service = admin_service
        self.dataframe = dataframe
        self.setup_ui()
        
        if dataframe is None:
            self.load_data()
        else:
            self.display_data(dataframe)
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Верхняя панель
        toolbar = QHBoxLayout()
        
        self.refresh_btn = QPushButton("Обновить")
        self.refresh_btn.clicked.connect(self.load_data)
        toolbar.addWidget(self.refresh_btn)
        
        toolbar.addWidget(QLabel("Лимит:"))
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(10, 10000)
        self.limit_spin.setValue(100)
        self.limit_spin.valueChanged.connect(self.load_data)
        toolbar.addWidget(self.limit_spin)
        
        toolbar.addStretch()
        
        self.status_label = QLabel("")
        toolbar.addWidget(self.status_label)
        
        layout.addLayout(toolbar)
        
        # Таблица
        self.table = QTableWidget()
        layout.addWidget(self.table)
    
    def load_data(self):
        """Загрузка данных в отдельном потоке"""
        self.status_label.setText("Загрузка...")
        self.refresh_btn.setEnabled(False)
        
        self.loader = LoadTableThread(
            self.admin_service, 
            self.table_name, 
            self.limit_spin.value()
        )
        self.loader.finished.connect(self.display_data)
        self.loader.error.connect(self.show_error)
        self.loader.start()
    
    def display_data(self, df: pd.DataFrame):
        """Отображение данных"""
        self.status_label.setText(f"Загружено строк: {len(df)}")
        self.refresh_btn.setEnabled(True)
        
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns.tolist())
        
        for i, row in df.iterrows():
            for j, value in enumerate(row):
                self.table.setItem(i, j, QTableWidgetItem(str(value)))
        
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    
    def show_error(self, error_msg: str):
        """Показать ошибку"""
        self.status_label.setText("Ошибка загрузки")
        self.refresh_btn.setEnabled(True)
        QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить данные:\n{error_msg}")
    
    def refresh(self):
        """Обновить данные"""
        self.load_data()