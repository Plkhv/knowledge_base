from PyQt6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QPushButton

class DataEditorWidget(QWidget):
    """Виджет для редактирования данных (заглушка)"""
    
    def __init__(self):
        super().__init__()
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        toolbar = QHBoxLayout()
        
        self.save_btn = QPushButton("Сохранить")
        self.cancel_btn = QPushButton("Отмена")
        
        toolbar.addWidget(self.save_btn)
        toolbar.addWidget(self.cancel_btn)
        toolbar.addStretch()
        
        layout.addLayout(toolbar)
        
        self.table = QTableWidget()
        layout.addWidget(self.table)