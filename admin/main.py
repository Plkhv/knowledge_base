#!/usr/bin/env python3
import sys
from PyQt6.QtWidgets import QApplication
from ui.main_window import LakehouseAdminPanel

def main():
    print("1. Запуск приложения...")
    app = QApplication(sys.argv)
    print("2. QApplication создан")
    app.setStyle("Fusion")
    
    print("3. Создание окна...")
    window = LakehouseAdminPanel()
    print("4. Окно создано")
    window.show()
    print("5. Окно показано")
    
    sys.exit(app.exec())

if __name__ == "__main__":
    main()