#!/usr/bin/env python3
import sys
from PyQt6.QtWidgets import QApplication
from ui.main_window import LakehouseAdminPanel

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    window = LakehouseAdminPanel()
    window.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()