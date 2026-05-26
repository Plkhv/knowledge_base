#!/usr/bin/env python3
import sys
from PyQt6.QtWidgets import QApplication
from ui.main_window import LakehouseAdminPanel
from config import Config

def main():
    # Run light security validations for secrets/configuration
    try:
        Config.validate_security()
    except Exception:
        pass

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    window = LakehouseAdminPanel()
    window.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()