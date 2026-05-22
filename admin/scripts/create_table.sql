#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Точка входа для загрузки реальных таблиц в Lakehouse.

Этот файл оставлен с исходным именем `create_table.sql`, потому что его уже
запускали как `python create_table.sql`.

Фактическая логика находится в `lakehouse_model_loader.py`.
"""

from pathlib import Path
import sys


# Безопасный импорт: `admin/` не является python-пакетом.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_model_loader import main


if __name__ == "__main__":
    raise SystemExit(main())
