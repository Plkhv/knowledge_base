# parsers/graphic_reestr_parser.py
# -*- coding: utf-8 -*-
"""
GraphicReestrParser — регистрирует графические файлы в таблице graphic_reestr.

Логика:
  - Парсер не извлекает данные из содержимого файла (изображения не читаются).
  - Из имени файла извлекается: тип материала, номер схемы / описание.
  - Поле link заполняется DAG-ом через record.setdefault('source_file', s3_path),
    которое затем копируется в link в методе parse().
  - Поддерживаемые форматы: .dwg, .jpg, .jpeg, .png, .pdf, .tif, .tiff, .bmp, .svg

Схема таблицы graphic_reestr:
  incident_id, material_id, name, description, link, inspection_id, source_file
"""

import os
import re
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional

import logging
logger = logging.getLogger(__name__)


# Расширения графических файлов которые обрабатывает этот парсер
GRAPHIC_EXTENSIONS = {'.dwg', '.jpg', '.jpeg', '.png', '.pdf', '.tif', '.tiff', '.bmp', '.svg'}

# Паттерны для извлечения описания из имени файла
# Убираем расширение, дату (YYYY-MM-DD или YYYYMMDD), спецсимволы
_CLEAN_PATTERNS = [
    (r'\d{4}[-_]\d{2}[-_]\d{2}', ''),    # дата YYYY-MM-DD
    (r'\d{8}',                    ''),    # дата YYYYMMDD
    (r'[-_]+',                    ' '),   # дефисы/подчёркивания → пробел
    (r'\s{2,}',                   ' '),   # множественные пробелы
]

# Ключевые слова → тип материала
_TYPE_KEYWORDS = {
    'схема': 'Схема вентиляции',
    'вентил': 'Схема вентиляции',
    'план': 'План горных работ',
    'горн': 'План горных работ',
    'разрез': 'Геологический разрез',
    'геол': 'Геологический разрез',
    'лава': 'Схема лавы',
    'osmotr': 'Схема осмотра',
    'осмотр': 'Схема осмотра',
    'акт': 'Акт осмотра',
    'фото': 'Фотоматериал',
    'foto': 'Фотоматериал',
    'photo': 'Фотоматериал',
    'img': 'Фотоматериал',
    'скважин': 'Схема скважин',
    'дегазац': 'Схема дегазации',
    'транспорт': 'Транспортная схема',
    'сейсм': 'Сейсмограмма',
    'seism': 'Сейсмограмма',
}


def _extract_description(filename_stem: str) -> str:
    """Извлекает читаемое описание из имени файла."""
    text = filename_stem
    for pattern, repl in _CLEAN_PATTERNS:
        text = re.sub(pattern, repl, text, flags=re.IGNORECASE)
    return text.strip().capitalize() or filename_stem


def _infer_type(filename_stem: str) -> str:
    """Определяет тип графического материала по ключевым словам в имени."""
    lower = filename_stem.lower()
    for keyword, mat_type in _TYPE_KEYWORDS.items():
        if keyword in lower:
            return mat_type
    return 'Графический материал'


class GraphicReestrParser:
    """
    Парсер графических файлов.

    Не читает содержимое файла — только регистрирует факт его существования
    в таблице graphic_reestr с метаданными, извлечёнными из имени файла.
    """

    # Парсер не является текстовым — методы parse(content, file_name)
    # получат content=None, читать его не нужно.
    GRAPHIC_ONLY = True

    def __init__(self, incident_id: str):
        self.incident_id = incident_id

    def can_handle(self, file_path: str) -> bool:
        """Возвращает True если файл является графическим."""
        return Path(file_path).suffix.lower() in GRAPHIC_EXTENSIONS

    def parse_file(self, file_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Основной метод — вызывается из DAG напрямую для графических файлов.
        Возвращает dict {table_name: [record]} совместимый с форматом остальных парсеров.
        """
        path       = Path(file_path)
        stem       = path.stem
        ext        = path.suffix.lower().lstrip('.')
        name       = path.name
        description = _extract_description(stem)
        mat_type   = _infer_type(stem)

        record = {
            'incident_id':   self.incident_id,
            'material_id':   str(uuid.uuid4()),
            'name':          name,
            'description':   f"{mat_type}. {description}",
            # link заполнится через record.setdefault('source_file', s3_path) в DAG
            # и затем копируется в link ниже — при вызове parse() мы не знаем s3_path,
            # поэтому link выставляем как None, DAG после setdefault обновит через link_from_source
            'link':          None,
            'inspection_id': None,
            'source_file':   None,   # выставит DAG
        }

        logger.info(f"graphic_reestr: registered {name} as '{mat_type}'")
        return {'graphic_reestr': [record]}

    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Совместимость с интерфейсом BaseParser.
        Графический парсер вызывается через parse_file(), но если фабрика
        вызовет parse() — возвращаем пустой список (контент не читаем).
        """
        return []
