# parsers/graphic_reestr_parser.py
# -*- coding: utf-8 -*-
"""
MediaReestrParser — регистрирует графические файлы в таблице graphic_reestr.

Не читает содержимое файла — формирует запись реестра на основе имени файла:
  - material_id  : UUID
  - name         : оригинальное имя файла
  - description  : тип материала + читаемое описание из имени
  - link         : S3-путь (заполняется DAG-ом через record['link'] = s3_path)
  - source_file  : S3-путь (заполняется DAG-ом через setdefault)

"""

import re
import uuid
import logging
from pathlib import Path
from typing import List, Dict, Any

from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)

# Расширения которые обрабатывает этот парсер
MEDIA_EXTENSIONS = {
    # Графика
    '.dwg', '.jpg', '.jpeg', '.png',
    '.pdf', '.tif', '.tiff', '.bmp', '.svg',
    # Аудио
    '.mp3', '.wav', '.ogg', '.m4a', 
    '.flac', '.aac', '.wma', '.opus', 
    '.amr', '.aiff',
}

# Очистка имени файла: даты, спецсимволы → пробелы
_CLEAN_PATTERNS = [
    (r'\d{4}[-_]\d{2}[-_]\d{2}', ''),   # YYYY-MM-DD
    (r'\d{8}',                    ''),   # YYYYMMDD
    (r'[-_]+',                    ' '),  # дефис/подчёркивание → пробел
    (r'\s{2,}',                   ' '),  # лишние пробелы
]

# Ключевые слова → тип материала
_TYPE_KEYWORDS = {
    'схем':       'Схема',
    'вентил':     'Схема вентиляции',
    'план':       'План горных работ',
    'горн':       'План горных работ',
    'разрез':     'Геологический разрез',
    'геол':       'Геологический разрез',
    'лава':       'Схема лавы',
    'осмотр':     'Схема осмотра',
    'акт':        'Акт осмотра',
    'фото':       'Фотоматериал',
    'foto':       'Фотоматериал',
    'photo':      'Фотоматериал',
    'img':        'Фотоматериал',
    'скважин':    'Схема скважин',
    'дегазац':    'Схема дегазации',
    'транспорт':  'Транспортная схема',
    'сейсм':      'Сейсмограмма',
    'seism':      'Сейсмограмма',
    'карт':       'Карта',
    'map':        'Карта',
    'бассейн':    'Карта бассейна',
}


def _extract_description(filename_stem: str) -> str:
    """Читаемое описание из имени файла — убирает даты и спецсимволы."""
    text = filename_stem
    for pattern, repl in _CLEAN_PATTERNS:
        text = re.sub(pattern, repl, text, flags=re.IGNORECASE)
    return text.strip().capitalize() or filename_stem


def _infer_type(filename_stem: str) -> str:
    """Тип материала по ключевым словам в имени файла."""
    lower = filename_stem.lower()
    for keyword, mat_type in _TYPE_KEYWORDS.items():
        if keyword in lower:
            return mat_type
    return 'Графический материал'


class MediaReestrParser(BaseParser):
    """
    Парсер графических файлов.

    Не читает содержимое — только регистрирует файл в graphic_reestr.
    Вызывается из ParserFactory.parse_file() напрямую через parse_file(path),
    минуя чтение содержимого файла.
    """

    def __init__(self, incident_id: str):
        super().__init__(incident_id)

    def supports(self, file_name: str) -> bool:
            """Проверяет, поддерживает ли парсер данный файл по расширению"""
            ext = Path(file_name).suffix.lower()
            return ext in MEDIA_EXTENSIONS

    def parse_file(self, file_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Основной метод — возвращает {table: [record]}.
        Поля link и source_file выставляются DAG-ом после загрузки в MinIO.
        """
        path       = Path(file_path)
        stem       = path.stem
        name       = path.name
        mat_type   = _infer_type(stem)
        description = _extract_description(stem)

        record = {
            'incident_id':   self.incident_id,
            'material_id':   str(uuid.uuid4()),
            'name':          name,
            'description':   f"{mat_type}. {description}",
            'link':          None,        # DAG заполнит: record['link'] = s3_path
            'inspection_id': None,
            'source_file':   None,        # DAG заполнит: setdefault('source_file', s3_path)
        }

        logger.info(f"graphic_reestr: registered '{name}' as '{mat_type}'")
        return {'graphic_reestr': [record]}

    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Совместимость с BaseParser.parse() — графика не парсится как текст.
        Реальная работа идёт через parse_file().
        """
        return []