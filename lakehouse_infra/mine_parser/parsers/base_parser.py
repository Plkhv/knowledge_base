# parsers/base_parser.py
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pathlib import Path
import json
import re
import logging

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Базовый абстрактный класс для всех парсеров.
    """

    def __init__(self, incident_id: str | None, config_path: str = "./config"):
        self.config_path = Path(config_path)
        self._table_name: Optional[str] = None
        # ИСПРАВЛЕНИЕ: incident_id передаётся явно из ParserFactory,
        # fallback на конфиг только если не передан
        self.incident_id = incident_id or self._get_default_incident_id()

    def _get_default_incident_id(self) -> str:
        """Загружает ID инцидента из конфига"""
        config = self._load_config_file("incident_config.json")
        return config.get("incident_id", "INC-UNKNOWN")

    def _load_config_file(self, config_file: str) -> Dict:
        """
        Загружает конфигурационный файл из директории config.
        ИСПРАВЛЕНИЕ: убран захардкоженный Windows-путь, используется self.config_path.
        """
        config_full_path = self.config_path / config_file
        if config_full_path.exists():
            try:
                with open(config_full_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Не удалось загрузить конфиг {config_full_path}: {e}")
                return {}
        logger.debug(f"Конфиг не найден: {config_full_path}")
        return {}

    @abstractmethod
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Основной метод парсинга, принимает содержимое файла и имя"""
        pass

    def parse_file(self, file_path: str) -> List[Dict[Any, Any]]:
        """
        Удобный метод для парсинга файла по пути.
        """
        file_path_obj = Path(file_path)
        file_name = file_path_obj.name
        file_ext = file_path_obj.suffix.lower()

        if file_ext in ['.xlsx', '.xls']:
            try:
                import pandas as pd
                df = pd.read_excel(file_path)
                records = df.to_dict('records')
                for record in records:
                    self._add_metadata(record, file_name)
                return records
            except ImportError:
                logger.error("pandas не установлен для чтения Excel файлов")
                return []
            except Exception as e:
                logger.error(f"Ошибка чтения Excel файла {file_path}: {e}")
                return []
        else:
            content = None
            for encoding in ['utf-8', 'cp1251', 'latin-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue

            if content is None:
                logger.error(f"Не удалось прочитать файл {file_path}")
                return []

            return self.parse(content, file_name)

    @abstractmethod
    def supports(self, file_name: str) -> bool:
        """Проверяет, может ли парсер обработать файл с таким именем"""
        pass

    def get_table_name(self) -> Optional[str]:
        return self._table_name

    def set_table_name(self, table_name: str) -> None:
        self._table_name = table_name

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _to_float(self, value) -> Optional[float]:
        """
        ИСПРАВЛЕНИЕ: возвращает None вместо sentinel -1000000000.
        DAG всё равно конвертировал сентинел в NULL, но это ненадёжно
        и ломает числовые проверки в парсере.
        """
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        try:
            result = float(str(value).replace(',', '.').strip())
            return result
        except (ValueError, TypeError):
            return None

    def _to_int(self, value) -> Optional[int]:
        if not value:
            return None
        try:
            return int(re.sub(r'[^\d-]', '', str(value)))
        except (ValueError, TypeError):
            return None

    def _add_metadata(self, record: Any, source_file: str) -> Dict[str, Any]:
        if isinstance(record, dict):
            record['source_document'] = source_file
            record['source_file_name'] = Path(source_file).name
        return record