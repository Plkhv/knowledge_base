# parsers/gas_analysis_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_gas_measurement_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class GasAnalysisParser(BaseParser):
    """
    Парсер для ручных газовых замеров.
    Поддерживает: замеры метана в выработках.
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    # Порог аномалии для CH4
    CH4_ANOMALY_THRESHOLD = 1.0
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("gas_analysis")
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга газовых замеров"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'gas_measurements', 'gas_analysis', 'замеры', 'отвод газа'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с газовыми замерами.
        """
        records = []
        
        # Извлекаем дату из заголовка
        date_match = re.search(r'Дата:\s*(\d{2}\.\d{2}\.\d{4})', content)
        measurement_date = date_match.group(1) if date_match else None
        
        # Парсим таблицу
        table_records = self._parse_table(content, measurement_date, file_name)
        records.extend(table_records)
        
        return records
    
    def _parse_table(self, content: str, measurement_date: Optional[str], file_name: str) -> List[Dict[str, Any]]:
        """Парсит таблицу с замерами"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Время замера', 'Точка замера', 'CH4']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'время' in header:
                indices['time'] = idx
            elif 'точка' in header or 'место' in header:
                indices['location'] = idx
            elif 'ch4' in header:
                indices['ch4'] = idx
            elif 'скорость' in header or 'воздух' in header:
                indices['velocity'] = idx
            elif 'примечан' in header:
                indices['note'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            if 'Суммарно' in line:
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем время
            time_str = parts[indices['time']] if 'time' in indices and indices['time'] < len(parts) else None
            if not time_str:
                continue
            
            # Формируем полную дату-время
            measurement_dttm = None
            if measurement_date and time_str:
                dt_str = f"{measurement_date} {time_str}"
                measurement_dttm = DateParser.parse_to_str(dt_str, '%Y-%m-%d %H:%M:%S')
            
            # Извлекаем локацию
            location = parts[indices['location']] if 'location' in indices and indices['location'] < len(parts) else None
            
            # Извлекаем CH4
            ch4 = self._to_float(parts[indices['ch4']]) if 'ch4' in indices and indices['ch4'] < len(parts) else None
            
            # Извлекаем скорость воздуха (если есть)
            velocity = None
            if 'velocity' in indices and indices['velocity'] < len(parts):
                velocity = self._to_float(parts[indices['velocity']])
            
            # Извлекаем примечание
            note = parts[indices['note']] if 'note' in indices and indices['note'] < len(parts) else None
            
            # Определяем аномалию
            is_anomaly = 1 if ch4 and ch4 >= self.CH4_ANOMALY_THRESHOLD else 0
            
            # Высота замера (по умолчанию 0 см - у почвы)
            measurement_height_cm = 0.0
            
            record = {
                'measurement_id': generate_gas_measurement_id(),
                'incident_id': self.INCIDENT_ID,
                'location': location,
                'measurement_dttm': measurement_dttm,
                'ch4_percent': ch4,
                'air_velocity_mps': velocity,
                'measurement_height_cm': measurement_height_cm,
                'is_anomaly': is_anomaly,
                'note': note,
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records