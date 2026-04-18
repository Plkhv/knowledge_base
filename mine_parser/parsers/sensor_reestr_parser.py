# parsers/sensor_reestr_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_sensor_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class SensorReestrParser(BaseParser):
    """
    Парсер для реестра датчиков (sensor_reestr).
    Поддерживает: проекты АСКА, журналы поверки, отчеты о работе.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("sensor_reestr")
        # Хранилище для объединения данных из разных файлов
        self.sensors_cache = {}
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга реестра датчиков"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'sensor', 'датчик', 'аска', 'sensor_reestr', 'калибровк', 'поверк'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с данными о датчиках.
        Объединяет информацию из разных источников по sensor_id.
        """
        file_lower = file_name.lower()
        
        if 'аска' in file_lower or 'проект' in file_lower:
            records = self._parse_aska_project(content, file_name)
        elif 'калибровк' in file_lower or 'поверк' in file_lower:
            records = self._parse_calibration(content, file_name)
        elif 'отчет' in file_lower or 'incident' in file_lower:
            records = self._parse_incident_report(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Объединяем с кэшем (если данные из разных файлов)
        for record in records:
            sensor_id = record.get('sensor_id')
            if sensor_id and sensor_id in self.sensors_cache:
                # Обновляем существующую запись
                self.sensors_cache[sensor_id].update(record)
            elif sensor_id:
                self.sensors_cache[sensor_id] = record
        
        return records
    
    # parsers/sensor_reestr_parser.py - добавьте этот метод

    def _parse_aska_project(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит проект АСКА (список датчиков с локациями)"""
        records = []
        lines = content.split('\n')
        
        for line in lines:
            # Ищем строки вида: "1. Датчик CH4/CO - М37(2) - исходящая струя лавы (ВШ 48К3-з)"
            pattern = r'Датчик\s+([A-Z0-9/]+)\s*-\s*([A-Z0-9\(\)]+)\s*-\s*(.+)'
            match = re.search(pattern, line, re.IGNORECASE)
            
            if match:
                sensor_type_raw = match.group(1).strip()
                sensor_id = match.group(2).strip()
                location = match.group(3).strip()
                
                sensor_type = self._normalize_sensor_type(sensor_type_raw)
                unit = self._get_unit_by_type(sensor_type)
                
                record = {
                    'sensor_id': sensor_id,
                    'sensor_type': sensor_type,
                    'model': None,
                    'location': location,
                    'measurement_range': None,
                    'unit': unit,
                    'last_calibration_date': None,
                    'battery_life_hours': None,
                    'actual_battery_time_at_incident': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records
    
    def _parse_calibration(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит журнал поверки датчиков (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Sensor', 'Тип', 'Модель', 'Место']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'sensor' in header or 'id' in header:
                indices['sensor_id'] = idx
            elif 'тип' in header:
                indices['sensor_type'] = idx
            elif 'модель' in header:
                indices['model'] = idx
            elif 'место' in header or 'установк' in header:
                indices['location'] = idx
            elif 'диапазон' in header:
                indices['range'] = idx
            elif 'ед' in header or 'изм' in header:
                indices['unit'] = idx
            elif 'дата' in header and ('поверк' in header or 'калибр' in header):
                indices['calibration_date'] = idx
            elif 'батаре' in header or 'чаc' in header:
                indices['battery'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            record = {}
            
            # sensor_id
            if 'sensor_id' in indices and indices['sensor_id'] < len(parts):
                record['sensor_id'] = parts[indices['sensor_id']]
            
            # sensor_type
            if 'sensor_type' in indices and indices['sensor_type'] < len(parts):
                raw_type = parts[indices['sensor_type']]
                record['sensor_type'] = self._normalize_sensor_type(raw_type)
            
            # model
            if 'model' in indices and indices['model'] < len(parts):
                record['model'] = parts[indices['model']]
            
            # location
            if 'location' in indices and indices['location'] < len(parts):
                record['location'] = parts[indices['location']]
            
            # measurement_range
            if 'range' in indices and indices['range'] < len(parts):
                record['measurement_range'] = parts[indices['range']]
            
            # unit
            if 'unit' in indices and indices['unit'] < len(parts):
                record['unit'] = parts[indices['unit']]
            elif record.get('sensor_type'):
                record['unit'] = self._get_unit_by_type(record['sensor_type'])
            
            # last_calibration_date
            if 'calibration_date' in indices and indices['calibration_date'] < len(parts):
                date_str = parts[indices['calibration_date']]
                record['last_calibration_date'] = DateParser.parse_to_str(date_str)
            
            # battery_life_hours
            if 'battery' in indices and indices['battery'] < len(parts):
                record['battery_life_hours'] = self._to_int(parts[indices['battery']])
            
            record['_source_file'] = file_name
            
            if record.get('sensor_id'):
                records.append(record)
        
        return records
    
    def _parse_incident_report(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит отчет о работе датчиков при аварии"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Sensor', 'Время работы', 'Примечание']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'sensor' in header or 'id' in header:
                indices['sensor_id'] = idx
            elif 'тип' in header:
                indices['sensor_type'] = idx
            elif 'время' in header and ('работ' in header or 'батаре' in header):
                indices['battery_actual'] = idx
            elif 'примечание' in header:
                pass  # Не используем пока
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 2:
                continue
            
            record = {}
            
            # sensor_id
            if 'sensor_id' in indices and indices['sensor_id'] < len(parts):
                record['sensor_id'] = parts[indices['sensor_id']]
            
            # sensor_type
            if 'sensor_type' in indices and indices['sensor_type'] < len(parts):
                raw_type = parts[indices['sensor_type']]
                record['sensor_type'] = self._normalize_sensor_type(raw_type)
            
            # actual_battery_time_at_incident
            if 'battery_actual' in indices and indices['battery_actual'] < len(parts):
                record['actual_battery_time_at_incident'] = self._to_float(
                    parts[indices['battery_actual']]
                )
            
            record['_source_file'] = file_name
            
            if record.get('sensor_id'):
                records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг для нестандартных форматов"""
        records = []
        
        # Ищем паттерны вида "M37_2" (ID датчика)
        sensor_ids = re.findall(r'([A-Z0-9]+_\d+)', content)
        
        for sensor_id in set(sensor_ids):
            # Ищем контекст вокруг ID
            lines = content.split('\n')
            context = ""
            for line in lines:
                if sensor_id in line:
                    context = line
                    break
            
            record = {
                'sensor_id': sensor_id,
                'sensor_type': self._infer_sensor_type(sensor_id, context),
                'model': None,
                'location': self._extract_location_from_context(context),
                'measurement_range': None,
                'unit': None,
                'last_calibration_date': None,
                'battery_life_hours': None,
                'actual_battery_time_at_incident': None,
                '_source_file': file_name
            }
            
            if record['unit'] is None and record['sensor_type']:
                record['unit'] = self._get_unit_by_type(record['sensor_type'])
            
            records.append(record)
        
        return records
    
    def _normalize_sensor_type(self, raw_type: str) -> str:
        """Нормализует тип датчика"""
        raw_upper = raw_type.upper()
        
        if 'CH4' in raw_upper:
            return 'CH4'
        elif 'CO' in raw_upper:
            return 'CO'
        elif 'СКОРОСТ' in raw_upper or 'SPEED' in raw_upper:
            return 'SPEED'
        elif 'ДАВЛЕНИ' in raw_upper or 'PRESSURE' in raw_upper:
            return 'PRESSURE'
        elif 'СЕЙСМ' in raw_upper or 'SEISMIC' in raw_upper:
            return 'SEISMIC'
        elif 'CH4/CO' in raw_upper or 'CO/CH4' in raw_upper:
            return 'CH4/CO'
        else:
            return raw_type
    
    def _get_unit_by_type(self, sensor_type: str) -> Optional[str]:
        """Возвращает единицу измерения по типу датчика"""
        sensor_type_upper = sensor_type.upper()
        
        if 'CH4' in sensor_type_upper:
            return '%'
        elif 'CO' in sensor_type_upper:
            return '%'
        elif 'SPEED' in sensor_type_upper:
            return 'm/s'
        elif 'PRESSURE' in sensor_type_upper:
            return 'mmHg'
        elif 'SEISMIC' in sensor_type_upper:
            return 'magnitude'
        else:
            return None
    
    def _infer_sensor_type(self, sensor_id: str, context: str) -> str:
        """Определяет тип датчика по ID и контексту"""
        # По ID
        if sensor_id.startswith('M') or 'CH4' in sensor_id:
            return 'CH4'
        elif sensor_id.startswith('OY') or 'CO' in sensor_id:
            return 'CO'
        elif sensor_id.startswith('C') or 'SPEED' in sensor_id or 'скорость' in context.lower():
            return 'SPEED'
        
        # По контексту
        context_lower = context.lower()
        if 'метан' in context_lower or 'ch4' in context_lower:
            return 'CH4'
        elif 'оксид углерода' in context_lower or 'co' in context_lower:
            return 'CO'
        elif 'скорость' in context_lower:
            return 'SPEED'
        
        return 'unknown'
    
    def _extract_location_from_context(self, context: str) -> Optional[str]:
        """Извлекает место установки из контекста"""
        patterns = [
            r'(ВШ\s+[0-9К-]+)',
            r'(КШ\s+[0-9К-]+)',
            r'(лава\s+[0-9К-]+)',
            r'(камера\s+[^\s]+)',
            r'(околоствольный\s+двор)',
            r'(свежая\s+струя)',
            r'(исходящая\s+струя)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, context, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def get_all_sensors(self) -> List[Dict[str, Any]]:
        """Возвращает все собранные датчики из кэша"""
        return list(self.sensors_cache.values())
    
    def clear_cache(self) -> None:
        """Очищает кэш датчиков"""
        self.sensors_cache.clear()