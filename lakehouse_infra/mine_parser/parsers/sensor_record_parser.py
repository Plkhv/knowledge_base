# parsers/sensor_record_parser.py
# -*- coding: utf-8 -*-

import csv
import io
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_sensor_record_id
from parsers.date_parser import DateParser


class SensorRecordParser(BaseParser):
    """
    Парсер для показаний датчиков (телеметрия).
    Поддерживает: CSV форматы SCADA.
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    # Пороговые значения для определения критичности
    THRESHOLDS = {
        'CH4': {'warning': 0.5, 'alarm': 1.0},
        'CO': {'warning': 0.001, 'alarm': 0.002},
        'SPEED': {'min': 0.5, 'max': 4.0},
    }
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("sensor_record")
    
# parsers/sensor_record_parser.py - добавьте в supports():

    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'scada', 'sensor_record', 'trends', 'equipment_trends', 'mine_scada',
            'barometric', 'давление', 'seismic', 'сейсмическ', 'velocity'  # ← добавить
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит CSV файл с показаниями датчиков.
        """
        records = []
        file_ext = Path(file_name).suffix.lower()
        
        if file_ext == '.csv':
            records = self._parse_csv(content, file_name)
        else:
            records = self._parse_text(content, file_name)
        
        # Добавляем incident_id и вычисляем is_critical
        for record in records:
            record['incident_id'] = self.INCIDENT_ID
            record['is_critical'] = self._check_critical(
                record.get('sensor_type'),
                record.get('value')
            )
        
        return records
    
    def _parse_csv(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит CSV файл"""
        records = []
        
        try:
            csv_reader = csv.DictReader(io.StringIO(content))
            
            for row in csv_reader:
                # Определяем тип датчика
                sensor_type = self._determine_sensor_type(row)
                
                # Парсим время
                timestamp_str = row.get('timestamp_utc') or row.get('timestamp') or row.get('record_dttm')
                record_dttm = None
                if timestamp_str:
                    dt = DateParser.parse_timestamp(timestamp_str)
                    if dt:
                        record_dttm = dt.strftime('%Y-%m-%d %H:%M:%S')
                
                if not record_dttm:
                    continue
                
                # Извлекаем значение
                value = self._to_float(row.get('value'))
                if value is None:
                    continue
                
                # Извлекаем статус качества данных
                status = row.get('status', '1')
                data_quality_flag = 1 if str(status) in ['1', 'normal'] else 0
                
                record = {
                    'record_id': generate_sensor_record_id(),
                    'incident_id': self.INCIDENT_ID,
                    'sensor_id': row.get('sensor_id', '').strip(),
                    'sensor_type': sensor_type,
                    'record_dttm': record_dttm,
                    'value': value,
                    'unit': row.get('unit', self._get_unit_by_type(sensor_type)),
                    'is_critical': None,  # будет вычислен позже
                    'data_quality_flag': data_quality_flag,
                    'x_coordinate': None,
                    'y_coordinate': None,
                    '_source_file': file_name
                }
                
                records.append(record)
                
        except Exception as e:
            print(f"Error parsing CSV: {e}")
        
        return records
    
    def _parse_text(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит текстовый файл (альтернативный формат)"""
        records = []
        lines = content.split('\n')
        
        for line in lines:
            if ',' not in line:
                continue
            if 'timestamp' in line.lower():
                continue
            
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 5:
                continue
            
            timestamp_str = parts[0] if len(parts) > 0 else None
            sensor_id = parts[1] if len(parts) > 1 else None
            sensor_type = parts[2] if len(parts) > 2 else None
            value = self._to_float(parts[3]) if len(parts) > 3 else None
            unit = parts[4] if len(parts) > 4 else None
            
            if not timestamp_str or value is None:
                continue
            
            record_dttm = None
            dt = DateParser.parse_timestamp(timestamp_str)
            if dt:
                record_dttm = dt.strftime('%Y-%m-%d %H:%M:%S')
            
            if not record_dttm:
                continue
            
            record = {
                'record_id': generate_sensor_record_id(),
                'incident_id': self.INCIDENT_ID,
                'sensor_id': sensor_id,
                'sensor_type': self._normalize_sensor_type(sensor_type),
                'record_dttm': record_dttm,
                'value': value,
                'unit': unit or self._get_unit_by_type(sensor_type),
                'is_critical': None,
                'data_quality_flag': 1,
                'x_coordinate': None,
                'y_coordinate': None,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    def _determine_sensor_type(self, row: Dict[str, str]) -> str:
        """Определяет тип датчика по строке CSV"""
        param = row.get('parameter', '').upper()
        sensor_id = row.get('sensor_id', '').upper()
        
        if 'CH4' in param or 'CH4' in sensor_id:
            return 'CH4'
        elif 'CO' in param or 'CO' in sensor_id or 'OY' in sensor_id:
            return 'CO'
        elif 'SPEED' in param or 'C' in sensor_id:
            return 'SPEED'
        else:
            return param or 'unknown'
    
    def _normalize_sensor_type(self, sensor_type: Optional[str]) -> str:
        """Нормализует тип датчика"""
        if not sensor_type:
            return 'unknown'
        
        type_upper = sensor_type.upper()
        if 'CH4' in type_upper:
            return 'CH4'
        elif 'CO' in type_upper:
            return 'CO'
        elif 'SPEED' in type_upper:
            return 'SPEED'
        else:
            return sensor_type
    
    def _get_unit_by_type(self, sensor_type: str|None) -> str:
        """Возвращает единицу измерения по типу датчика"""
        if sensor_type == 'CH4':
            return '%'
        elif sensor_type == 'CO':
            return '%'
        elif sensor_type == 'SPEED':
            return 'm/s'
        else:
            return ''
    
    def _check_critical(self, sensor_type: Optional[str], value: Optional[float]) -> int:
        """Проверяет, является ли значение критическим"""
        if not sensor_type or value is None:
            return 0
        
        sensor_type = sensor_type.upper()
        
        if sensor_type == 'CH4':
            threshold = self.THRESHOLDS['CH4']['alarm']
            return 1 if value >= threshold else 0
        elif sensor_type == 'CO':
            threshold = self.THRESHOLDS['CO']['alarm']
            return 1 if value >= threshold else 0
        elif sensor_type == 'SPEED':
            min_val = self.THRESHOLDS['SPEED']['min']
            max_val = self.THRESHOLDS['SPEED']['max']
            return 1 if value < min_val or value > max_val else 0
        
        return 0