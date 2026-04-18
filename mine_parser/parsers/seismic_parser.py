# parsers/seismic_parser.py
# -*- coding: utf-8 -*-

import re
import json
import csv
import io
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2

from base_parser import BaseParser
from id_generator import generate_seismic_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class SeismicParser(BaseParser):
    """
    Парсер для сейсмических событий.
    Поддерживает: CSV, XML, TXT, JSON (USGS/EMSC форматы).
    """
    
    # Координаты шахты им. Костенко (для расчета расстояния)
    MINE_LATITUDE = 49.8200
    MINE_LONGITUDE = 73.1200
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("seismic_event")
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга сейсмических событий"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'seismic', 'сейсмическ', 'earthquake', 'quake'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с сейсмическими событиями.
        Определяет формат по расширению и содержимому.
        """
        file_ext = Path(file_name).suffix.lower()
        
        if file_ext == '.csv':
            return self._parse_csv(content, file_name)
        else:
            return self._parse_text(content, file_name)
    
    def _parse_csv(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит CSV файл"""
        records = []
        
        try:
            # Пробуем прочитать CSV
            csv_reader = csv.DictReader(io.StringIO(content))
            
            for row in csv_reader:
                record = self._parse_csv_row(row, file_name)
                if record:
                    # Добавляем расчетное расстояние до шахты
                    record['distance_to_mine_km'] = self._calculate_distance(
                        record.get('latitude'),
                        record.get('longitude')
                    )
                    records.append(record)
        except Exception as e:
            print(f"Error parsing CSV: {e}")
        
        return records
    
    def _parse_csv_row(self, row: Dict[str, str], file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит одну строку CSV"""
        
        # Определяем названия колонок (могут быть на русском или английском)
        event_id = row.get('event_id') or row.get('id')
        event_dttm = row.get('event_dttm') or row.get('time') or row.get('datetime')
        latitude = row.get('latitude') or row.get('lat')
        longitude = row.get('longitude') or row.get('lon')
        depth_km = row.get('depth_km') or row.get('depth')
        magnitude = row.get('magnitude') or row.get('mag')
        energy_class = row.get('energy_class') or row.get('class')
        source = row.get('source') or 'unknown'
        
        # Парсим дату
        event_datetime = None
        if event_dttm:
            event_datetime = DateParser.parse_timestamp(str(event_dttm))
        
        record = {
            'event_id': event_id or generate_seismic_id(),
            'event_dttm': event_datetime.strftime('%Y-%m-%d %H:%M:%S') if event_datetime else None,
            'latitude': self._to_float(latitude),
            'longitude': self._to_float(longitude),
            'depth_km': self._to_float(depth_km),
            'magnitude': self._to_float(magnitude),
            'energy_class': self._to_float(energy_class),
            'source': str(source).strip() if source else 'unknown',
            '_source_file': file_name
        }
        
        return record if record['event_dttm'] else None
    
    def _parse_usgs_feature(self, feature: Dict, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит USGS GeoJSON feature"""
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        coords = geom.get('coordinates', [])
        
        # USGS: coordinates = [longitude, latitude, depth]
        longitude = coords[0] if len(coords) > 0 else None
        latitude = coords[1] if len(coords) > 1 else None
        depth_km = coords[2] if len(coords) > 2 else None
        
        # Время в миллисекундах
        time_ms = props.get('time')
        event_dttm = None
        if time_ms:
            from datetime import datetime
            event_dttm = datetime.fromtimestamp(time_ms / 1000)
        
        record = {
            'event_id': feature.get('id') or generate_seismic_id(),
            'event_dttm': event_dttm.strftime('%Y-%m-%d %H:%M:%S') if event_dttm else None,
            'latitude': latitude,
            'longitude': longitude,
            'depth_km': depth_km,
            'magnitude': props.get('mag'),
            'energy_class': None,
            'source': props.get('net', 'USGS'),
            '_source_file': file_name
        }
        
        # Добавляем расчетное расстояние
        if record['latitude'] and record['longitude']:
            record['distance_to_mine_km'] = self._calculate_distance(
                record['latitude'], record['longitude']
            )
        
        return record if record['event_dttm'] else None
    
    def _parse_text(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит текстовый файл (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Дата', 'Широта', 'Долгота', 'Магнитуда']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'дата' in header or 'время' in header:
                indices['datetime'] = idx
            elif 'широт' in header:
                indices['latitude'] = idx
            elif 'долгот' in header:
                indices['longitude'] = idx
            elif 'глубин' in header:
                indices['depth'] = idx
            elif 'магнитуд' in header:
                indices['magnitude'] = idx
            elif 'энерг' in header or 'класс' in header:
                indices['energy_class'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем дату
            dt_idx = indices.get('datetime')
            if dt_idx is None or dt_idx >= len(parts):
                continue
            
            event_dttm = DateParser.parse_timestamp(parts[dt_idx])
            if not event_dttm:
                continue
            
            record = {
                'event_id': generate_seismic_id(),
                'event_dttm': event_dttm.strftime('%Y-%m-%d %H:%M:%S'),
                'latitude': self._to_float(parts[indices['latitude']]) if 'latitude' in indices else None,
                'longitude': self._to_float(parts[indices['longitude']]) if 'longitude' in indices else None,
                'depth_km': self._to_float(parts[indices['depth']]) if 'depth' in indices else None,
                'magnitude': self._to_float(parts[indices['magnitude']]) if 'magnitude' in indices else None,
                'energy_class': self._to_float(parts[indices['energy_class']]) if 'energy_class' in indices else None,
                'source': self._extract_source_from_text(content),
                '_source_file': file_name
            }
            
            # Добавляем расчетное расстояние
            if record['latitude'] and record['longitude']:
                record['distance_to_mine_km'] = self._calculate_distance(
                    record['latitude'], record['longitude']
                )
            
            records.append(record)
        
        return records
    
    def _extract_source_from_text(self, content: str) -> str:
        """Извлекает источник данных из текста"""
        patterns = [
            r'Источник:\s*([^\n]+)',
            r'Source:\s*([^\n]+)',
            r'Источник данных:\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return 'unknown'
    
    def _calculate_distance(self, lat: Optional[float], lon: Optional[float]) -> Optional[float]:
        """
        Рассчитывает расстояние от сейсмического события до шахты.
        Используется формула гаверсинуса.
        
        Args:
            lat: Широта события
            lon: Долгота события
        
        Returns:
            Расстояние в километрах
        """
        if lat is None or lon is None:
            return None
        
        R = 6371  # Радиус Земли в км
        
        lat1 = radians(self.MINE_LATITUDE)
        lon1 = radians(self.MINE_LONGITUDE)
        lat2 = radians(lat)
        lon2 = radians(lon)
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return round(R * c, 2)
    
    def _to_float(self, value: Any) -> Optional[float]:
        """Безопасное преобразование в float"""
        if value is None:
            return None
        try:
            return float(str(value).replace(',', '.').strip())
        except (ValueError, TypeError):
            return None