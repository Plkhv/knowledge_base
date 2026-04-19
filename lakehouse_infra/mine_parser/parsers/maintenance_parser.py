# parsers/maintenance_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_maintenance_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser


class MaintenanceParser(BaseParser):
    """
    Парсер для журналов технического обслуживания.
    Поддерживает: комбайны, конвейеры, крепь.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("equipment_maintenance")
        # Словарь для сопоставления оборудования (будет заполняться post-hoc)
        self.equipment_mapping = {}
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга журналов ТО"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'maintenance', 'то', 'журнал', 'техническое обслуживание'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит журнал технического обслуживания.
        Поддерживает табличный формат с разделителем |.
        """
        records = []
        
        # Определяем оборудование из файла
        equipment_model = self._extract_equipment_model(content, file_name)
        
        # Парсим таблицу
        records = self._parse_table(content, file_name, equipment_model)
        
        return records
    
    def _extract_equipment_model(self, content: str, file_name: str) -> Optional[str]:
        """Извлекает модель оборудования из файла"""
        patterns = [
            r'Оборудование:\s*([^\n]+)',
            r'Equipment:\s*([^\n]+)',
            r'Комбайн\s+([A-Z0-9/.-]+)',
            r'Конвейер\s+([A-Z0-9/.-]+)',
            r'Крепь\s+([A-Z0-9/.-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # По имени файла
        name_lower = file_name.lower()
        if 'combine' in name_lower or 'комбайн' in name_lower:
            return 'FS300/1.0'
        elif 'conveyor' in name_lower or 'конвейер' in name_lower:
            return 'FFC-8'
        elif 'support' in name_lower or 'крепь' in name_lower:
            return 'Glinik 10/25-2'
        
        return None
    
    def _parse_table(self, content: str, file_name: str, equipment_model: Optional[str]) -> List[Dict[str, Any]]:
        """Парсит таблицу с записями ТО"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        header_indices = None
        
        for line in lines:
            if '|' not in line:
                continue
            
            # Определяем заголовки по ключевым словам
            if any(keyword in line for keyword in ['Дата', 'Смена', 'Тип работ', 'Исполнитель', 'Замечания', 'Выполнено']):
                header_line = line
                header_indices = self._parse_header(line)
                break
        
        if not header_indices:
            return []
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 5:
                continue
            
            record = self._parse_record(parts, header_indices, file_name, equipment_model)
            if record:
                records.append(record)
        
        return records
    
    def _parse_header(self, header_line: str) -> Dict[str, int]:
        """Парсит заголовок таблицы, возвращает словарь {название_поля: индекс}"""
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'дата' in header:
                indices['date'] = idx
            elif 'смен' in header:
                indices['shift'] = idx
            elif 'тип' in header or 'работ' in header:
                indices['operation_type'] = idx
            elif 'исполнитель' in header or 'кто' in header:
                indices['performed_by'] = idx
            elif 'замечан' in header or 'примечан' in header:
                indices['anomaly_notes'] = idx
            elif 'выполнен' in header or 'статус' in header:
                indices['is_completed'] = idx
            elif 'секц' in header:
                indices['section'] = idx
        
        return indices
    
    def _parse_record(
        self, 
        parts: List[str], 
        header_indices: Dict[str, int], 
        file_name: str,
        equipment_model: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Парсит одну строку таблицы"""
        
        # Извлекаем дату
        date_idx = header_indices.get('date')
        if date_idx is None or date_idx >= len(parts):
            return None
        
        date_str = parts[date_idx]
        maintenance_date = DateParser.parse_to_str(date_str)
        if not maintenance_date:
            return None
        
        # Извлекаем смену
        shift_idx = header_indices.get('shift')
        shift = parts[shift_idx] if shift_idx and shift_idx < len(parts) else None
        
        # Извлекаем тип работ
        op_idx = header_indices.get('operation_type')
        operation_type = parts[op_idx] if op_idx and op_idx < len(parts) else None
        
        # Извлекаем исполнителя
        performer_idx = header_indices.get('performed_by')
        performed_by = parts[performer_idx] if performer_idx and performer_idx < len(parts) else None
        
        # Извлекаем замечания
        notes_idx = header_indices.get('anomaly_notes')
        anomaly_notes = parts[notes_idx] if notes_idx and notes_idx < len(parts) else None
        
        # Извлекаем статус выполнения
        completed_idx = header_indices.get('is_completed')
        is_completed = None
        if completed_idx and completed_idx < len(parts):
            val = parts[completed_idx].lower()
            if val in ['да', '+', 'yes', 'выполнено', '1']:
                is_completed = 1
            elif val in ['нет', '-', 'no', 'не выполнено', '0']:
                is_completed = 0
        
        # Если нет информации о выполнении, считаем что выполнено (по умолчанию)
        if is_completed is None:
            is_completed = 1
        
        # Определяем equipment_id по модели
        equipment_id = None
        if equipment_model:
            equipment_id = self.equipment_mapping.get(equipment_model)
        
        record = {
            'maintenance_id': generate_maintenance_id(),
            'equipment_id': equipment_id,
            'maintenance_date': maintenance_date,
            'shift': self._normalize_shift(shift),
            'operation_type': operation_type,
            'performed_by': performed_by,
            'anomaly_notes': anomaly_notes,
            'is_completed': is_completed,
            '_source_file': file_name,
            '_equipment_model': equipment_model  # временное поле для отладки
        }
        
        # Очищаем строковые поля
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = TextCleaner.clean_whitespace(value)
        
        return record
    
    def _normalize_shift(self, shift: Optional[str]) -> Optional[str]:
        """Нормализует обозначение смены"""
        if not shift:
            return None
        
        shift_lower = shift.lower()
        if '1' in shift_lower or 'перв' in shift_lower:
            return '1-я'
        elif '2' in shift_lower or 'втор' in shift_lower:
            return '2-я'
        elif '3' in shift_lower or 'трет' in shift_lower:
            return '3-я'
        elif '4' in shift_lower or 'четв' in shift_lower:
            return '4-я'
        
        return shift
    
    def set_equipment_mapping(self, mapping: Dict[str, str]) -> None:
        """
        Устанавливает маппинг для сопоставления оборудования.
        
        Args:
            mapping: Словарь вида {"модель": "equipment_id"}
        """
        self.equipment_mapping = mapping