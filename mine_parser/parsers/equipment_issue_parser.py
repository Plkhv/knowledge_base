# parsers/equipment_issue_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_issue_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class EquipmentIssueParser(BaseParser):
    """
    Парсер для журнала выдачи оборудования и инструмента.
    Поддерживает: журналы выдачи, наряды-допуски, выдачу СИЗ.
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    # Справочник типов оборудования
    EQUIPMENT_TYPES = {
        'шлифовальн': 'инструмент',
        'диск': 'расходный материал',
        'огнетушитель': 'средство пожаротушения',
        'газоанализатор': 'контрольно-измерительный прибор',
        'молоток': 'инструмент',
        'лебедка': 'оборудование',
        'пур-патрон': 'расходный материал',
        'лом': 'инструмент',
        'кувалда': 'инструмент',
        'телефон': 'средство связи',
        'каска': 'СИЗ',
        'сапоги': 'СИЗ',
        'костюм': 'СИЗ',
        'респиратор': 'СИЗ',
    }
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("equipment_issue_log")
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга выдачи оборудования"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'issue', 'выдач', 'журнал', 'наряд', 'equipment_issue'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с записями о выдаче оборудования.
        """
        file_lower = file_name.lower()
        
        if 'журнал' in file_lower or 'issue_log' in file_lower:
            records = self._parse_issue_log(content, file_name)
        elif 'сиз' in file_lower or 'safety' in file_lower:
            records = self._parse_safety_issue(content, file_name)
        elif 'наряд' in file_lower or 'work_order' in file_lower:
            records = self._parse_work_order_issue(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Добавляем incident_id
        for record in records:
            record['incident_id'] = self.INCIDENT_ID
        
        return records
    
    def _parse_issue_log(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит журнал выдачи (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Дата выдачи', 'Оборудование', 'Кому выдано']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'дата' in header:
                indices['issue_date'] = idx
            elif 'смен' in header:
                indices['shift'] = idx
            elif 'оборуд' in header or 'инструмент' in header or 'наименован' in header:
                indices['equipment_name'] = idx
            elif 'инв' in header or 'номер' in header:
                indices['inventory_number'] = idx
            elif 'кол' in header:
                indices['quantity'] = idx
            elif 'ед' in header:
                indices['unit'] = idx
            elif 'кому' in header or 'выдано' in header:
                indices['issued_to'] = idx
            elif 'должн' in header:
                indices['position'] = idx
            elif 'назначен' in header or 'цель' in header:
                indices['purpose'] = idx
            elif 'возврат' in header:
                indices['return_date'] = idx
            elif 'примечан' in header:
                indices['notes'] = idx
        
        # Извлекаем информацию о выдавшем
        issued_by_match = re.search(r'Ответственный[:\s]+([^\n]+)', content)
        issued_by = issued_by_match.group(1).strip() if issued_by_match else None
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            if 'Итого' in line:
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем данные
            issue_date_str = parts[indices['issue_date']] if 'issue_date' in indices and indices['issue_date'] < len(parts) else None
            issue_date = DateParser.parse_to_str(issue_date_str) if issue_date_str else None
            
            shift = parts[indices['shift']] if 'shift' in indices and indices['shift'] < len(parts) else None
            
            equipment_name = parts[indices['equipment_name']] if 'equipment_name' in indices and indices['equipment_name'] < len(parts) else None
            if not equipment_name:
                continue
            
            # Определяем тип оборудования
            equipment_type = self._determine_equipment_type(equipment_name)
            
            inventory_number = parts[indices['inventory_number']] if 'inventory_number' in indices and indices['inventory_number'] < len(parts) else None
            
            quantity = self._to_int(parts[indices['quantity']]) if 'quantity' in indices and indices['quantity'] < len(parts) else 1
            
            unit = parts[indices['unit']] if 'unit' in indices and indices['unit'] < len(parts) else 'шт'
            
            issued_to = parts[indices['issued_to']] if 'issued_to' in indices and indices['issued_to'] < len(parts) else None
            
            position = parts[indices['position']] if 'position' in indices and indices['position'] < len(parts) else None
            
            purpose = parts[indices['purpose']] if 'purpose' in indices and indices['purpose'] < len(parts) else None
            
            return_date_str = parts[indices['return_date']] if 'return_date' in indices and indices['return_date'] < len(parts) else None
            return_date = DateParser.parse_to_str(return_date_str) if return_date_str else None
            
            notes = parts[indices['notes']] if 'notes' in indices and indices['notes'] < len(parts) else None
            
            # Определяем признак возврата
            is_returned = 1 if return_date else 0
            
            record = {
                'issue_id': generate_issue_id(),
                'incident_id': self.INCIDENT_ID,
                'equipment_name': equipment_name,
                'equipment_type': equipment_type,
                'inventory_number': inventory_number,
                'quantity': quantity,
                'unit': unit,
                'issued_to': issued_to,
                'employee_id': None,  # будет заполнен post-hoc
                'position': position,
                'issue_date': issue_date,
                'shift': self._normalize_shift(shift),
                'return_date': return_date,
                'is_returned': is_returned,
                'issued_by': issued_by,
                'purpose': purpose,
                'notes': notes,
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    def _parse_safety_issue(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит журнал выдачи СИЗ"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['ФИО', 'Наименование', 'СИЗ']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'дата' in header:
                indices['issue_date'] = idx
            elif 'смен' in header:
                indices['shift'] = idx
            elif 'фио' in header or 'фамили' in header:
                indices['issued_to'] = idx
            elif 'должн' in header:
                indices['position'] = idx
            elif 'наименован' in header:
                indices['equipment_name'] = idx
            elif 'размер' in header:
                indices['size'] = idx
            elif 'кол' in header:
                indices['quantity'] = idx
            elif 'выдал' in header:
                indices['issued_by'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            issue_date_str = parts[indices['issue_date']] if 'issue_date' in indices and indices['issue_date'] < len(parts) else None
            issue_date = DateParser.parse_to_str(issue_date_str) if issue_date_str else None
            
            shift = parts[indices['shift']] if 'shift' in indices and indices['shift'] < len(parts) else None
            
            issued_to = parts[indices['issued_to']] if 'issued_to' in indices and indices['issued_to'] < len(parts) else None
            if not issued_to:
                continue
            
            position = parts[indices['position']] if 'position' in indices and indices['position'] < len(parts) else None
            
            equipment_name = parts[indices['equipment_name']] if 'equipment_name' in indices and indices['equipment_name'] < len(parts) else None
            equipment_type = 'СИЗ'
            
            quantity = self._to_int(parts[indices['quantity']]) if 'quantity' in indices and indices['quantity'] < len(parts) else 1
            
            issued_by = parts[indices['issued_by']] if 'issued_by' in indices and indices['issued_by'] < len(parts) else None
            
            record = {
                'issue_id': generate_issue_id(),
                'incident_id': self.INCIDENT_ID,
                'equipment_name': equipment_name,
                'equipment_type': equipment_type,
                'inventory_number': None,
                'quantity': quantity,
                'unit': 'шт',
                'issued_to': issued_to,
                'employee_id': None,
                'position': position,
                'issue_date': issue_date,
                'shift': self._normalize_shift(shift),
                'return_date': None,
                'is_returned': 0,
                'issued_by': issued_by,
                'purpose': 'СИЗ',
                'notes': None,
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    def _parse_work_order_issue(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит выдачу по наряду-допуску"""
        records = []
        
        # Извлекаем дату и смену
        date_match = re.search(r'Дата:\s*(\d{2}\.\d{2}\.\d{4})', content)
        issue_date = DateParser.parse_to_str(date_match.group(1)) if date_match else None
        
        shift_match = re.search(r'Смена:\s*(\d+)[-я]', content)
        shift = shift_match.group(1) if shift_match else None
        
        # Извлекаем ответственного за выдачу
        issued_by_match = re.search(r'Ответственный за выдачу:\s*([^\n]+)', content)
        issued_by = issued_by_match.group(1).strip() if issued_by_match else None
        
        # Ищем таблицу с оборудованием
        lines = content.split('\n')
        in_table = False
        
        for line in lines:
            if '|' in line:
                if 'Наименование' in line and 'Инв.' in line:
                    in_table = True
                    continue
                
                if in_table and '|' in line and not line.strip().startswith('---'):
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 5:
                        equipment_name = parts[1] if len(parts) > 1 else None
                        inventory_number = parts[2] if len(parts) > 2 else None
                        quantity = self._to_int(parts[3]) if len(parts) > 3 else 1
                        issued_to = parts[4] if len(parts) > 4 else None
                        
                        if equipment_name:
                            equipment_type = self._determine_equipment_type(equipment_name)
                            
                            record = {
                                'issue_id': generate_issue_id(),
                                'incident_id': self.INCIDENT_ID,
                                'equipment_name': equipment_name,
                                'equipment_type': equipment_type,
                                'inventory_number': inventory_number,
                                'quantity': quantity,
                                'unit': 'шт',
                                'issued_to': issued_to,
                                'employee_id': None,
                                'position': None,
                                'issue_date': issue_date,
                                'shift': self._normalize_shift(shift),
                                'return_date': None,
                                'is_returned': 0,
                                'issued_by': issued_by,
                                'purpose': 'демонтаж крепи',
                                'notes': None,
                                '_source_file': file_name
                            }
                            records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг выдачи оборудования"""
        records = []
        
        # Ищем строки с выдачей
        patterns = [
            r'([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.).*?(?:выдан|получил)\s+([^,\n]+)',
            r'(\d{2}\.\d{2}\.\d{4}).*?(\w+)\s+(\d+)\s+шт',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                record = {
                    'issue_id': generate_issue_id(),
                    'incident_id': self.INCIDENT_ID,
                    'equipment_name': match[2] if len(match) > 2 else None,
                    'equipment_type': None,
                    'inventory_number': None,
                    'quantity': 1,
                    'unit': 'шт',
                    'issued_to': f"{match[0]} {match[1]}" if len(match) > 1 else None,
                    'employee_id': None,
                    'position': None,
                    'issue_date': DateParser.parse_to_str(match[0]) if match[0] else None,
                    'shift': None,
                    'return_date': None,
                    'is_returned': 0,
                    'issued_by': None,
                    'purpose': None,
                    'notes': None,
                    '_source_file': file_name
                }
                if record['equipment_name']:
                    record['equipment_type'] = self._determine_equipment_type(record['equipment_name'])
                    records.append(record)
        
        return records
    
    def _determine_equipment_type(self, equipment_name: str) -> str:
        """Определяет тип оборудования по названию"""
        if not equipment_name:
            return 'оборудование'
        
        name_lower = equipment_name.lower()
        for keyword, eq_type in self.EQUIPMENT_TYPES.items():
            if keyword in name_lower:
                return eq_type
        
        return 'оборудование'
    
    def _normalize_shift(self, shift: Optional[str]) -> Optional[str]:
        """Нормализует обозначение смены"""
        if not shift:
            return None
        
        shift_str = str(shift)
        if '1' in shift_str or 'перв' in shift_str.lower():
            return '1-я'
        elif '2' in shift_str or 'втор' in shift_str.lower():
            return '2-я'
        elif '3' in shift_str or 'трет' in shift_str.lower():
            return '3-я'
        elif '4' in shift_str or 'четв' in shift_str.lower():
            return '4-я'
        
        return shift_str