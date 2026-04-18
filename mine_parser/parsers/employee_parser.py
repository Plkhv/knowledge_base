# parsers/employee_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_employee_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class EmployeeParser(BaseParser):
    """
    Парсер для данных о сотрудниках.
    Поддерживает: списки сотрудников, книги нарядов, списки пострадавших.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("employee")
        self.employees_cache = {}
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'employee', 'сотрудник', 'список', 'личный', 'погибш', 
            'пострадавш', 'work_order', 'наряд', 'смена', 'журнал',
            'employees_list', 'incident_victims', 'shift_log'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        if 'employees_list' in file_name.lower():
            records = self._parse_employee_list(content, file_name)
        elif 'incident_victims' in file_name.lower():
            records = self._parse_victims_list(content, file_name)
        elif 'shift_log' in file_name.lower():
            records = self._parse_shift_log(content, file_name)
        elif 'work_order' in file_name.lower():
            records = self._parse_work_order(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Объединяем с кэшем
        for record in records:
            fio = self._get_fio_key(record)
            if fio and fio in self.employees_cache:
                old_record = self.employees_cache[fio]
                old_record.update(record)
            elif fio:
                self.employees_cache[fio] = record
        
        return records
    
    def _parse_employee_list(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит список сотрудников (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and 'Табельный номер' in line:
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'табельный' in header:
                indices['id_number'] = idx
            elif 'фамилия' in header:
                indices['lastname'] = idx
            elif 'имя' in header:
                indices['firstname'] = idx
            elif 'отчество' in header:
                indices['middlename'] = idx
            elif 'дата рождения' in header:
                indices['birth_date'] = idx
            elif 'должность' in header:
                indices['position'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем ФИО
            lastname = parts[indices['lastname']] if 'lastname' in indices and indices['lastname'] < len(parts) else None
            firstname = parts[indices['firstname']] if 'firstname' in indices and indices['firstname'] < len(parts) else None
            middlename = parts[indices['middlename']] if 'middlename' in indices and indices['middlename'] < len(parts) else None
            
            # Извлекаем дату рождения
            birth_date = None
            if 'birth_date' in indices and indices['birth_date'] < len(parts):
                birth_date = DateParser.parse_to_str(parts[indices['birth_date']])
            
            # Извлекаем должность
            position = parts[indices['position']] if 'position' in indices and indices['position'] < len(parts) else None
            
            if lastname:
                record = {
                    'employee_id': generate_employee_id(),
                    'lastname': lastname,
                    'firstname': firstname,
                    'middlename': middlename,
                    'birth_date': birth_date,
                    'position': position,
                    'department': 'участок №6',
                    'status_at_incident': None,
                    'injury_type': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records
    
    def _parse_victims_list(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит список погибших и пострадавших"""
        records = []
        
        current_status = None
        
        for line in content.split('\n'):
            line = line.strip()
            
            if 'ПОГИБШИЕ' in line:
                current_status = 'погиб'
                continue
            elif 'ПОСТРАДАВШИЕ' in line:
                current_status = 'пострадал'
                continue
            
            # Парсим строки вида: "1. Бобряшов Сергей Владимирович - ГРОЗ участка №6 (отравление)"
            if current_status and re.match(r'^\d+\.', line):
                # Формат с ФИО
                match = re.match(r'^\d+\.\s*([А-Я][а-я]+)\s+([А-Я][а-я]+)\s+([А-Я][а-я]+)?\s*[-–]\s*([^(]+)(?:\s*\(([^)]+)\))?', line)
                if match:
                    lastname = match.group(1)
                    firstname = match.group(2)
                    middlename = match.group(3) if match.group(3) else None
                    position_raw = match.group(4).strip()
                    injury_raw = match.group(5) if match.group(5) else None
                    
                    # Извлекаем должность
                    position = self._extract_position(position_raw)
                    
                    record = {
                        'employee_id': generate_employee_id(),
                        'lastname': lastname,
                        'firstname': firstname,
                        'middlename': middlename,
                        'birth_date': None,
                        'position': position,
                        'department': 'участок №6',
                        'status_at_incident': current_status,
                        'injury_type': injury_raw,
                        '_source_file': file_name
                    }
                    records.append(record)
                
                # Альтернативный формат: "1. Бобряшов С.В. - ГРОЗ"
                match = re.match(r'^\d+\.\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)\s*[-–]\s*([^(]+)', line)
                if match and not records:
                    lastname = match.group(1)
                    initials = match.group(2)
                    position = match.group(3).strip()
                    
                    firstname = initials[0] + '.' if len(initials) > 0 else None
                    middlename = initials[2] + '.' if len(initials) > 2 else None
                    
                    record = {
                        'employee_id': generate_employee_id(),
                        'lastname': lastname,
                        'firstname': firstname,
                        'middlename': middlename,
                        'birth_date': None,
                        'position': position,
                        'department': 'участок №6',
                        'status_at_incident': current_status,
                        'injury_type': None,
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records
    
    def _parse_shift_log(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит журнал смены"""
        records = []
        
        # Ищем строки с сотрудниками
        for line in content.split('\n'):
            # Формат: "1. Иванов И.И. (ГРОЗ) - явился"
            match = re.match(r'^\d+\.\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)\s*\(([^)]+)\)', line)
            if match:
                lastname = match.group(1)
                initials = match.group(2)
                position = match.group(3)
                
                firstname = initials[0] + '.' if len(initials) > 0 else None
                middlename = initials[2] + '.' if len(initials) > 2 else None
                
                record = {
                    'employee_id': generate_employee_id(),
                    'lastname': lastname,
                    'firstname': firstname,
                    'middlename': middlename,
                    'birth_date': None,
                    'position': position,
                    'department': 'участок №6',
                    'status_at_incident': 'выжил',
                    'injury_type': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records
    
    def _parse_work_order(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит книгу нарядов"""
        records = []
        
        # Ищем список погибших в детализации наряда
        in_list = False
        for line in content.split('\n'):
            line = line.strip()
            
            if 'Состав бригады:' in line:
                in_list = True
                continue
            
            if in_list and re.match(r'^\d+\.', line):
                # Формат: "1. Бобряшов С.В. (ГРОЗ) - погиб"
                match = re.match(r'^\d+\.\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)\s*\(([^)]+)\)\s*[-–]\s*(.+)', line)
                if match:
                    lastname = match.group(1)
                    initials = match.group(2)
                    position = match.group(3)
                    status_text = match.group(4).lower()
                    
                    firstname = initials[0] + '.' if len(initials) > 0 else None
                    middlename = initials[2] + '.' if len(initials) > 2 else None
                    
                    status = 'погиб' if 'погиб' in status_text else 'выжил'
                    
                    record = {
                        'employee_id': generate_employee_id(),
                        'lastname': lastname,
                        'firstname': firstname,
                        'middlename': middlename,
                        'birth_date': None,
                        'position': position,
                        'department': 'участок №6',
                        'status_at_incident': status,
                        'injury_type': None,
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг"""
        records = []
        
        # Ищем ФИО
        pattern = r'([А-Я][а-я]+)\s+([А-Я][а-я]+)\s+([А-Я][а-я]+)'
        matches = re.findall(pattern, content)
        
        for lastname, firstname, middlename in matches:
            record = {
                'employee_id': generate_employee_id(),
                'lastname': lastname,
                'firstname': firstname,
                'middlename': middlename,
                'birth_date': None,
                'position': None,
                'department': None,
                'status_at_incident': None,
                'injury_type': None,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    def _extract_position(self, text: str) -> Optional[str]:
        """Извлекает должность из текста"""
        if not text:
            return None
        
        patterns = [
            r'ГРОЗ',
            r'электрослесарь',
            r'помощник ГРОЗ',
            r'главный механик',
            r'инженер-газовик',
            r'начальник участка',
            r'ВГСЧ',
        ]
        
        for pattern in patterns:
            if pattern.lower() in text.lower():
                return pattern
        
        return text.strip()
    
    def _get_fio_key(self, record: Dict[str, Any]) -> Optional[str]:
        """Формирует ключ для кэша по ФИО"""
        lastname = record.get('lastname')
        firstname = record.get('firstname')
        
        if not lastname:
            return None
        
        if firstname:
            return f"{lastname}_{firstname}"
        return lastname
    
    def get_all_employees(self) -> List[Dict[str, Any]]:
        return list(self.employees_cache.values())
    
    def clear_cache(self) -> None:
        self.employees_cache.clear()