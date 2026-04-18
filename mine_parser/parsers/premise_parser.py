# parsers/premise_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_premise_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class PremiseParser(BaseParser):
    """
    Парсер для помещений и выработок.
    Поддерживает: списки выработок, паспорта лав, журналы проветривания.
    """
    
    # Константы
    COMPANY_ID = "COMP-00001"  # ID шахты им. Костенко
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("premise")
        # Хранилище для объединения данных из разных файлов
        self.premises_cache = {}
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга выработок"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'premises', 'выработк', 'помещени', 'паспорт', 'face_passport',
            'ventilation_log', 'проветриван'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с данными о выработках.
        Объединяет информацию из разных источников по названию.
        """
        file_lower = file_name.lower()
        
        if 'premises' in file_lower or 'выработк' in file_lower:
            records = self._parse_premises_list(content, file_name)
        elif 'паспорт' in file_lower or 'face_passport' in file_lower:
            records = self._parse_face_passport(content, file_name)
        elif 'ventilation_log' in file_lower or 'проветриван' in file_lower:
            records = self._parse_ventilation_log(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Объединяем с кэшем
        for record in records:
            premise_name = record.get('premise_name')
            if premise_name and premise_name in self.premises_cache:
                old_record = self.premises_cache[premise_name]
                old_record.update(record)
            elif premise_name:
                self.premises_cache[premise_name] = record
        
        return records
    
    def _parse_premises_list(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит список выработок (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Код', 'Наименование', 'Тип', 'Длина']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'код' in header:
                indices['code'] = idx
            elif 'наименован' in header or 'названи' in header:
                indices['name'] = idx
            elif 'тип' in header:
                indices['type'] = idx
            elif 'длина' in header:
                indices['length'] = idx
            elif 'сечени' in header:
                indices['section'] = idx
            elif 'горизонт' in header or 'отметк' in header:
                indices['level'] = idx
            elif 'x' in header or 'коорд' in header and 'x' in header.lower():
                indices['x'] = idx
            elif 'y' in header or 'коорд' in header and 'y' in header.lower():
                indices['y'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': None,
                'premise_type': None,
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': None,
                'length_m': None,
                'cross_section_m2': None,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            
            # premise_id (используем код из таблицы или генерируем)
            if 'code' in indices and indices['code'] < len(parts):
                record['premise_id'] = parts[indices['code']]
            
            # premise_name
            if 'name' in indices and indices['name'] < len(parts):
                record['premise_name'] = parts[indices['name']]
            
            # premise_type
            if 'type' in indices and indices['type'] < len(parts):
                record['premise_type'] = self._normalize_premise_type(parts[indices['type']])
            
            # length_m
            if 'length' in indices and indices['length'] < len(parts):
                record['length_m'] = self._to_float(parts[indices['length']])
            
            # cross_section_m2
            if 'section' in indices and indices['section'] < len(parts):
                record['cross_section_m2'] = self._to_float(parts[indices['section']])
            
            # level_m
            if 'level' in indices and indices['level'] < len(parts):
                record['level_m'] = self._to_int(parts[indices['level']])
            
            # coordinates
            if 'x' in indices and indices['x'] < len(parts):
                record['x_coordinate'] = self._to_float(parts[indices['x']])
            if 'y' in indices and indices['y'] < len(parts):
                record['y_coordinate'] = self._to_float(parts[indices['y']])
            
            if record['premise_name']:
                records.append(record)
        
        return records
    
    def _parse_face_passport(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит паспорт лавы"""
        records = []
        
        # Извлекаем информацию о лаве
        lava_match = re.search(r'ЛАВЫ\s+([0-9К-]+)', content, re.IGNORECASE)
        lava_name = lava_match.group(1) if lava_match else None
        
        if lava_name:
            # Длина лавы
            length_match = re.search(r'Длина лавы:\s*(\d+\.?\d*)\s*м', content)
            length = self._to_float(length_match.group(1)) if length_match else None
            
            # Сечение лавы
            section_match = re.search(r'Сечение лавы:\s*(\d+\.?\d*)\s*м²', content)
            section = self._to_float(section_match.group(1)) if section_match else None
            
            # Горизонт
            level_match = re.search(r'Горизонт:\s*[-]?(\d+)\s*м', content)
            level = self._to_int(level_match.group(1)) if level_match else None
            if level and level_match and '-' in level_match.group(0):
                level = -abs(level)
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': f"Лава {lava_name}",
                'premise_type': 'лава',
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': level,
                'length_m': length,
                'cross_section_m2': section,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            records.append(record)
        
        # Извлекаем информацию о вентиляционном штреке
        vsh_match = re.search(r'Вентиляционный штрек.*?Название:\s*([^\n]+)', content, re.DOTALL)
        if vsh_match:
            vsh_name = vsh_match.group(1).strip()
            
            length_match = re.search(r'Длина:\s*(\d+\.?\d*)\s*м', content)
            length = self._to_float(length_match.group(1)) if length_match else None
            
            section_match = re.search(r'Сечение:\s*(\d+\.?\d*)\s*м²', content)
            section = self._to_float(section_match.group(1)) if section_match else None
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': vsh_name,
                'premise_type': 'вентиляционный штрек',
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': None,
                'length_m': length,
                'cross_section_m2': section,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            records.append(record)
        
        # Извлекаем информацию о конвейерном штреке
        ksh_match = re.search(r'Конвейерный штрек.*?Название:\s*([^\n]+)', content, re.DOTALL)
        if ksh_match:
            ksh_name = ksh_match.group(1).strip()
            
            length_match = re.search(r'Длина:\s*(\d+\.?\d*)\s*м', content)
            length = self._to_float(length_match.group(1)) if length_match else None
            
            section_match = re.search(r'Сечение:\s*(\d+\.?\d*)\s*м²', content)
            section = self._to_float(section_match.group(1)) if section_match else None
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': ksh_name,
                'premise_type': 'конвейерный штрек',
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': None,
                'length_m': length,
                'cross_section_m2': section,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            records.append(record)
        
        # Извлекаем информацию о сбойках
        sboika_matches = re.finditer(r'Сбойка\s+№(\d+)[^:]*:\s*длина\s+(\d+\.?\d*)\s*м,\s*сечение\s+(\d+\.?\d*)\s*м²', content, re.IGNORECASE)
        for match in sboika_matches:
            sboika_num = match.group(1)
            length = self._to_float(match.group(2))
            section = self._to_float(match.group(3))
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': f"Сбойка №{sboika_num}",
                'premise_type': 'сбойка',
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': None,
                'length_m': length,
                'cross_section_m2': section,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    def _parse_ventilation_log(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит журнал проветривания (извлекает параметры выработок)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Выработка', 'Тип', 'Сечение', 'Длина']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'выработк' in header or 'наименован' in header:
                indices['name'] = idx
            elif 'тип' in header:
                indices['type'] = idx
            elif 'сечени' in header:
                indices['section'] = idx
            elif 'длина' in header:
                indices['length'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            record = {
                'premise_id': generate_premise_id(),
                'premise_name': None,
                'premise_type': None,
                'x_coordinate': None,
                'y_coordinate': None,
                'level_m': None,
                'length_m': None,
                'cross_section_m2': None,
                'company_id': self.COMPANY_ID,
                '_source_file': file_name
            }
            
            if 'name' in indices and indices['name'] < len(parts):
                record['premise_name'] = parts[indices['name']]
            
            if 'type' in indices and indices['type'] < len(parts):
                record['premise_type'] = self._normalize_premise_type(parts[indices['type']])
            
            if 'section' in indices and indices['section'] < len(parts):
                record['cross_section_m2'] = self._to_float(parts[indices['section']])
            
            if 'length' in indices and indices['length'] < len(parts):
                record['length_m'] = self._to_float(parts[indices['length']])
            
            # Определяем тип по названию, если не указан
            if not record['premise_type'] and record['premise_name']:
                record['premise_type'] = self._infer_premise_type(record['premise_name'])
            
            if record['premise_name']:
                records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг выработок"""
        records = []
        
        # Ищем упоминания выработок
        patterns = [
            r'(ВШ\s+[0-9К-]+)',
            r'(КШ\s+[0-9К-]+)',
            r'(лава\s+[0-9К-]+)',
            r'(сбойка\s+№\s*\d+)',
            r'(уклон\s+[0-9К-]+)',
        ]
        
        found_names = set()
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                if name not in found_names:
                    found_names.add(name)
                    record = {
                        'premise_id': generate_premise_id(),
                        'premise_name': name,
                        'premise_type': self._infer_premise_type(name),
                        'x_coordinate': None,
                        'y_coordinate': None,
                        'level_m': None,
                        'length_m': None,
                        'cross_section_m2': None,
                        'company_id': self.COMPANY_ID,
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records
    
    def _normalize_premise_type(self, premise_type: str) -> str:
        """Нормализует тип выработки"""
        if not premise_type:
            return 'None'
        
        type_lower = premise_type.lower()
        
        if 'лава' in type_lower or 'очистной забой' in type_lower:
            return 'лава'
        elif 'вентиляционный штрек' in type_lower or 'вш' in type_lower:
            return 'вентиляционный штрек'
        elif 'конвейерный штрек' in type_lower or 'кш' in type_lower:
            return 'конвейерный штрек'
        elif 'сбойк' in type_lower:
            return 'сбойка'
        elif 'уклон' in type_lower:
            return 'уклон'
        elif 'ствол' in type_lower:
            return 'ствол'
        elif 'камер' in type_lower:
            return 'камера'
        elif 'станц' in type_lower:
            return 'станция'
        else:
            return premise_type
    
    def _infer_premise_type(self, name: str) -> str:
        """Определяет тип выработки по названию"""
        name_upper = name.upper()
        
        if 'ЛАВА' in name_upper:
            return 'лава'
        elif 'ВШ' in name_upper or 'ВЕНТИЛЯЦИОННЫЙ' in name_upper:
            return 'вентиляционный штрек'
        elif 'КШ' in name_upper or 'КОНВЕЙЕРНЫЙ' in name_upper:
            return 'конвейерный штрек'
        elif 'СБОЙК' in name_upper:
            return 'сбойка'
        elif 'УКЛОН' in name_upper:
            return 'уклон'
        elif 'СТВОЛ' in name_upper:
            return 'ствол'
        elif 'КАМЕР' in name_upper:
            return 'камера'
        else:
            return 'выработка'
    
    def get_all_premises(self) -> List[Dict[str, Any]]:
        """Возвращает все выработки из кэша"""
        return list(self.premises_cache.values())
    
    def clear_cache(self) -> None:
        """Очищает кэш выработок"""
        self.premises_cache.clear()