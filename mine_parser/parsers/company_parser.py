# parsers/company_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_company_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class CompanyParser(BaseParser):
    """
    Парсер для описания предприятия.
    Поддерживает: описание предприятия, паспорта шахт, планы развития.
    """
    
    def __init__(self, config_path: str = "./config", incident_id: str = 'None'):
        super().__init__(config_path)
        self.set_table_name("company_description")
        self.incident_id = incident_id
        self._company_id = None
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга описания предприятия"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'company', 'описани', 'предприят', 'паспорт', 'шахт', 'mining_plan'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с описанием предприятия.
        Возвращает одну запись (объединяет данные из разных файлов).
        """
        file_lower = file_name.lower()
        
        if 'описани' in file_lower or 'company' in file_lower:
            record = self._parse_company_description(content, file_name)
        elif 'паспорт' in file_lower:
            record = self._parse_mine_passport(content, file_name)
        elif 'plan' in file_lower or 'план' in file_lower:
            record = self._parse_mining_plan(content, file_name)
        else:
            record = self._parse_generic(content, file_name)
        
        # Если запись уже существует, обновляем её
        if self._company_id and record:
            record['company_id'] = self._company_id
        
        if record and record.get('company_name'):
            self._company_id = record.get('company_id')
        
        return [record] if record else []
    
    def _parse_company_description(self, content: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит описание предприятия (таблица с | разделителем)"""
        record = {
            'company_id': generate_company_id(),
            'company_name': None,
            'mine_name': None,
            'year_commissioned': None,
            'annual_production_tons': None,
            'actual_production_tons': None,
            'depth_m': None,
            'employees_count': None,
            'gas_hazard_category': None,
            'outburst_hazard': None,
            '_source_file': file_name
        }
        
        lines = content.split('\n')
        in_table = False
        
        for line in lines:
            line = line.strip()
            if '|' not in line:
                continue
            
            # Проверяем, что это строка таблицы с параметрами
            if 'Параметр' in line and 'Значение' in line:
                in_table = True
                continue
            
            if in_table and '|' in line and not line.startswith('---'):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 2:
                    param = parts[0].lower()
                    value = parts[1]
                    
                    if 'полное наименование' in param or 'наименование предприятия' in param:
                        record['company_name'] = value
                    elif 'название шахты' in param or 'шахта' in param:
                        record['mine_name'] = value
                    elif 'год ввода' in param:
                        record['year_commissioned'] = self._to_int(value)
                    elif 'проектная мощность' in param:
                        record['annual_production_tons'] = self._extract_production(value)
                    elif 'фактическая добыча' in param:
                        record['actual_production_tons'] = self._extract_production(value)
                    elif 'глубина разработки' in param:
                        record['depth_m'] = self._extract_depth(value)
                    elif 'количество работающих' in param or 'численность' in param:
                        record['employees_count'] = self._to_int(value)
                    elif 'категория по газу' in param:
                        record['gas_hazard_category'] = value.lower()
                    elif 'опасность по выбросам' in param:
                        record['outburst_hazard'] = value
        
        # Если не нашли company_name, пробуем извлечь из текста
        if not record['company_name']:
            match = re.search(r'АО\s+["«]([^"»]+)["»]', content)
            if match:
                record['company_name'] = f"АО «{match.group(1)}»"
        
        if not record['mine_name']:
            match = re.search(r'Шахта:\s*([^\n]+)', content)
            if match:
                record['mine_name'] = match.group(1).strip()
        
        # Извлекаем краткую характеристику
        summary_match = re.search(r'Краткая характеристика:\s*\n(.*?)(?=\n={10,}|$)', content, re.DOTALL)
        if summary_match:
            record['description'] = TextCleaner.clean_whitespace(summary_match.group(1))
        
        return record
    
    def _parse_mine_passport(self, content: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит паспорт шахты"""
        record = {
            'company_id': generate_company_id(),
            'company_name': None,
            'mine_name': None,
            'year_commissioned': None,
            'annual_production_tons': None,
            'actual_production_tons': None,
            'depth_m': None,
            'employees_count': None,
            'gas_hazard_category': None,
            'outburst_hazard': None,
            'relative_gas_content': None,      # ← новое поле
            'absolute_gas_content': None,       # ← новое поле
            'workings_length_km': None,         # ← новое поле
            'production_faces_count': None,     # ← новое поле
            'development_faces_count': None,    # ← новое поле
            '_source_file': file_name
        }
        
        # Полное наименование предприятия
        match = re.search(r'Полное наименование предприятия:\s*([^\n]+)', content)
        if match:
            record['company_name'] = match.group(1).strip()
        
        # Наименование шахты
        match = re.search(r'Наименование шахты:\s*([^\n]+)', content)
        if match:
            record['mine_name'] = match.group(1).strip()
        
        # Год ввода
        match = re.search(r'Год ввода в эксплуатацию:\s*(\d{4})', content)
        if match:
            record['year_commissioned'] = self._to_int(match.group(1))
        
        # Проектная мощность
        match = re.search(r'Проектная мощность:\s*([\d\.]+)\s*(?:млн\s*тонн|млн\s*т|т/год)', content)
        if match:
            value = match.group(1)
            record['annual_production_tons'] = self._to_float(value) * 1000000 if value is not None and '.' in value else self._to_float(value)
        
        # Фактическая добыча
        match = re.search(r'Фактическая добыча за\s+\d{4}\s*год:\s*([\d\.]+)\s*(?:млн\s*тонн|млн\s*т|т/год)', content)
        if match:
            value = match.group(1)
            record['actual_production_tons'] = self._to_float(value) * 1000000 if '.' in value else self._to_float(value)
        
        # Глубина разработки
        match = re.search(r'Глубина разработки:\s*(\d+)-(\d+)\s*м', content)
        if match:
            min_depth = self._to_int(match.group(1))
            max_depth = self._to_int(match.group(2))
            if min_depth and max_depth:
                record['depth_m'] = (min_depth + max_depth) // 2
        
        # Количество работающих
        match = re.search(r'Общая численность:\s*(\d+)\s*человек', content)
        if match:
            record['employees_count'] = self._to_int(match.group(1))
        
        # Рабочие и ИТР
        match = re.search(r'Рабочих:\s*(\d+)\s*человек', content)
        if match:
            record['workers_count'] = self._to_int(match.group(1))
        
        match = re.search(r'ИТР:\s*(\d+)\s*человек', content)
        if match:
            record['engineers_count'] = self._to_int(match.group(1))
        
        # Категория по газу
        match = re.search(r'Категория шахты:\s*([^\n]+)', content)
        if match:
            record['gas_hazard_category'] = match.group(1).strip().lower()
        
        # Газоносность
        match = re.search(r'Относительная газоносность:\s*([\d\.]+)\s*м³/т', content)
        if match:
            record['relative_gas_content'] = self._to_float(match.group(1))
        
        match = re.search(r'Абсолютная газоносность:\s*([\d\.]+)\s*м³/мин', content)
        if match:
            record['absolute_gas_content'] = self._to_float(match.group(1))
        
        # Протяженность выработок
        match = re.search(r'Протяженность выработок:\s*(\d+)\s*км', content)
        if match:
            record['workings_length_km'] = self._to_float(match.group(1))
        
        # Количество забоев
        match = re.search(r'Количество очистных забоев:\s*(\d+)', content)
        if match:
            record['production_faces_count'] = self._to_int(match.group(1))
        
        match = re.search(r'Количество подготовительных забоев:\s*(\d+)', content)
        if match:
            record['development_faces_count'] = self._to_int(match.group(1))
        
        # Опасность по выбросам
        outburst_parts = []
        if re.search(r'Пласт\s+К3.*?опасен', content, re.IGNORECASE):
            outburst_parts.append("пласт К3")
        if re.search(r'Пласт\s+К2.*?опасен', content, re.IGNORECASE):
            outburst_parts.append("пласт К2")
        
        if outburst_parts:
            record['outburst_hazard'] = f"опасна по внезапным выбросам ({', '.join(outburst_parts)})"
        elif re.search(r'опасн', content, re.IGNORECASE):
            record['outburst_hazard'] = "опасна по внезапным выбросам"
        else:
            record['outburst_hazard'] = "не опасна"
        
        return record
    
    def _parse_mining_plan(self, content: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит план развития горных работ"""
        record = {
            'company_id': generate_company_id(),
            'company_name': None,
            'mine_name': None,
            'year_commissioned': None,
            'annual_production_tons': None,
            'actual_production_tons': None,
            'depth_m': None,
            'employees_count': None,
            'gas_hazard_category': None,
            'outburst_hazard': None,
            'avg_daily_load_lava_48': None,      # ← новое поле
            'avg_daily_load_lava_42': None,      # ← новое поле
            'production_9months': None,           # ← новое поле
            '_source_file': file_name
        }
        
        # Ищем в таблице
        lines = content.split('\n')
        in_table = False
        
        for line in lines:
            if '|' in line:
                if 'Показатель' in line and 'Значение' in line:
                    in_table = True
                    continue
                
                if in_table and '|' in line and not line.strip().startswith('---'):
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 2:
                        param = parts[0].lower()
                        value = parts[1]
                        
                        if 'проектная мощность шахты' in param:
                            record['annual_production_tons'] = self._extract_production(value)
                        elif 'плановая добыча' in param:
                            record['actual_production_tons'] = self._extract_production(value)
                        elif 'среднесуточная нагрузка на лаву 48к3-з' in param:
                            match = re.search(r'(\d+)-(\d+)', value)
                            if match:
                                record['avg_daily_load_lava_48'] = f"{match.group(1)}-{match.group(2)}"
                        elif 'среднесуточная нагрузка на лаву 42к2-в' in param:
                            match = re.search(r'(\d+)-(\d+)', value)
                            if match:
                                record['avg_daily_load_lava_42'] = f"{match.group(1)}-{match.group(2)}"
                        elif 'год ввода' in param:
                            record['year_commissioned'] = self._to_int(value)
                        elif 'категория' in param:
                            record['gas_hazard_category'] = value.lower()
                        elif 'количество работающих' in param:
                            record['employees_count'] = self._to_int(value)
        
        # Фактическая добыча за 9 месяцев
        match = re.search(r'Фактическая добыча за 9 месяцев 2023:\s*([\d\s]+)\s*т', content)
        if match:
            value = match.group(1).replace(' ', '')
            record['production_9months'] = self._to_int(value)
        
        # Название шахты
        match = re.search(r'Шахта:\s*([^\n]+)', content)
        if match:
            record['mine_name'] = match.group(1).strip()
        
        return record
    
    def _parse_generic(self, content: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Универсальный парсинг описания предприятия"""
        record = {
            'company_id': generate_company_id(),
            'company_name': None,
            'mine_name': None,
            'year_commissioned': None,
            'annual_production_tons': None,
            'actual_production_tons': None,
            'depth_m': None,
            'employees_count': None,
            'gas_hazard_category': None,
            'outburst_hazard': None,
            '_source_file': file_name
        }
        
        # Название шахты
        match = re.search(r'им\.\s*Костенко', content)
        if match:
            record['mine_name'] = 'им. Костенко'
        
        # Компания
        match = re.search(r'АО\s+["«]([^"»]+)["»]', content)
        if match:
            record['company_name'] = f"АО «{match.group(1)}»"
        
        # Год ввода
        match = re.search(r'(\d{4})\s*г(?:од)?(?:\s*ввода|\s*основания)', content)
        if match:
            record['year_commissioned'] = self._to_int(match.group(1))
        
        # Категория по газу
        if 'сверхкатегорийная' in content.lower():
            record['gas_hazard_category'] = 'сверхкатегорийная'
        
        return record
    
    def _extract_production(self, value: str) -> Optional[float]:
        """Извлекает значение добычи из строки"""
        if not value:
            return None
        
        # Форматы: "2.5 млн тонн/год", "2500000 т/год", "1.82 млн тонн"
        match = re.search(r'([\d\.]+)\s*(?:млн|миллионов?)\s*(?:тонн|т)', value, re.IGNORECASE)
        if match:
            return self._to_float(match.group(1)) * 1000000
        
        match = re.search(r'([\d\.]+)\s*т/год', value)
        if match:
            return self._to_float(match.group(1))
        
        match = re.search(r'([\d]+)\s*т', value)
        if match:
            return self._to_float(match.group(1))
        
        return None
    
    def _extract_depth(self, value: str) -> Optional[int]:
        """Извлекает глубину разработки из строки"""
        if not value:
            return None
        
        match = re.search(r'(\d+)-(\d+)\s*м', value)
        if match:
            min_depth = self._to_int(match.group(1))
            max_depth = self._to_int(match.group(2))
            if min_depth and max_depth:
                return (min_depth + max_depth) // 2
        
        match = re.search(r'(\d+)\s*м', value)
        if match:
            return self._to_int(match.group(1))
        
        return None