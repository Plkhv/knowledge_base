# parsers/inspection_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from base_parser import BaseParser
from id_generator import generate_inspection_fact_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class InspectionParser(BaseParser):
    """
    Парсер для описания осмотров места происшествия.
    Поддерживает: акты осмотра, дневники осмотра.
    """
    
    def __init__(self, config_path: str = "./config", incident_id: str = 'None'):
        super().__init__(config_path)
        self.set_table_name("inspection_description")
        self.incident_id = incident_id  # ← теперь передается извне
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга осмотров"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'inspection_act', 'inspection_diary', 'осмотр', 'акт осмотра'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит файл с описанием осмотра."""
        file_lower = file_name.lower()
        
        if 'inspection_act' in file_lower:
            records = self._parse_inspection_act(content, file_name)
        elif 'inspection_diary' in file_lower:
            records = self._parse_inspection_diary(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Добавляем incident_id
        for record in records:
            record['incident_id'] = self.incident_id
        
        return records
    
    def _parse_inspection_act(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит акт осмотра"""
        records = []
        
        # Извлекаем дату осмотра
        inspection_date = self._extract_date(content)
        
        # Извлекаем членов комиссии (инспекторов)
        inspectors = self._extract_inspectors(content)
        inspector_name = '; '.join(inspectors) if inspectors else None
        
        # Извлекаем осмотренные выработки
        locations = self._extract_locations(content)
        
        # Парсим результаты осмотра по пунктам
        results_section = re.search(r'РЕЗУЛЬТАТЫ ОСМОТРА:(.*?)(?=ФОТОФИКСАЦИЯ|ЗАКЛЮЧЕНИЕ|$)', content, re.DOTALL)
        if results_section:
            results_text = results_section.group(1)
            
            # Разбиваем по пунктам (1., 2., 3., 4.)
            items = re.split(r'\n\d+\.\s+', results_text)
            
            current_location = None
            for item in items:
                if not item.strip():
                    continue
                
                # Определяем локацию по началу пункта
                current_location = self._extract_location_from_header(item, current_location)
                
                # Разбиваем на отдельные замечания по маркерам "-"
                sub_items = re.split(r'\n\s*-\s+', item)
                
                for sub_item in sub_items:
                    if not sub_item.strip():
                        continue
                    
                    fact = sub_item.strip()
                    if len(fact) < 10:
                        continue
                    
                    # Определяем, является ли это нарушением
                    violations = self._extract_violations(fact)
                    
                    # Извлекаем конкретное оборудование
                    equipment = self._extract_equipment_from_fact(fact)
                    
                    record = {
                        'fact_id': generate_inspection_fact_id(),
                        'incident_id': self.incident_id,
                        'inspection_date': inspection_date,
                        'fact_description': fact,
                        'inspector_name': inspector_name,
                        'location': current_location,
                        'condition_description': self._extract_condition_description(fact),
                        'violations_found': violations,
                        'equipment_name': equipment,
                        '_source_file': file_name
                    }
                    records.append(record)
        
        # Добавляем информацию из заключения
        conclusion = self._extract_conclusion(content)
        if conclusion:
            record = {
                'fact_id': generate_inspection_fact_id(),
                'incident_id': self.incident_id,
                'inspection_date': inspection_date,
                'fact_description': f"ЗАКЛЮЧЕНИЕ: {conclusion}",
                'inspector_name': inspector_name,
                'location': 'общее',
                'condition_description': None,
                'violations_found': None,
                'equipment_name': None,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    def _parse_inspection_diary(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит дневник осмотра"""
        records = []
        
        # Извлекаем имя эксперта
        expert_name = self._extract_expert_name(content)
        
        # Разбиваем на спуски
        descent_pattern = r'--- СПУСК №\d+ \((\d{2}\.\d{2}\.\d{4})\) ---\s*\n(.*?)(?=--- СПУСК|---\s*$|\Z)'
        descents = re.findall(descent_pattern, content, re.DOTALL)
        
        for descent_date_str, descent_content in descents:
            inspection_date = self._parse_date_from_str(descent_date_str)
            
            # Извлекаем время
            time_match = re.search(r'Время:\s*([\d:]+)-([\d:]+)', descent_content)
            
            # Извлекаем маршрут
            route_match = re.search(r'Маршрут:\s*([^\n]+)', descent_content)
            route = route_match.group(1) if route_match else None
            
            # Извлекаем осмотренные объекты
            inspected_section = re.search(r'Осмотрено:\s*\n(.*?)(?=Изъято|Фото|---|$)', descent_content, re.DOTALL)
            if inspected_section:
                items = inspected_section.group(1).strip().split('\n')
                for item in items:
                    item = item.strip()
                    if not item or not item.startswith('-'):
                        continue
                    
                    fact = item[1:].strip()
                    
                    # Определяем локацию
                    location = self._extract_location_from_diary_fact(fact)
                    if not location and route:
                        location = route
                    
                    # Извлекаем нарушения
                    violations = self._extract_violations(fact)
                    
                    # Извлекаем оборудование
                    equipment = self._extract_equipment_from_fact(fact)
                    
                    record = {
                        'fact_id': generate_inspection_fact_id(),
                        'incident_id': self.incident_id,
                        'inspection_date': inspection_date,
                        'fact_description': fact,
                        'inspector_name': expert_name,
                        'location': location,
                        'condition_description': self._extract_condition_description(fact),
                        'violations_found': violations,
                        'equipment_name': equipment,
                        '_source_file': file_name
                    }
                    records.append(record)
            
            # Извлекаем информацию об изъятом
            seized_match = re.search(r'Изъято:\s*([^\n]+)', descent_content)
            if seized_match:
                record = {
                    'fact_id': generate_inspection_fact_id(),
                    'incident_id': self.incident_id,
                    'inspection_date': inspection_date,
                    'fact_description': f"Изъято: {seized_match.group(1)}",
                    'inspector_name': expert_name,
                    'location': route,
                    'condition_description': None,
                    'violations_found': None,
                    'equipment_name': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        # Добавляем общий вывод
        general_conclusion = self._extract_general_conclusion(content)
        if general_conclusion:
            record = {
                'fact_id': generate_inspection_fact_id(),
                'incident_id': self.incident_id,
                'inspection_date': None,
                'fact_description': f"ОБЩИЙ ВЫВОД: {general_conclusion}",
                'inspector_name': expert_name,
                'location': 'общее',
                'condition_description': None,
                'violations_found': None,
                'equipment_name': None,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг осмотров"""
        records = []
        
        # Ищем дату
        inspection_date = self._extract_date(content)
        
        # Ищем имена инспекторов
        inspector_names = self._extract_inspectors(content)
        inspector_name = '; '.join(inspector_names) if inspector_names else None
        
        # Ищем факты с маркерами
        facts = re.findall(r'[-•]\s*([А-Я][^.\n]+[.!]?)', content)
        
        for fact in facts:
            fact = fact.strip()
            if len(fact) < 10:
                continue
            
            location = self._extract_location_from_fact(fact)
            violations = self._extract_violations(fact)
            equipment = self._extract_equipment_from_fact(fact)
            
            record = {
                'fact_id': generate_inspection_fact_id(),
                'incident_id': self.incident_id,
                'inspection_date': inspection_date,
                'fact_description': fact,
                'inspector_name': inspector_name,
                'location': location,
                'condition_description': self._extract_condition_description(fact),
                'violations_found': violations,
                'equipment_name': equipment,
                '_source_file': file_name
            }
            records.append(record)
        
        return records
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    def _extract_date(self, content: str) -> Optional[str]:
        """Извлекает дату осмотра"""
        patterns = [
            r'Дата осмотра:\s*(\d{2})\s*(\w+)\s*(\d{4})',
            r'Дата осмотра:\s*(\d{2}\.\d{2}\.\d{4})',
            r'(\d{2}\.\d{2}\.\d{4})',
        ]
        
        month_map = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                if '.' in pattern:
                    return match.group(1)
                elif len(match.groups()) == 3:
                    day, month_name, year = match.groups()
                    month = month_map.get(month_name.lower(), '01')
                    return f"{year}-{month}-{day.zfill(2)}"
        return None
    
    def _parse_date_from_str(self, date_str: str) -> Optional[str]:
        """Парсит дату из строки вида '04.11.2023'"""
        match = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
        if match:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
        return None
    
    def _extract_inspectors(self, content: str) -> List[str]:
        """Извлекает членов комиссии"""
        inspectors = []
        in_commission = False
        
        for line in content.split('\n'):
            if 'Члены подкомиссии:' in line:
                in_commission = True
                continue
            if in_commission and re.match(r'^\d+\.', line.strip()):
                match = re.match(r'^\d+\.\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)', line)
                if match:
                    inspectors.append(f"{match.group(1)} {match.group(2)}")
            if in_commission and line.strip() and not line.strip().startswith(('1.', '2.', '3.')):
                in_commission = False
        
        return inspectors
    
    def _extract_expert_name(self, content: str) -> Optional[str]:
        """Извлекает имя эксперта из дневника"""
        match = re.search(r'Эксперт:\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)', content)
        if match:
            return f"{match.group(1)} {match.group(2)}"
        return None
    
    def _extract_locations(self, content: str) -> List[str]:
        """Извлекает осмотренные выработки"""
        locations = []
        location_section = re.search(r'Осмотрены выработки:(.*?)(?=РЕЗУЛЬТАТЫ)', content, re.DOTALL)
        if location_section:
            for line in location_section.group(1).split('\n'):
                line = line.strip()
                if line and line.startswith('-'):
                    locations.append(line[1:].strip())
        return locations
    
    def _extract_location_from_header(self, text: str, current_location: Optional[str]) -> str:
        """Определяет локацию по заголовку пункта"""
        if 'Лава' in text:
            return 'Лава 48 К3-з'
        elif 'Конвейерный штрек' in text:
            return 'Конвейерный штрек 48 К3-з'
        elif 'Вентиляционный штрек' in text:
            return 'Вентиляционный штрек 48 К3-з'
        elif 'Выработанное пространство' in text:
            return 'Выработанное пространство'
        return current_location or 'не указано'
    
    def _extract_location_from_fact(self, fact: str) -> Optional[str]:
        """Извлекает локацию из факта"""
        patterns = [
            r'(Лава\s+[0-9К-]+)',
            r'(КШ\s+[0-9К-]+)',
            r'(ВШ\s+[0-9К-]+)',
            r'(Сбойка\s+№\d+)',
            r'(секция\w*\s+\d+(?:-\d+)?)',
            r'(Камера\s+[A-Za-z0-9]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, fact, re.IGNORECASE)
            if match:
                return match.group(1)
        return None
    
    def _extract_location_from_diary_fact(self, fact: str) -> Optional[str]:
        """Извлекает локацию из факта дневника"""
        if 'секции' in fact.lower():
            return 'Лава 48 К3-з'
        elif 'за секциями' in fact.lower():
            return 'Выработанное пространство'
        elif 'камера' in fact.lower():
            return 'Камера комбайна'
        return None
    
    def _extract_equipment_from_fact(self, fact: str) -> Optional[str]:
        """Извлекает название оборудования из факта"""
        equipment_patterns = [
            (r'Комбайн\s+([A-Z0-9/.-]+)', 'комбайн'),
            (r'Конвейер\s+([A-Z0-9/.-]+)', 'конвейер'),
            (r'шлифовальн[ая]+\s+машинк[аи]', 'шлифовальная машинка'),
            (r'ПУР-патрон[аы]?', 'ПУР-патроны'),
            (r'Перемычка\s+№(\d+)', 'перемычка'),
            (r'аэрозольн[ая]+\s+баллон', 'аэрозольный баллон'),
        ]
        
        for pattern, eq_type in equipment_patterns:
            match = re.search(pattern, fact, re.IGNORECASE)
            if match:
                if eq_type == 'перемычка' and match.group(1):
                    return f"Перемычка №{match.group(1)}"
                elif eq_type in ['комбайн', 'конвейер'] and match.group(1):
                    return f"{eq_type} {match.group(1)}"
                return eq_type
        return None
    
    def _extract_violations(self, fact: str) -> Optional[str]:
        """Извлекает выявленные нарушения из факта"""
        violation_keywords = [
            'износ', 'ржавчина', 'деформирована', 'повреждени', 'трещин',
            'царапины', 'оплавлени', 'выгорание', 'срезанн', 'нарушен'
        ]
        
        violations = []
        for keyword in violation_keywords:
            if keyword in fact.lower():
                sentences = re.split(r'[.!?]', fact)
                for sent in sentences:
                    if keyword in sent.lower():
                        violations.append(sent.strip())
                        break
        
        if violations:
            return '; '.join(violations[:3])
        return None
    
    def _extract_condition_description(self, fact: str) -> Optional[str]:
        """Извлекает описание состояния из факта"""
        condition_keywords = [
            'чисто', 'целые', 'исправн', 'работоспособн',
            'следов нет', 'без повреждений', 'не нарушена'
        ]
        
        for keyword in condition_keywords:
            if keyword in fact.lower():
                return fact.strip()
        return None
    
    def _extract_conclusion(self, content: str) -> Optional[str]:
        """Извлекает заключение из акта"""
        match = re.search(r'ЗАКЛЮЧЕНИЕ:\s*\n(.+?)(?=\nПодписи|$)', content, re.DOTALL)
        if match:
            return match.group(1).strip()
        return None
    
    def _extract_general_conclusion(self, content: str) -> Optional[str]:
        """Извлекает общий вывод из дневника"""
        match = re.search(r'Общий вывод:\s*([^\n]+)', content)
        if match:
            return match.group(1).strip()
        return None