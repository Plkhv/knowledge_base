# parsers/affected_areas_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_affected_area_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser


class AffectedAreasParser(BaseParser):
    """
    Парсер для зон поражения.
    Поддерживает: моделирование взрывов, акты осмотра, дневники осмотра.
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("affected_areas")
    
    def supports(self, file_name: str) -> bool:
        """Проверяет, подходит ли файл для парсинга зон поражения"""
        name_lower = file_name.lower()
        return any(keyword in name_lower for keyword in [
            'affected', 'поражен', 'explosion_model', 'inspection_act', 'inspection_diary'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с информацией о зонах поражения.
        """
        file_lower = file_name.lower()
        
        if 'explosion_model' in file_lower:
            records = self._parse_explosion_model(content, file_name)
        elif 'inspection_act' in file_lower:
            records = self._parse_inspection_act(content, file_name)
        elif 'inspection_diary' in file_lower:
            records = self._parse_inspection_diary(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Добавляем incident_id
        for record in records:
            record['incident_id'] = self.INCIDENT_ID
        
        return records
    
    def _parse_explosion_model(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        # Извлекаем название лавы из заголовка
        lava_match = re.search(r'Лава\s+([0-9К-]+)', content)
        lava_name = lava_match.group(1) if lava_match else "Unknown"
        
        # Извлекаем концентрацию CH4
        ch4_match = re.search(r'Концентрация CH4:\s*([\d\.]+)%', content)
        ch4_value = ch4_match.group(1) if ch4_match else "N/A"
        
        # Извлекаем секции из "ЗОНА ПОРАЖЕНИЯ" или "вывод"
        sections_match = re.search(r'секции\s+(\d+)-(\d+)', content)
        
        # Первичный очаг (из вывода)
        primary_record = {
            'area_id': generate_affected_area_id(),
            'incident_id': self.INCIDENT_ID,  
            'premise_id': f"Лава {lava_name}",
            'damage_type': 'первичный взрыв',
            'damage_description': f"очаг взрыва, концентрация CH4 {ch4_value}%",
            'is_primary_blast_zone': 1,
            'geo_metca': f"секции {sections_match.group(1)}-{sections_match.group(2)}" if sections_match else None,
            '_source_file': file_name
        }
        records.append(primary_record)
        
        # Парсим зоны поражения из списка
        in_zone = False
        for line in content.split('\n'):
            if 'ЗОНА ПОРАЖЕНИЯ' in line:
                in_zone = True
                continue
            if 'ЗОНА ВНЕ ПОРАЖЕНИЯ' in line:
                break
            if in_zone and re.match(r'^\d+\.', line.strip()):
                # Извлекаем название выработки
                premise_match = re.match(r'^\d+\.\s*(.+)', line.strip())
                if premise_match:
                    premise_name = premise_match.group(1).strip()
                    record = {
                        'area_id': generate_affected_area_id(),
                        'incident_id': self.INCIDENT_ID,  # ❌ хардкод
                        'premise_id': premise_name,
                        'damage_type': 'поражение ударной волной',
                        'damage_description': f"зона поражения: {premise_name}",
                        'is_primary_blast_zone': 0,
                        'geo_metca': None,
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records
    
    def _parse_inspection_act(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        # Извлекаем дату осмотра
        date_match = re.search(r'Дата осмотра:\s*(\d{2})\s*(\w+)\s*(\d{4})', content)
        inspection_date = None
        if date_match:
            day, month_name, year = date_match.groups()
            month_map = {'ноября': '11', 'октября': '10', 'сентября': '09'}
            month = month_map.get(month_name.lower(), '01')
            inspection_date = f"{year}-{month}-{day.zfill(2)}"
        
        # Извлекаем членов комиссии
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
            if in_commission and line.strip() and not line.strip().startswith('1.'):
                in_commission = False
        
        inspector_name = '; '.join(inspectors) if inspectors else None
        
        # Парсим результаты осмотра
        results_section = re.search(r'РЕЗУЛЬТАТЫ ОСМОТРА:(.*?)(?=ФОТОФИКСАЦИЯ|ЗАКЛЮЧЕНИЕ|$)', content, re.DOTALL)
        if results_section:
            results_text = results_section.group(1)
            
            # Разбиваем по пунктам (1., 2., 3., 4.)
            items = re.split(r'\n\d+\.\s+', results_text)
            
            for item in items:
                if not item.strip():
                    continue
                
                # Определяем локацию по началу пункта
                if 'Лава' in item:
                    current_location = 'Лава 48 К3-з'
                elif 'Конвейерный штрек' in item:
                    current_location = 'Конвейерный штрек 48 К3-з'
                elif 'Вентиляционный штрек' in item:
                    current_location = 'Вентиляционный штрек 48 К3-з'
                elif 'Выработанное пространство' in item:
                    current_location = 'Выработанное пространство'
                else:
                    continue
                
                # Разбиваем на подпункты с "-"
                sub_items = re.split(r'\n\s*-\s+', item)
                for sub_item in sub_items:
                    if not sub_item.strip():
                        continue
                    
                    # Определяем тип повреждения
                    damage_type = self._infer_damage_type_from_text(sub_item)
                    
                    # Извлекаем конкретное оборудование
                    premise_id = self._extract_premise_from_text(sub_item, current_location)
                    
                    record = {
                        'area_id': generate_affected_area_id(),
                        'incident_id': self.INCIDENT_ID,  # ❌ хардкод
                        'premise_id': premise_id,
                        'damage_type': damage_type,
                        'damage_description': sub_item.strip(),
                        'is_primary_blast_zone': 1 if 'секции 138-148' in sub_item or 'верхняя' in sub_item else 0,
                        'geo_metca': self._extract_sections(sub_item),
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records

    def _extract_premise_from_text(self, text: str, default_location: str) -> str:
        """Извлекает конкретное оборудование из текста"""
        if 'комбайн' in text.lower():
            match = re.search(r'Комбайн\s+([A-Z0-9/.-]+)', text, re.IGNORECASE)
            return match.group(1) if match else 'Комбайн FS300/1.0'
        elif 'шлифовальн' in text.lower():
            match = re.search(r'шлифовальн[ая]+.*?(?:тип\s+)?([A-Z0-9-]+)', text, re.IGNORECASE)
            return match.group(1) if match else 'Шлифовальная машинка ИП-2014Б'
        elif 'перемычка' in text.lower():
            match = re.search(r'Перемычка\s+№(\d+)', text, re.IGNORECASE)
            return f"Перемычка №{match.group(1)}" if match else default_location
        elif 'секци' in text.lower():
            match = re.search(r'секциях?\s+(\d+)-(\d+)', text, re.IGNORECASE)
            if match:
                return f"Лава 48 К3-з (секции {match.group(1)}-{match.group(2)})"
        return default_location

    def _infer_damage_type_from_text(self, text: str) -> str:
        """Определяет тип повреждения по тексту"""
        text_lower = text.lower()
        if 'термические повреждения' in text_lower or 'оплавление' in text_lower:
            return 'термические повреждения'
        elif 'царапины' in text_lower or 'износ' in text_lower or 'ржавчина' in text_lower:
            return 'механические повреждения'
        elif 'деформирована' in text_lower:
            return 'деформация'
        elif 'трещины' in text_lower:
            return 'геомеханические нарушения'
        else:
            return 'повреждения'

    def _extract_sections(self, text: str) -> Optional[str]:
        """Извлекает номера секций из текста"""
        match = re.search(r'секци[яи]\s+(\d+)-(\d+)', text, re.IGNORECASE)
        if match:
            return f"секции {match.group(1)}-{match.group(2)}"
        return None
    
    def _parse_inspection_diary(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        # Извлекаем имя эксперта
        expert_match = re.search(r'Эксперт:\s*([А-Я][а-я]+)\s+([А-Я]\.\s*[А-Я]\.)', content)
        expert_name = f"{expert_match.group(1)} {expert_match.group(2)}" if expert_match else None
        
        # Разбиваем на спуски
        descent_pattern = r'--- СПУСК №\d+ \((\d{2}\.\d{2}\.\d{4})\) ---\s*\n(.*?)(?=--- СПУСК|---\s*$|\Z)'
        descents = re.findall(descent_pattern, content, re.DOTALL)
        
        for descent_date_str, descent_content in descents:
            inspection_date = DateParser.parse_to_str(descent_date_str)
            
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
                    location = self._extract_premise_from_diary_fact(fact)
                    if not location and route:
                        location = route
                    
                    # Определяем тип повреждения
                    damage_type = self._infer_damage_type_from_text(fact)
                    
                    # Извлекаем premise_id
                    premise_id = self._extract_premise_from_diary_fact(fact)
                    
                    record = {
                        'area_id': generate_affected_area_id(),
                        'incident_id': self.INCIDENT_ID,  # ❌ хардкод
                        'premise_id': premise_id,
                        'damage_type': damage_type,
                        'damage_description': fact,
                        'is_primary_blast_zone': 1 if 'секции 138-148' in fact or 'секций 138-148' in fact else 0,
                        'geo_metca': self._extract_sections(fact),
                        '_source_file': file_name
                    }
                    records.append(record)
        
        return records

    def _extract_premise_from_diary_fact(self, text: str) -> str:
        """Извлекает название выработки/оборудования из факта дневника"""
        if 'Комбайн' in text:
            match = re.search(r'Комбайн\s+([A-Z0-9/.-]+)', text, re.IGNORECASE)
            return match.group(1) if match else 'Комбайн FS300'
        elif 'Конвейер' in text:
            match = re.search(r'Конвейер\s+([A-Z0-9/.-]+)', text, re.IGNORECASE)
            return match.group(1) if match else 'Конвейер FFC-8'
        elif 'секци' in text.lower():
            match = re.search(r'Секции\s+(\d+)-(\d+)', text)
            if match:
                return f"Лава 48 К3-з (секции {match.group(1)}-{match.group(2)})"
        elif 'шлифовальн' in text.lower():
            return 'Шлифовальная машинка ИП-2014Б'
        elif 'за секциями' in text.lower():
            return 'Выработанное пространство'
        return 'Лава 48 К3-з'
    

    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг зон поражения"""
        records = []
        
        # Ищем упоминания выработок с повреждениями
        patterns = [
            (r'(Лава\s+[0-9К-]+)', 'лава'),
            (r'(КШ\s+[0-9К-]+)', 'конвейерный штрек'),
            (r'(ВШ\s+[0-9К-]+)', 'вентиляционный штрек'),
            (r'(Сбойка\s+№\d+)', 'сбойка'),
        ]
        
        for pattern, premise_type in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                record = {
                    'area_id': generate_affected_area_id(),
                    'incident_id': self.INCIDENT_ID,
                    'premise_id': match,
                    'damage_type': 'повреждения',
                    'damage_description': 'выработка находится в зоне поражения',
                    'is_primary_blast_zone': 0,
                    'geo_metca': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records
    
    def _infer_damage_type(self, premise_name: str, description: str|None) -> str:
        """Определяет тип повреждения по названию и описанию"""
        if 'лава' in premise_name.lower():
            return 'взрывные повреждения'
        elif 'штрек' in premise_name.lower():
            return 'повреждения крепи'
        elif 'сбойка' in premise_name.lower():
            return 'обрушение'
        else:
            return 'повреждения'