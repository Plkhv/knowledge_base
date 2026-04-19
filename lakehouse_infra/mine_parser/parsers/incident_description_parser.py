# parsers/incident_description_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_incident_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser  # ← используем существующий DateParser


class IncidentDescriptionParser(BaseParser):
    """
    Парсер для общего описания инцидента.
    Использует mawo-natasha для извлечения локаций и сущностей.
    """
    
    def __init__(self, config_path: str = "./config", incident_id: str = 'None'):
        super().__init__(config_path, incident_id)
        self.set_table_name("incident_description")
        self._init_mawo()
    
    def _init_mawo(self):
        """Инициализирует mawo-natasha"""
        try:
            from mawo_natasha import MAWODoc
            from mawo_slovnet import NewsNERTagger
            self.MAWODoc = MAWODoc
            self.ner_tagger = NewsNERTagger()
            self.mawo_available = True
        except ImportError:
            self.mawo_available = False
            print("Warning: mawo-natasha not installed")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in ['incident_summary', 'описание', 'сводка'])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        inc_id = self.incident_id if self.incident_id else generate_incident_id()
        
        # Извлекаем полную дату+время
        incident_datetime = self._extract_datetime(content)
        
        record = {
            'incident_id': inc_id,
            'incident_date': incident_datetime[:10] if incident_datetime else None,  # YYYY-MM-DD
            'incident_time': incident_datetime[11:] if incident_datetime else None,  # HH:MM:SS
            'mine_name': self._extract_mine_name(content),
            'incident_type': self._extract_incident_type(content),
            'fatalities': self._extract_fatalities(content),
            'injuries': self._extract_injuries(content),
            'material_damage': self._extract_material_damage(content),
            'brief_description': self._extract_description(content),
            'extracted_locations': self._extract_locations_mawo(content),
            'extracted_entities': self._extract_entities_mawo(content),
            '_source_file': file_name
        }
        
        # Очищаем строковые поля
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = TextCleaner.clean_whitespace(value)
        
        return [record]
    
    def _extract_datetime(self, content: str) -> Optional[str]:
        """Извлекает дату и время из текста, возвращает 'YYYY-MM-DD HH:MM:SS'"""
        
        # Сначала ищем явные поля
        date_match = re.search(r'Дата аварии:\s*(\d{2}\.\d{2}\.\d{4})', content)
        time_match = re.search(r'Время аварии:\s*(\d{2}:\d{2}:\d{2})', content)
        
        if date_match and time_match:
            date_str = date_match.group(1)
            time_str = time_match.group(1)
            # Парсим дату через DateParser
            dt = DateParser.parse(date_str)
            if dt:
                return f"{dt.strftime('%Y-%m-%d')} {time_str}"
        
        # Пробуем извлечь из текста: "28 октября 2023 года в 02:43:49"
        match = re.search(r'(\d{2})\s+(\w+)\s+(\d{4})\s*года?\s+в\s+(\d{2}:\d{2}:\d{2})', content)
        if match:
            day, month_name, year, time_str = match.groups()
            date_str = f"{day} {month_name} {year}"
            dt = DateParser.parse(date_str)
            if dt:
                return f"{dt.strftime('%Y-%m-%d')} {time_str}"
        
        # Только дата
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', content)
        if date_match:
            dt = DateParser.parse(date_match.group(1))
            if dt:
                return f"{dt.strftime('%Y-%m-%d')} 00:00:00"
        
        return None
    
    def _extract_mine_name(self, content: str) -> str:
        match = re.search(r'Место:\s*([^\n]+)', content)
        if match:
            return match.group(1).strip()
        
        match = re.search(r'на\s+(шахте\s+[^\n,\.]+)', content)
        if match:
            return match.group(1).strip()
        
        return 'им. Костенко'
    
    def _extract_incident_type(self, content: str) -> Optional[str]:
        match = re.search(r'Тип:\s*([^\n]+)', content)
        if match:
            return match.group(1).strip()
        
        match = re.search(r'произошел\s+([а-яё\s-]+?)(?:\s+в\s+|\.)', content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None
    
    def _extract_fatalities(self, content: str) -> Optional[int]:
        match = re.search(r'Количество погибших:\s*(\d+)', content)
        if match:
            return int(match.group(1))
        
        match = re.search(r'погибли\s+(\d+)\s+человек', content)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_injuries(self, content: str) -> Optional[int]:
        match = re.search(r'Количество пострадавших:\s*(\d+)', content)
        if match:
            return int(match.group(1))
        
        match = re.search(r'(\d+)\s+получили\s+травмы', content)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_material_damage(self, content: str) -> Optional[str]:
        match = re.search(r'Материальный ущерб:\s*([^\n]+)', content)
        if match:
            return match.group(1).strip()
        return None
    
    def _extract_description(self, content: str) -> Optional[str]:
        # Ищем "Краткое описание:"
        match = re.search(r'Краткое описание:\s*\n(.+?)(?=\n={10,}|$)', content, re.DOTALL)
        if match:
            return TextCleaner.clean_whitespace(match.group(1))
        
        # Если нет явного маркера, берем весь текст после первых строк
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'Краткое описание' in line:
                desc_lines = []
                for j in range(i + 1, min(i + 20, len(lines))):
                    if lines[j].strip() and not lines[j].startswith('='):
                        desc_lines.append(lines[j].strip())
                    elif lines[j].startswith('='):
                        break
                if desc_lines:
                    return TextCleaner.clean_whitespace(' '.join(desc_lines))
        
        # Если ничего не нашли, возвращаем первые несколько строк
        first_lines = []
        for line in lines[:15]:
            if line.strip() and not line.startswith('='):
                first_lines.append(line.strip())
        return ' '.join(first_lines) if first_lines else None
    
    def _extract_locations_mawo(self, content: str) -> Optional[str]:
        """Использует mawo-natasha для извлечения локаций"""
        if not self.mawo_available:
            return self._extract_locations_regex(content)
        
        try:
            doc = self.MAWODoc(content)
            doc.segment()
            markup = self.ner_tagger(doc.text)
            
            locations = []
            for span in markup.spans:
                if span.type == 'LOC':
                    locations.append(span.text)
            
            # Также ищем специфические шахтные локации
            mine_locations = re.findall(r'(лава\s+[0-9К-]+|КШ\s+[0-9К-]+|ВШ\s+[0-9К-]+|секци[яи]\s+\d+(?:-\d+)?)', content, re.IGNORECASE)
            locations.extend(mine_locations)
            
            return '; '.join(set(locations)) if locations else None
        except Exception as e:
            print(f"Error in mawo extraction: {e}")
            return self._extract_locations_regex(content)
    
    def _extract_locations_regex(self, content: str) -> Optional[str]:
        """Извлекает локации с помощью regex (fallback)"""
        locations = []
        
        patterns = [
            r'(лава\s+[0-9К-]+)',
            r'(КШ\s+[0-9К-]+)',
            r'(ВШ\s+[0-9К-]+)',
            r'(секци[яи]\s+\d+(?:-\d+)?)',
            r'(шахта\s+[^\n,\.]+)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            locations.extend(matches)
        
        return '; '.join(set(locations)) if locations else None
    
    def _extract_entities_mawo(self, content: str) -> Optional[str]:
        """Использует mawo-natasha для извлечения всех сущностей"""
        if not self.mawo_available:
            return None
        
        try:
            doc = self.MAWODoc(content)
            doc.segment()
            markup = self.ner_tagger(doc.text)
            
            entities = []
            for span in markup.spans:
                if span.text and len(span.text) > 1:
                    entities.append(f"{span.text} ({span.type})")
            
            result = '; '.join(entities[:15]) if entities else None
            
            # Если слишком много PER, возможно ошибка NER
            if result and result.count('(PER)') > 10:
                return None
            
            return result
        except Exception as e:
            print(f"Error in mawo extraction: {e}")
            return None