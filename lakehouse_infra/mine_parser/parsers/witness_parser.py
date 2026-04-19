# parsers/witness_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional

from parsers.base_parser import BaseParser
from utils.id_generator import generate_statement_id
from parsers.date_parser import DateParser


class WitnessParser(BaseParser):
    """
    Парсер для протоколов опроса свидетелей.
    Активно использует mawo-natasha для NER и семантического анализа.
    """
    
    INCIDENT_ID = "INC-2023-001"
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("witness_statement")
        self._init_mawo()
    
    def _init_mawo(self):
        """Инициализирует mawo-natasha"""
        try:
            from mawo_natasha import MAWODoc
            from mawo_slovnet import NewsNERTagger
            from mawo_natasha import RealRussianEmbedding
            self.MAWODoc = MAWODoc
            self.ner_tagger = NewsNERTagger()
            self.embedding = RealRussianEmbedding(use_navec=True)
            self.mawo_available = True
        except ImportError:
            self.mawo_available = False
            print("Warning: mawo-natasha not installed")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in ['witness', 'опрос', 'показан', 'протокол'])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        # Разбиваем на отдельные показания
        statements = re.split(r'={70,}', content)
        
        for stmt in statements:
            if not stmt.strip():
                continue
            
            record = self._parse_single_statement(stmt, file_name)
            if record:
                # Добавляем mawo-обогащение
                record = self._enrich_with_mawo(record, stmt)
                records.append(record)
        
        return records
    
    def _parse_single_statement(self, text: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит одно показание"""
        
        # Извлекаем имя свидетеля
        witness_name = self._extract_witness_name(text)
        
        # Извлекаем дату и время опроса
        statement_datetime = self._extract_interview_datetime(text)
        
        # Извлекаем текст показаний
        testimony_text = self._extract_testimony(text)
        
        if not testimony_text:
            return None
        
        return {
            'statement_id': generate_statement_id(),
            'incident_id': self.INCIDENT_ID,
            'witness_name': witness_name,
            'statement_datetime': statement_datetime,
            'testimony_text': testimony_text[:5000],
            '_source_file': file_name
        }
    
    def _enrich_with_mawo(self, record: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Обогащает запись данными из mawo-natasha"""
        if not self.mawo_available:
            return record
        
        try:
            # Создаём документ
            doc = self.MAWODoc(text)
            doc.segment()
            
            # NER
            markup = self.ner_tagger(doc.text)
            
            # Извлекаем персоны
            persons = []
            locations = []
            organizations = []
            
            for span in markup.spans:
                if span.type == 'PER':
                    persons.append(span.text)
                elif span.type == 'LOC':
                    locations.append(span.text)
                elif span.type == 'ORG':
                    organizations.append(span.text)
            
            # Разбиваем на факты (предложения)
            facts = [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 30]
            
            # Ищем ключевые события
            events = self._extract_events_from_text(text)
            
            # Добавляем обогащённые поля
            record['extracted_persons'] = '; '.join(set(persons)) if persons else None
            record['extracted_locations'] = '; '.join(set(locations)) if locations else None
            record['extracted_organizations'] = '; '.join(set(organizations)) if organizations else None
            record['extracted_facts'] = '; '.join(facts[:10]) if facts else None
            record['extracted_events'] = '; '.join(events[:5]) if events else None
            
        except Exception as e:
            print(f"Error in mawo enrichment: {e}")
        
        return record
    
    def _extract_witness_name(self, text: str) -> Optional[str]:
        """Извлекает имя свидетеля"""
        patterns = [
            r'Опрашиваемый:\s*([^\n]+)',
            r'Свидетель:\s*([^\n]+)',
            r'ФИО:\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_interview_datetime(self, text: str) -> Optional[str]:
        """Извлекает дату и время опроса"""
        date_match = re.search(r'Дата опроса:\s*(\d{2}\.\d{2}\.\d{4})', text)
        time_match = re.search(r'Время:\s*(\d{2}:\d{2})', text)
        
        if date_match:
            date_str = date_match.group(1)
            if time_match:
                return f"{date_str} {time_match.group(1)}:00"
            return DateParser.parse_to_str(date_str)
        return None
    
    def _extract_testimony(self, text: str) -> Optional[str]:
        """Извлекает текст показаний"""
        # Ищем секцию "Ответ:"
        match = re.search(r'Ответ:\s*(.+?)(?=\n\n|\n[А-Я]{2,}|$)', text, re.DOTALL)
        if match:
            testimony = match.group(1).strip()
            testimony = re.sub(r'\n\s*', '\n', testimony)
            return testimony
        
        # Если нет явного "Ответ:", берем все после последнего вопроса
        parts = re.split(r'Вопрос:', text)
        if len(parts) > 1:
            return parts[-1].strip()
        
        return None
    
    def _extract_events_from_text(self, text: str) -> List[str]:
        """Извлекает события из текста показаний"""
        event_keywords = [
            'взрыв', 'пожар', 'горит', 'дым', 'эвакуация', 'выходить',
            'отключение', 'энергия', 'пострадавший', 'ожог'
        ]
        
        events = []
        sentences = re.split(r'[.!?]', text)
        
        for sent in sentences:
            for keyword in event_keywords:
                if keyword in sent.lower():
                    events.append(sent.strip())
                    break
        
        return events