# parsers/chronology_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from base_parser import BaseParser
from id_generator import generate_event_id


class ChronologyParser(BaseParser):
    """
    Парсер для хронологии событий из транскриптов аудиозаписей.
    Активно использует mawo-natasha для NER и извлечения сущностей.
    """
    
    INCIDENT_ID = "INC-2023-001"
    
    EVENT_KEYWORDS = {
        'сообщение о пожаре': ['горит', 'пожар', 'огонь', 'загорание', 'пламя'],
        'взрыв': ['взрыв', 'хлопок', 'ударная волна', 'бабах'],
        'отключение энергии': ['энергию выбило', 'свет погас', 'отключили', 'фидер'],
        'эвакуация': ['выходите', 'уходите', 'эвакуация', 'бегите', 'дуйте'],
        'задымление': ['дым', 'задымление', 'смог', 'видимость'],
        'превышение газа': ['превышение', 'газ', 'метан', 'ППМК', 'CH4', 'СО'],
        'сейсмическое событие': ['толчок', 'сейсмическое'],
        'вызов помощи': ['вызываю', 'план вводить', 'ВГСЧ', 'помощь'],
        'локализация очага': ['за секцией', 'очаг', 'секция', 'видно'],
        'тушение': ['тушим', 'огнетушитель', 'затушить', 'вода'],
    }
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("chronology_incident")
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
        return any(kw in name_lower for kw in ['transcript', 'аудио', 'звонок', 'audio'])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        
        # Разбиваем на блоки по временным меткам
        blocks = self._split_by_timestamp(content)
        
        for block in blocks:
            events = self._extract_events_from_block(block, file_name)
            records.extend(events)
        
        # Добавляем взрыв по данным SCADA (02:43:49)
        records.append(self._create_explosion_event())
        
        # Сортируем по времени
        records.sort(key=lambda x: x.get('event_dttm', ''))
        
        return records
    
    def _split_by_timestamp(self, content: str) -> List[Dict[str, Any]]:
        """Разбивает транскрипт на блоки по временным меткам"""
        blocks = []
        lines = content.split('\n')
        
        current_block = {'timestamp': None, 'text': [], 'raw_timestamp': None}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Проверяем, есть ли временная метка
            timestamp, raw_ts = self._extract_timestamp_from_line(line)
            
            if timestamp:
                # Сохраняем предыдущий блок
                if current_block['timestamp'] and current_block['text']:
                    blocks.append(current_block)
                # Начинаем новый блок
                current_block = {'timestamp': timestamp, 'text': [], 'raw_timestamp': raw_ts}
            else:
                current_block['text'].append(line)
        
        # Добавляем последний блок
        if current_block['timestamp'] and current_block['text']:
            blocks.append(current_block)
        
        return blocks
    
    def _extract_timestamp_from_line(self, line: str) -> Tuple[Optional[datetime], Optional[str]]:
        """Извлекает временную метку из строки"""
        # Формат: "02:32:00"
        match = re.search(r'(\d{2}):(\d{2}):(\d{2})', line)
        if match:
            dt = datetime(2023, 10, 28, 
                         int(match.group(1)), 
                         int(match.group(2)), 
                         int(match.group(3)))
            return dt, match.group(0)
        
        # Формат: "2 ч 32 мин"
        match = re.search(r'(\d+)\s*ч\s*(\d+)\s*мин', line)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2))
            dt = datetime(2023, 10, 28, hour, minute, 0)
            return dt, match.group(0)
        
        return None, None
    
    def _extract_events_from_block(self, block: Dict, file_name: str) -> List[Dict[str, Any]]:
        """Извлекает все события из блока текста"""
        events = []
        text = ' '.join(block['text'])
        
        # Разбиваем на предложения
        sentences = self._split_into_sentences(text)
        
        for sentence in sentences:
            event_type = self._determine_event_type(sentence)
            if event_type:
                event = {
                    'event_id': generate_event_id(),
                    'incident_id': self.INCIDENT_ID,
                    'event_dttm': block['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if block['timestamp'] else None,
                    'event_type': event_type,
                    'action_description': self._extract_action_description(sentence),
                    'temperature_c': None,
                    'pressure_mmHg': None,
                    'humidity_percent': None,
                    'source': 'аудиозапись',
                    'location': self._extract_location_mawo(sentence),
                    'persons': self._extract_persons_mawo(sentence),
                    'speaker': self._extract_speaker(sentence),
                    '_source_file': file_name
                }
                events.append(event)
        
        return events
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """Разбивает текст на предложения"""
        # Сначала пробуем mawo-natasha
        if self.mawo_available:
            try:
                doc = self.MAWODoc(text)
                doc.segment()
                return [sent.text.strip() for sent in doc.sents if len(sent.text.strip()) > 10]
            except:
                pass
        
        # Fallback на regex
        sentences = re.split(r'[.!?]', text)
        return [s.strip() for s in sentences if len(s.strip()) > 10]
    
    def _determine_event_type(self, text: str) -> Optional[str]:
        """Определяет тип события по ключевым словам"""
        text_lower = text.lower()
        for event_type, keywords in self.EVENT_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_lower:
                    return event_type
        return None
    
    def _extract_action_description(self, text: str) -> str:
        """Извлекает описание действия"""
        # Убираем лишние символы
        text = re.sub(r'«|»', '"', text)
        text = re.sub(r'\s+', ' ', text)
        return text[:500]
    
    def _extract_location_mawo(self, text: str) -> Optional[str]:
        """Использует mawo-natasha для извлечения локации"""
        # Сначала пробуем regex (быстрее)
        patterns = [
            r'(?:секци[яи]\s+)(\d{3})',
            r'(\d{3})\s*[-–]\s*(\d{3})',
            r'(ВШ|КШ|лава|сбойка)\s+([0-9К-]+)',
            r'(верхняя|нижняя)\s+(?:секция|часть)',
            r'(ОСК|околоствольная камера)',
            r'(вентиляционны[йй])\s+(?:штрек|участок)',
            r'(конвейерны[йй])\s+(?:штрек|участок)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if 'секци' in pattern and match.group(1):
                    return f"секция {match.group(1)}"
                elif match.group(1) and len(match.groups()) > 1 and match.group(2):
                    return f"секции {match.group(1)}-{match.group(2)}"
                return match.group(0)
        
        # Пробуем mawo-natasha NER
        if self.mawo_available:
            try:
                doc = self.MAWODoc(text)
                doc.segment()
                markup = self.ner_tagger(doc.text)
                for span in markup.spans:
                    if span.type == 'LOC':
                        return span.text
            except:
                pass
        
        return None
    
    def _extract_persons_mawo(self, text: str) -> Optional[str]:
        """Использует mawo-natasha для извлечения имён"""
        persons = []
        
        # Regex для быстрого поиска
        patterns = [
            r'([А-Я][а-я]+)\s*[–-]\s*',
            r'(?:диспетчер|Даулет|Крайнов|Наталья|Николаевич)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            persons.extend(matches)
        
        # mawo-natasha NER
        if self.mawo_available:
            try:
                doc = self.MAWODoc(text)
                doc.segment()
                markup = self.ner_tagger(doc.text)
                for span in markup.spans:
                    if span.type == 'PER':
                        persons.append(span.text)
            except:
                pass
        
        return '; '.join(set(persons)) if persons else None
    
    def _extract_speaker(self, text: str) -> Optional[str]:
        """Извлекает имя говорящего"""
        match = re.search(r'^([А-Я][а-я]+)\s*[–-]', text)
        if match:
            return match.group(1)
        return None
    
    def _create_explosion_event(self) -> Dict[str, Any]:
        """Создает событие взрыва на основе сейсмических данных"""
        return {
            'event_id': generate_event_id(),
            'incident_id': self.INCIDENT_ID,
            'event_dttm': '2023-10-28 02:43:49',
            'event_type': 'взрыв',
            'action_description': 'Сейсмическое событие (первичный взрыв метано-воздушной смеси)',
            'temperature_c': None,
            'pressure_mmHg': None,
            'humidity_percent': None,
            'source': 'SCADA/сейсмика',
            'location': 'лава 48 К3-з',
            'persons': None,
            'speaker': None,
            '_source_file': 'seismic_data'
        }