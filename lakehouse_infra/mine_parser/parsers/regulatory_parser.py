# parsers/regulatory_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_doc_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser


class RegulatoryParser(BaseParser):
    """
    Парсер для нормативных документов.
    Поддерживает: методические рекомендации, руководства по эксплуатации,
                 правила безопасности, стандарты.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("regulatory_document")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'guidelines', 'рекомендац', 'правила', 'руководство',
            'стандарт', 'норматив', 'regulatory'  # ← добавить
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит нормативный документ.
        Возвращает одну запись на документ (с собранными нормативными значениями).
        """
        # Извлекаем основные поля
        doc_name = self._extract_doc_name(content, file_name)
        doc_number = self._extract_doc_number(content)
        issue_date = self._extract_issue_date(content)
        effective_date = self._extract_effective_date(content, issue_date)
        
        # Извлекаем все нормативные значения
        normative_values = self._extract_all_normative_values(content)
        
        # Извлекаем все разделы/пункты
        sections = self._extract_all_sections(content)
        
        record = {
            'doc_id': generate_doc_id(),
            'doc_name': doc_name,
            'doc_number': doc_number,
            'issue_date': issue_date,
            'effective_date': effective_date,
            'normative_value': '; '.join(normative_values) if normative_values else None,
            'section': '; '.join(sections) if sections else None,
            '_source_file': file_name
        }
        
        # Очищаем строковые поля
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = TextCleaner.clean_whitespace(value)
        
        return [record]
    
    def _extract_doc_name(self, content: str, file_name: str) -> str:
        """Извлекает наименование документа"""
        # Ищем в первых строках
        lines = content.split('\n')[:20]
        for line in lines:
            line = line.strip()
            if len(line) > 10 and not line.startswith('=') and not line.startswith('-'):
                # Проверяем, что это не служебная строка
                if any(keyword in line.lower() for keyword in [
                    'методическ', 'руководств', 'правила', 'стандарт',
                    'инструкц', 'положени', 'норматив'
                ]):
                    return line
        
        # По имени файла
        name_lower = file_name.lower()
        if 'dust' in name_lower or 'пыль' in name_lower:
            return "Методические рекомендации по борьбе с пылью и пылевзрывозащите на угольных шахтах"
        elif 'conveyor' in name_lower or 'конвейер' in name_lower:
            return "Руководство по эксплуатации конвейера забойного скребкового FFC-8"
        elif 'combine' in name_lower or 'комбайн' in name_lower:
            return "Руководство по эксплуатации очистного комбайна FS300/1.0"
        elif 'safety' in name_lower or 'безопасност' in name_lower:
            return "Правила безопасности в угольных шахтах"
        
        return 'None'
    
    def _extract_doc_number(self, content: str) -> Optional[str]:
        """Извлекает номер документа"""
        patterns = [
            r'Номер документа:\s*([^\n]+)',
            r'№\s*([А-Я0-9\-\.]+)',
            r'Document number:\s*([^\n]+)',
            r'Рег\.\s*№\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_issue_date(self, content: str) -> Optional[str]:
        """Извлекает дату утверждения"""
        patterns = [
            r'Утверждены?:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Утверждены?:\s*([а-я]+\s+[0-9]{4})',
            r'Дата утверждения:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Approved:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                parsed = DateParser.parse_to_str(date_str)
                if parsed:
                    return parsed
        
        return None
    
    def _extract_effective_date(self, content: str, issue_date: Optional[str]) -> Optional[str]:
        """Извлекает дату вступления в силу"""
        patterns = [
            r'Введены? в действие:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Дата вступления:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Effective date:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                parsed = DateParser.parse_to_str(date_str)
                if parsed:
                    return parsed
        
        # Если не указана отдельно, обычно совпадает с датой утверждения
        return issue_date
    
    def _extract_all_normative_values(self, content: str) -> List[str]:
        """Извлекает все нормативные значения из документа"""
        values = []
        
        # Паттерны для нормативных значений
        patterns = [
            # Проценты
            r'(\d+(?:\.\d+)?)\s*%\s*(?:не менее|не более|не выше|не ниже)',
            r'(?:не менее|не более|не выше|не ниже)\s*(\d+(?:\.\d+)?)\s*%',
            r'содержание\s+(\w+)\s+(\d+(?:\.\d+)?)\s*%',
            
            # Расход воды
            r'(\d+(?:\.\d+)?)\s*л/мин',
            r'(\d+(?:\.\d+)?)\s*л/с',
            
            # Давление
            r'(\d+(?:\.\d+)?)\s*МПа',
            
            # Расстояние
            r'(\d+(?:\.\d+)?)\s*м\s*(?:не более|не менее)',
            r'(?:не более|не менее)\s*(\d+(?:\.\d+)?)\s*м',
            
            # Скорость
            r'(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*м/с',
            
            # Концентрация газов
            r'(?:CH4|CO|метан)\s*[:\-]\s*(\d+(?:\.\d+)?)\s*%',
            r'предупредительный\s+(\d+(?:\.\d+)?)\s*%',
            r'аварийный\s+(\d+(?:\.\d+)?)\s*%',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    if len(match) == 2 and '-' in pattern:
                        values.append(f"{match[0]}-{match[1]} м/с")
                    elif len(match) == 2:
                        values.append(f"{match[0]} {match[1]}")
                    else:
                        values.append(str(match[0]))
                else:
                    values.append(match)
        
        # Уникальные значения
        unique_values = []
        for v in values:
            if v and v not in unique_values:
                unique_values.append(v)
        
        return unique_values[:20]  # Ограничиваем количество
    
    def _extract_all_sections(self, content: str) -> List[str]:
        """Извлекает все разделы и пункты из документа"""
        sections = []
        
        # Ищем разделы
        section_patterns = [
            r'---\s*РАЗДЕЛ\s+(\d+(?:\.\d+)*)\s*\.\s*([А-Я\s]+)\s*---',
            r'Раздел\s+(\d+(?:\.\d+)*)(?:\s+\([^)]+\))?',
            r'Пункт\s+(\d+(?:\.\d+)*)',
        ]
        
        for pattern in section_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    section_num = match[0]
                    section_name = match[1].strip() if len(match) > 1 else ""
                    if section_name:
                        sections.append(f"{section_num}. {section_name}")
                    else:
                        sections.append(section_num)
                else:
                    sections.append(match)
        
        # Уникальные разделы
        unique_sections = []
        for s in sections:
            if s and s not in unique_sections:
                unique_sections.append(s)
        
        return unique_sections[:20]  # Ограничиваем количество
    
    def _extract_specific_requirement(self, content: str, section: str) -> Optional[str]:
        """Извлекает текст требования из конкретного раздела"""
        # Ищем раздел
        pattern = rf'{section}[^\n]+\n"?([^=]+)"?'
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if match:
            text = match.group(1).strip()
            # Ограничиваем длину
            if len(text) > 500:
                text = text[:500] + "..."
            return text
        return None