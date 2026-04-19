# utils/date_parser.py
# -*- coding: utf-8 -*-

import re
from datetime import datetime
from typing import Optional, Union
from dateutil import parser
from dateutil.parser import ParserError


class DateParser:
    """
    Утилита для парсинга дат из разных форматов.
    Поддерживает русские и международные форматы.
    """
    
    # Русские названия месяцев
    RUSSIAN_MONTHS = {
        'янв': 1, 'января': 1, 'январь': 1,
        'фев': 2, 'февраля': 2, 'февраль': 2,
        'мар': 3, 'марта': 3, 'март': 3,
        'апр': 4, 'апреля': 4, 'апрель': 4,
        'май': 5, 'мая': 5,
        'июн': 6, 'июня': 6, 'июнь': 6,
        'июл': 7, 'июля': 7, 'июль': 7,
        'авг': 8, 'августа': 8, 'август': 8,
        'сен': 9, 'сентября': 9, 'сентябрь': 9,
        'окт': 10, 'октября': 10, 'октябрь': 10,
        'ноя': 11, 'ноября': 11, 'ноябрь': 11,
        'дек': 12, 'декабря': 12, 'декабрь': 12,
    }
    
    @staticmethod
    def parse(text: str, default_year: Optional[int] = None) -> Optional[datetime]:
        """
        Универсальный парсинг даты из текста.
        
        Поддерживаемые форматы:
        - 26.03.2026
        - 26 марта 2026 г.
        - 26 марта 2026
        - 2026-03-26
        - 26/03/2026
        """
        if not text:
            return None
        
        text = str(text).strip()
        
        # Попытка парсинга через dateutil
        try:
            return parser.parse(text, default=datetime(2000, 1, 1) if default_year is None else None)
        except ParserError:
            pass
        
        # Русский формат: "26 марта 2026 г."
        match = re.match(r'(\d{1,2})\s+([а-я]+)\s+(\d{4})(?:\s+г\.?)?', text, re.IGNORECASE)
        if match:
            day = int(match.group(1))
            month_name = match.group(2).lower()
            year = int(match.group(3))
            
            if month_name in DateParser.RUSSIAN_MONTHS:
                month = DateParser.RUSSIAN_MONTHS[month_name]
                try:
                    return datetime(year, month, day)
                except ValueError:
                    return None
        
        # Формат с точками: DD.MM.YYYY или DD.MM.YY
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})', text)
        if match:
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3))
            if year < 100:
                year += 2000 if year < 70 else 1900
            try:
                return datetime(year, month, day)
            except ValueError:
                return None
        
        # Формат с дефисами: YYYY-MM-DD
        match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', text)
        if match:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            try:
                return datetime(year, month, day)
            except ValueError:
                return None
        
        # Формат со слэшами: DD/MM/YYYY
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if match:
            day = int(match.group(1))
            month = int(match.group(2))
            year = int(match.group(3))
            try:
                return datetime(year, month, day)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def parse_to_str(text: str, output_format: str = "%Y-%m-%d", default_year: Optional[int] = None) -> Optional[str]:
        """Парсит дату и возвращает строку в указанном формате"""
        dt = DateParser.parse(text, default_year)
        if dt:
            return dt.strftime(output_format)
        return None
    
    @staticmethod
    def parse_timestamp(text: str) -> Optional[datetime]:
        """Парсит временную метку (дата + время)"""
        if not text:
            return None
        
        # Формат: 2023-10-28 02:29:04
        match = re.match(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', text)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            dt = DateParser.parse(date_part)
            if dt:
                time_parts = time_part.split(':')
                try:
                    return dt.replace(
                        hour=int(time_parts[0]),
                        minute=int(time_parts[1]),
                        second=int(time_parts[2])
                    )
                except (ValueError, IndexError):
                    return dt
            return None
        
        # Формат: 02:29:04 (только время, без даты)
        match = re.match(r'(\d{2}):(\d{2}):(\d{2})', text)
        if match:
            return datetime(1900, 1, 1,
                          hour=int(match.group(1)),
                          minute=int(match.group(2)),
                          second=int(match.group(3)))
        
        # Формат с T: 2023-10-28T02:29:04
        match = re.match(r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})', text)
        if match:
            return DateParser.parse_timestamp(f"{match.group(1)} {match.group(2)}")
        
        return None
    
    @staticmethod
    def is_date(text: str) -> bool:
        """Проверяет, является ли строка датой"""
        return DateParser.parse(text) is not None
    
    @staticmethod
    def extract_date(text: str) -> Optional[datetime]:
        """Извлекает первую дату из текста"""
        # Ищем паттерны дат
        patterns = [
            r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})',
            r'(\d{1,2})\s+([а-я]+)\s+(\d{4})(?:\s+г\.?)?',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if '.' in pattern:
                    day = int(match.group(1))
                    month = int(match.group(2))
                    year = int(match.group(3))
                    if year < 100:
                        year += 2000 if year < 70 else 1900
                elif '-' in pattern:
                    year = int(match.group(1))
                    month = int(match.group(2))
                    day = int(match.group(3))
                else:  # русский формат
                    day = int(match.group(1))
                    month_name = match.group(2).lower()
                    year = int(match.group(3))
                    month = DateParser.RUSSIAN_MONTHS.get(month_name)
                    if month is None:
                        continue
                
                try:
                    return datetime(year, month, day)
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def format_russian(dt: datetime, include_year: bool = True, include_time: bool = False) -> str:
        """Форматирует дату в русский формат (26 марта 2026 г.)"""
        months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
        
        result = f"{dt.day} {months[dt.month - 1]}"
        if include_year:
            result += f" {dt.year} г."
        if include_time:
            result += f" {dt.strftime('%H:%M:%S')}"
        return result