# utils/text_cleaner.py
# -*- coding: utf-8 -*-

import re
from typing import Optional, List, Tuple, Any


class TextCleaner:
    """
    Утилита для очистки текста из разных источников.
    """
    
    @staticmethod
    def clean_whitespace(text: str) -> str:
        """Удаляет лишние пробелы, табуляции, переносы строк"""
        if not text:
            return ""
        # Заменяем любые пробельные символы на один пробел
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    @staticmethod
    def remove_special_chars(text: str, keep_dots: bool = True, keep_digits: bool = True) -> str:
        """Удаляет спецсимволы, оставляя буквы, цифры и точки (опционально)"""
        if not text:
            return ""
        
        if keep_dots and keep_digits:
            pattern = r'[^а-яА-Яa-zA-Z0-9\s\.]'
        elif keep_dots and not keep_digits:
            pattern = r'[^а-яА-Яa-zA-Z\s\.]'
        elif not keep_dots and keep_digits:
            pattern = r'[^а-яА-Яa-zA-Z0-9\s]'
        else:
            pattern = r'[^а-яА-Яa-zA-Z\s]'
        
        text = re.sub(pattern, '', text)
        return TextCleaner.clean_whitespace(text)
    
    @staticmethod
    def normalize_russian(text: str) -> str:
        """Нормализует русский текст (замена ё на е, приведение к нижнему регистру)"""
        if not text:
            return ""
        text = text.lower()
        text = text.replace('ё', 'е')
        return text
    
    @staticmethod
    def extract_numbers(text: str, as_float: bool = False) -> List[Any]:
        """Извлекает все числа из текста"""
        if not text:
            return []
        
        if as_float:
            pattern = r'\d+[\.,]\d+|\d+'
        else:
            pattern = r'\d+'
        
        matches = re.findall(pattern, text)
        if as_float:
            return [float(m.replace(',', '.')) for m in matches]
        return [int(m) for m in matches]
    
    @staticmethod
    def extract_first_number(text: str, as_float: bool = False) -> Optional[float]:
        """Извлекает первое число из текста"""
        numbers = TextCleaner.extract_numbers(text, as_float)
        return numbers[0] if numbers else None
    
    @staticmethod
    def remove_parentheses_content(text: str) -> str:
        """Удаляет содержимое скобок (включая сами скобки)"""
        if not text:
            return ""
        # Удаляем содержимое круглых скобок
        text = re.sub(r'\([^)]*\)', '', text)
        # Удаляем содержимое квадратных скобок
        text = re.sub(r'\[[^\]]*\]', '', text)
        return TextCleaner.clean_whitespace(text)
    
    @staticmethod
    def split_fio(fio: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Разделяет ФИО на составные части.
        Поддерживает форматы:
        - "Иванов Иван Иванович"
        - "Иванов И.И."
        - "Иванов Иван"
        """
        if not fio:
            return (None, None, None)
        
        parts = fio.strip().split()
        
        if len(parts) == 0:
            return (None, None, None)
        elif len(parts) == 1:
            return (parts[0], None, None)
        elif len(parts) == 2:
            # Проверяем, не является ли вторая часть инициалами
            if re.match(r'^[А-Я]\.?[А-Я]?\.?$', parts[1]):
                return (parts[0], parts[1], None)
            return (parts[0], parts[1], None)
        else:
            # Три и более частей
            lastname = parts[0]
            firstname = parts[1]
            middlename = ' '.join(parts[2:]) if len(parts) > 2 else None
            return (lastname, firstname, middlename)
    
    @staticmethod
    def clean_table_line(line: str, separator: str = '|') -> List[str]:
        """Очищает строку таблицы и возвращает список значений"""
        if not line:
            return []
        
        parts = [p.strip() for p in line.split(separator)]
        # Убираем пустые части в начале и конце (обычно от ведущего/замыкающего разделителя)
        while parts and parts[0] == '':
            parts.pop(0)
        while parts and parts[-1] == '':
            parts.pop()
        return parts
    
    @staticmethod
    def is_header_line(line: str, keywords: List[str]) -> bool:
        """Проверяет, является ли строка заголовочной (содержит ключевые слова)"""
        if not line:
            return False
        line_lower = line.lower()
        for keyword in keywords:
            if keyword.lower() in line_lower:
                return True
        return False
    
    @staticmethod
    def is_separator_line(line: str, separator: str = '|') -> bool:
        """Проверяет, является ли строка разделительной (---, ===, ***)"""
        if not line:
            return False
        cleaned = line.replace(separator, '').strip()
        if not cleaned:
            return False
        # Явно проверяем, что все символы - это разделители
        for c in cleaned:
            if c not in '-=*':
                return False
        return True