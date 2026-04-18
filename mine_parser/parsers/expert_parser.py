# parsers/expert_parser.py
# -*- coding: utf-8 -*-

import re
import csv
import io
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_expert_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class ExpertParser(BaseParser):
    """
    Парсер для реестра экспертов (CSV файлы).
    Выходные поля: expert_id, profession, expert_lastname, expert_firstname, expert_middlename, category
    """
    
    def __init__(self, config_path: str = "./config", incident_id: str = 'None'):
        super().__init__(config_path, incident_id)
        self.set_table_name("expert_dictionary")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'реестр', 'эксперт', 'expert'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит файл с реестром экспертов"""
        file_ext = Path(file_name).suffix.lower()
        
        if file_ext == '.csv':
            return self._parse_csv(content, file_name)
        elif file_ext in ['.xlsx', '.xls']:
            return self._parse_excel_file(file_name)
        else:
            return self._parse_text(content, file_name)
    
    def _parse_csv(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит CSV файл с реестром экспертов"""
        records = []
        
        try:
            # Пробуем разные разделители
            for delimiter in [';', ',', '\t']:
                try:
                    csv_reader = csv.reader(io.StringIO(content), delimiter=delimiter)
                    rows = list(csv_reader)
                    
                    # Проверяем, что строк достаточно и есть данные
                    if len(rows) >= 3 and len(rows[0]) >= 3:
                        # Пропускаем первые 2 строки (заголовки)
                        # строка 0: "Реестр экспертов в области промышленной безопасности ;;;;"
                        # строка 1: "Фамилия, имя, отчество...;Область...;Категория...;Дата...;Доп..."
                        # строка 2: пустая (;;;;)
                        # данные начинаются со строки 3
                        data_rows = rows[3:]  # пропускаем 3 строки
                        
                        for row in data_rows:
                            # Пропускаем пустые строки
                            if not row or len(row) < 3:
                                continue
                            
                            # Очищаем значения
                            cleaned_row = [cell.strip() if cell else '' for cell in row]
                            
                            # Пропускаем строки, где нет ФИО
                            fio = cleaned_row[0] if len(cleaned_row) > 0 else None
                            if not fio or fio == '':
                                continue
                            
                            # Пропускаем строки-заголовки (если вдруг попали)
                            if fio.lower() in ['фамилия', 'имя', 'отчество', 'фамилия, имя, отчество']:
                                continue
                            
                            # Парсим ФИО
                            lastname, firstname, middlename = self._parse_fio(fio)
                            
                            # Получаем область аттестации (колонка 1)
                            profession = cleaned_row[1] if len(cleaned_row) > 1 and cleaned_row[1] else None
                            
                            # Получаем категорию (колонка 2)
                            category = None
                            if len(cleaned_row) > 2 and cleaned_row[2]:
                                category = self._parse_category(cleaned_row[2])
                            
                            # Получаем дату (колонка 3) - не обязательное поле
                            expiry_date = None
                            if len(cleaned_row) > 3 and cleaned_row[3]:
                                expiry_date = DateParser.parse_to_str(cleaned_row[3])
                            
                            # Создаем запись
                            record = {
                                'expert_id': generate_expert_id(),
                                'expert_lastname': lastname,
                                'expert_firstname': firstname,
                                'expert_middlename': middlename,
                                'profession': profession,
                                'category': category,
                                'expiry_date': expiry_date,
                                '_source_file': Path(file_name).name
                            }
                            
                            # Добавляем только если есть ФИО
                            if lastname:
                                records.append(record)
                        
                        # Если нашли данные, выходим из цикла по разделителям
                        if records:
                            break
                            
                except Exception as e:
                    # Пробуем следующий разделитель
                    continue
            
            return records
            
        except Exception as e:
            print(f"Error parsing CSV file {file_name}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _parse_excel_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Парсит Excel файл (конвертирует в CSV логику)"""
        try:
            import pandas as pd
            
            # Читаем Excel без заголовков
            df = pd.read_excel(file_path, header=None)
            
            records = []
            
            # Пропускаем первые 3 строки (заголовки)
            # строка 0: "Реестр экспертов..."
            # строка 1: "Фамилия, имя..."
            # строка 2: пустая
            # данные начинаются со строки 3
            for idx, row in df.iloc[3:].iterrows():
                # Получаем ФИО (колонка 0)
                fio = row.iloc[0] if pd.notna(row.iloc[0]) else None
                if not fio or str(fio).strip() == '' or str(fio).lower() == 'nan':
                    continue
                
                fio_str = str(fio).strip()
                
                # Парсим ФИО
                lastname, firstname, middlename = self._parse_fio(fio_str)
                
                # Получаем область аттестации (колонка 1)
                profession = None
                if pd.notna(row.iloc[1]):
                    profession = str(row.iloc[1]).strip()
                    if profession.lower() == 'nan':
                        profession = None
                
                # Получаем категорию (колонка 2)
                category = None
                if pd.notna(row.iloc[2]):
                    category = self._parse_category(row.iloc[2])
                
                # Получаем дату (колонка 3)
                expiry_date = None
                if pd.notna(row.iloc[3]):
                    expiry_date = DateParser.parse_to_str(str(row.iloc[3]))
                
                record = {
                    'expert_id': generate_expert_id(),
                    'expert_lastname': lastname,
                    'expert_firstname': firstname,
                    'expert_middlename': middlename,
                    'profession': profession,
                    'category': category,
                    'expiry_date': expiry_date,
                    '_source_file': Path(file_path).name
                }
                
                if lastname:
                    records.append(record)
            
            return records
            
        except ImportError:
            print("Warning: pandas or openpyxl not installed. Run: pip install pandas openpyxl")
            return []
        except Exception as e:
            print(f"Error parsing Excel file {file_path}: {e}")
            return []
    
    def _parse_text(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит текстовый файл"""
        records = []
        lines = content.split('\n')
        
        for line in lines:
            match = re.search(r'([А-Я][а-я]+)\s+([А-Я][а-я]+)\s+([А-Я][а-я]+)', line)
            if match:
                lastname, firstname, middlename = match.groups()
                record = {
                    'expert_id': generate_expert_id(),
                    'expert_lastname': lastname,
                    'expert_firstname': firstname,
                    'expert_middlename': middlename,
                    'profession': None,
                    'category': None,
                    'expiry_date': None,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records
    
    def _parse_fio(self, fio: str) -> tuple:
        """Парсит ФИО из строки"""
        if not fio:
            return None, None, None
        
        # Очищаем от лишних символов
        fio = re.sub(r'\s+', ' ', fio).strip()
        parts = fio.split()
        
        lastname = parts[0] if len(parts) > 0 else None
        firstname = parts[1] if len(parts) > 1 else None
        middlename = parts[2] if len(parts) > 2 else None
        
        return lastname, firstname, middlename
    
    def _parse_category(self, value) -> Optional[int]:
        """Парсит категорию эксперта в int"""
        if value is None:
            return None
        
        try:
            if isinstance(value, (int, float)):
                return int(value)
            
            val_str = str(value).strip()
            if val_str == 'nan' or val_str == '':
                return None
            
            # Ищем цифру (1, 2, 3)
            match = re.search(r'[123]', val_str)
            return int(match.group()) if match else None
        except:
            return None 