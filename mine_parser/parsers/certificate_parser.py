# parsers/certificate_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_certificate_id
from text_cleaner import TextCleaner
from date_parser import DateParser  # <-- ИСПРАВЛЕНО: правильный импорт


class CertificateParser(BaseParser):
    """
    Парсер для сертификатов и протоколов испытаний оборудования.
    Поддерживает: сертификаты соответствия, протоколы испытаний.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("equipment_certificate")
        self.equipment_mapping = {}
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'certificate', 'сертификат', 'cert', 'register'  
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с сертификатами.
        В одном файле может быть несколько сертификатов.
        """
        records = []
        
        # Разбиваем файл на блоки сертификатов
        certificate_blocks = self._split_certificates(content)
        
        for block in certificate_blocks:
            record = self._parse_certificate_block(block, file_name)
            if record:
                records.append(record)
        
        return records
    
    def _split_certificates(self, content: str) -> List[str]:
        """
        Разбивает содержимое на блоки сертификатов.
        Разделители: ====, ----, несколько пустых строк.
        """
        blocks = []
        
        # Ищем блоки, начинающиеся с "СЕРТИФИКАТ" или "ПРОТОКОЛ"
        cert_starts = re.finditer(
            r'(?:(?:СЕРТИФИКАТ|ПРОТОКОЛ)\s+[A-Z0-9\s\.]+)',
            content,
            re.IGNORECASE
        )
        
        starts = [m.start() for m in cert_starts]
        if not starts:
            # Если не нашли явных маркеров, обрабатываем весь файл как один блок
            return [content]
        
        for i, start in enumerate(starts):
            end = starts[i + 1] if i + 1 < len(starts) else len(content)
            blocks.append(content[start:end])
        
        return blocks
    
    def _parse_certificate_block(self, block: str, file_name: str) -> Optional[Dict[str, Any]]:
        """Парсит один блок сертификата"""
        
        # Определяем тип сертификата
        certificate_type = self._detect_certificate_type(block)
        
        record = {
            'certificate_id': generate_certificate_id(),
            'equipment_id': self._extract_equipment_id(block),
            'certificate_number': self._extract_certificate_number(block),
            'certificate_type': certificate_type,
            'issuing_body': self._extract_issuing_body(block),
            'issue_date': self._extract_issue_date(block),
            'expiry_date': self._extract_expiry_date(block),
            'is_valid_at_incident': None,  
            '_source_file': file_name
        }
        
        # Очищаем строковые поля
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = TextCleaner.clean_whitespace(value)
        
        # Если нет номера сертификата — пропускаем
        if not record['certificate_number']:
            return None
        
        return record
    
    def _detect_certificate_type(self, block: str) -> str:
        """Определяет тип сертификата"""
        block_upper = block.upper()
        
        if 'ПРОТОКОЛ ИСПЫТАНИЙ' in block_upper:
            return 'протокол испытаний'
        elif 'СЕРТИФИКАТ СООТВЕТСТВИЯ' in block_upper:
            return 'сертификат соответствия'
        elif 'ДЕКЛАРАЦИЯ' in block_upper:
            return 'декларация соответствия'
        else:
            return 'сертификат'
    
    def _extract_certificate_number(self, block: str) -> Optional[str]:
        """Извлекает номер сертификата"""
        patterns = [
            r'СЕРТИФИКАТ\s+([A-Z0-9\s\.]+)',
            r'СЕРТИФИКАТ СООТВЕТСТВИЯ\s+([A-Z0-9\s\.]+)',
            r'ПРОТОКОЛ\s+ИСПЫТАНИЙ\s+№\s*([A-Z0-9\-\.]+)',
            r'№\s*([A-Z0-9\-\.]+)',
            r'Certificate\s+Number:\s*([A-Z0-9\-\.]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_issuing_body(self, block: str) -> Optional[str]:
        """Извлекает орган по сертификации"""
        patterns = [
            r'Орган по сертификации:\s*([^\n]+)',
            r'Issuing body:\s*([^\n]+)',
            r'Сертификационный центр:\s*([^\n]+)',
            r'Issued by:\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_issue_date(self, block: str) -> Optional[str]:
        """Извлекает дату выдачи"""
        patterns = [
            r'Дата выдачи:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Дата выдачи:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})',
            r'Issue date:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Дата:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Выдан:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                # Парсим через DateParser
                parsed = DateParser.parse_to_str(date_str)
                if parsed:
                    return parsed
        return None
    
    def _extract_expiry_date(self, block: str) -> Optional[str]:
        """Извлекает дату истечения срока действия"""
        patterns = [
            r'Действителен до:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Действителен до:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})',
            r'Expiry date:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Срок действия до:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
            r'Valid until:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                date_str = match.group(1)
                parsed = DateParser.parse_to_str(date_str)
                if parsed:
                    return parsed
        
        # Ищем упоминание срока действия в тексте
        match = re.search(r'Срок действия[^:]*до\s+([0-9]{4})\s*г', block, re.IGNORECASE)
        if match:
            year = match.group(1)
            return f"{year}-12-31"
        
        return None
    
    def _extract_equipment_id(self, block: str) -> Optional[str]:
        """
        Извлекает или определяет ID оборудования.
        Здесь мы извлекаем модель и серийный номер для последующего сопоставления.
        """
        # Извлекаем модель
        model_patterns = [
            r'Изделие:\s*([^\n]+)',
            r'Оборудование:\s*([^\n]+)',
            r'Product:\s*([^\n]+)',
        ]
        
        model = None
        for pattern in model_patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                model = match.group(1).strip()
                break
        
        # Извлекаем серийный номер
        serial_patterns = [
            r'Заводской номер:\s*([^\n]+)',
            r'Серийный номер:\s*([^\n]+)',
            r'Serial number:\s*([^\n]+)',
        ]
        
        serial_number = None
        for pattern in serial_patterns:
            match = re.search(pattern, block, re.IGNORECASE)
            if match:
                serial_number = match.group(1).strip()
                break
        
        # Формируем ключ для поиска в equipment_mapping
        if model:
            key = model
            if serial_number:
                key = f"{model}|{serial_number}"
            return self.equipment_mapping.get(key)
        
        return None
    
    def set_equipment_mapping(self, mapping: Dict[str, str]) -> None:
        """
        Устанавливает маппинг для сопоставления оборудования.
        
        Args:
            mapping: Словарь вида {"модель": "equipment_id"} или {"модель|серийный": "equipment_id"}
        """
        self.equipment_mapping = mapping