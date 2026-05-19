# parsers/certificate_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional

from parsers.base_parser import BaseParser
from utils.id_generator import generate_certificate_id
from parsers.date_parser import DateParser


class CertificateParser(BaseParser):
    """
    Парсер для сертификатов и протоколов испытаний оборудования.

    Поддерживает форматы из данных:
    - certificate_combine_FS300.txt  (один сертификат, начинается с СЕРТИФИКАТ СООТВЕТСТВИЯ ...)
    - certificates_equipment.txt     (несколько сертификатов, разделены ===)
    - protocol_conveyor_FFC8.txt     (протокол испытаний, начинается с ПРОТОКОЛ ИСПЫТАНИЙ №)
    """

    # ИСПРАВЛЕНИЕ: __init__ принимает incident_id первым аргументом
    def __init__(self, incident_id: Optional[str] = None, config_path: str = "./config"):
        super().__init__(incident_id, config_path)
        self.set_table_name("equipment_certificate")

    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'certificate', 'сертификат', 'cert', 'protocol'
        ])

    def get_table_name(self) -> Optional[str]:
        return self._table_name

    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        records = []
        blocks = self._split_certificates(content)
        for block in blocks:
            record = self._parse_certificate_block(block, file_name)
            if record:
                records.append(record)
        return records

    def _split_certificates(self, content: str) -> List[str]:
        """
        Разбивает файл на блоки сертификатов.
        ИСПРАВЛЕНИЕ: добавлен разделитель по строкам из ===,
        которые фактически используются в certificates_equipment.txt.
        """
        # Сначала пробуем разбить по явным разделителям ===...===
        blocks_by_equals = re.split(r'={40,}', content)
        # Убираем пустые блоки и заголовок файла
        meaningful = []
        for block in blocks_by_equals:
            stripped = block.strip()
            if len(stripped) > 50 and any(
                kw in stripped.upper()
                for kw in ['СЕРТИФИКАТ', 'ПРОТОКОЛ']
            ):
                meaningful.append(stripped)

        if meaningful:
            return meaningful

        # Fallback: весь файл как один блок
        return [content]

    def _parse_certificate_block(self, block: str, file_name: str) -> Optional[Dict[str, Any]]:
        cert_number  = self._extract_certificate_number(block)
        if not cert_number:
            return None

        # Определяем срок действия на момент аварии (28.10.2023)
        expiry_date = self._extract_expiry_date(block)
        is_valid = self._check_validity_at_incident(expiry_date)

        record = {
            'certificate_id':       generate_certificate_id(),
            'equipment_id':         None,   # заполняется post-hoc через equipment_mapping
            'certificate_number':   cert_number,
            'certificate_type':     self._detect_certificate_type(block),
            'issuing_body':         self._extract_issuing_body(block),
            'issue_date':           self._extract_issue_date(block),
            'expiry_date':          expiry_date,
            'is_valid_at_incident': is_valid,
            'product_name':         self._extract_product_name(block),
            'serial_number':        self._extract_serial_number(block),
            'incident_id':          self.incident_id,
            'source_file':         file_name,
        }
        return record

    def _check_validity_at_incident(self, expiry_date: Optional[str]) -> Optional[bool]:
        """
        НОВОЕ: проверяет действительность сертификата на дату аварии 28.10.2023.
        В оригинале is_valid_at_incident всегда был None.
        """
        if not expiry_date:
            return None
        # Дата аварии
        INCIDENT_DATE = '2023-10-28'
        return expiry_date >= INCIDENT_DATE

    def _detect_certificate_type(self, block: str) -> str:
        b = block.upper()
        if 'ПРОТОКОЛ ИСПЫТАНИЙ' in b:
            return 'протокол испытаний'
        elif 'ДЕКЛАРАЦИЯ' in b:
            return 'декларация соответствия'
        elif 'СЕРТИФИКАТ СООТВЕТСТВИЯ' in b:
            return 'сертификат соответствия'
        return 'сертификат'

    def _extract_certificate_number(self, block: str) -> Optional[str]:
        patterns = [
            # ПРОТОКОЛ ИСПЫТАНИЙ № 31-17-ИЛ от 29.06.2017
            r'ПРОТОКОЛ\s+ИСПЫТАНИЙ\s*№\s*([A-Z0-9\-\.]+)',
            # СЕРТИФИКАТ СООТВЕТСТВИЯ ЕАЭС RU С-IT.АЖ58.В.04521
            r'СЕРТИФИКАТ\s+СООТВЕТСТВИЯ\s+(?:ЕАЭС\s+|ТС\s+)?([A-Z0-9А-ЯЁ\s\.\-]+?)(?:\n|$)',
            # KZ 3 110 00650
            r'СЕРТИФИКАТ\s+([A-Z]{2}\s*[\d\s]+)',
            r'№\s*([A-Z0-9А-ЯЁ\-\.]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return m.group(1).strip()[:100]
        return None

    def _extract_issuing_body(self, block: str) -> Optional[str]:
        patterns = [
            r'Орган по сертификации:\s*([^\n]+)',
            r'Issued by:\s*([^\n]+)',
            r'Сертификационный центр:\s*([^\n]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def _extract_issue_date(self, block: str) -> Optional[str]:
        patterns = [
            r'Дата\s+(?:выдачи|проведения):\s*(\d{2}\.\d{2}\.\d{4})',
            r'Issue date:\s*(\d{2}\.\d{2}\.\d{4})',
            r'Выдан:\s*(\d{2}\.\d{2}\.\d{4})',
            r'Дата:\s*(\d{2}\.\d{2}\.\d{4})',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return DateParser.parse_to_str(m.group(1))
        return None

    def _extract_expiry_date(self, block: str) -> Optional[str]:
        patterns = [
            r'Действителен до:\s*(\d{2}\.\d{2}\.\d{4})',
            r'Valid until:\s*(\d{2}\.\d{2}\.\d{4})',
            r'Срок действия до:\s*(\d{2}\.\d{2}\.\d{4})',
            r'Expiry date:\s*(\d{2}\.\d{2}\.\d{4})',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return DateParser.parse_to_str(m.group(1))

        # «до 2024 г.» → 2024-12-31
        m = re.search(r'до\s+(\d{4})\s*г', block, re.IGNORECASE)
        if m:
            return f"{m.group(1)}-12-31"

        return None

    def _extract_product_name(self, block: str) -> Optional[str]:
        """НОВОЕ: извлекаем название изделия — нужно для equipment_mapping."""
        patterns = [
            r'Изделие:\s*([^\n]+)',
            r'Оборудование:\s*([^\n]+)',
            r'Product:\s*([^\n]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def _extract_serial_number(self, block: str) -> Optional[str]:
        """НОВОЕ: извлекаем заводской номер — нужно для equipment_mapping."""
        patterns = [
            r'Заводской номер:\s*([^\n]+)',
            r'Серийный номер:\s*([^\n]+)',
            r'Serial number:\s*([^\n]+)',
        ]
        for pattern in patterns:
            m = re.search(pattern, block, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None