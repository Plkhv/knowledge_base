# parsers/equipment_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_equipment_id
from utils.text_cleaner import TextCleaner


class EquipmentParser(BaseParser):
    """
    Парсер для паспортов оборудования.
    Поддерживает: комбайны, конвейеры, крепь.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("equipment")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'combine', 'комбайн', 'conveyor', 'конвейер', 
            'support', 'крепь', 'equipment', 'паспорт',
            'pasport', 'spec', 'спецификаци', 'fs300', 'ffc8', 'glinik'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит паспорт оборудования и возвращает запись.
        """
        # Определяем тип оборудования по имени файла
        equipment_type = self._detect_equipment_type(file_name, content)
        
        record = {
            'equipment_id': generate_equipment_id(),
            'equipment_type': equipment_type,
            'model': self._extract_model(content),
            'serial_number': self._extract_serial_number(content),
            'manufacturer': self._extract_manufacturer(content),
            'year_of_manufacture': self._extract_year(content),
            'technical_condition': self._extract_technical_condition(content, file_name),
            'spark_safety_class': self._extract_spark_safety(content),
            'location_at_incident': self._extract_location(content, file_name),
            'manufacturer_requirements': self._extract_requirements(content),
            '_source_file': file_name
        }
        
        # Очищаем все строковые поля
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = TextCleaner.clean_whitespace(value)
        
        return [record]
    
    def _detect_equipment_type(self, file_name: str, content: str) -> str:
        """Определяет тип оборудования по имени файла и содержимому"""
        name_lower = file_name.lower()
        content_lower = content.lower()
        
        if 'combine' in name_lower or 'комбайн' in name_lower or 'fs300' in content_lower:
            return 'очистной комбайн'
        elif 'conveyor' in name_lower or 'конвейер' in name_lower or 'ffc' in content_lower:
            return 'скребковый конвейер'
        elif 'support' in name_lower or 'крепь' in name_lower or 'glinik' in content_lower:
            return 'механизированная крепь'
        elif 'датчик' in name_lower or 'sensor' in name_lower:
            return 'датчик'
        else:
            return 'оборудование'
    
    def _extract_model(self, content: str) -> Optional[str]:
        """Извлекает модель оборудования"""
        patterns = [
            r'Тип:\s*([A-Z0-9/.-]+)',
            r'Модель:\s*([A-Z0-9/.-]+)',
            r'Оборудование:\s*([A-Z0-9/.-]+)',
            r'Тип\s+оборудования:\s*([A-Z0-9/.-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_serial_number(self, content: str) -> Optional[str]:
        """Извлекает заводской номер"""
        patterns = [
            r'Заводской номер:\s*([A-Z0-9/-]+)',
            r'Серийный номер:\s*([A-Z0-9/-]+)',
            r'Serial number:\s*([A-Z0-9/-]+)',
            r'Зав\.\s*№\s*:\s*([A-Z0-9/-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_manufacturer(self, content: str) -> Optional[str]:
        """Извлекает название производителя"""
        patterns = [
            r'Завод-изготовитель:\s*([^\n]+)',
            r'Производитель:\s*([^\n]+)',
            r'Manufacturer:\s*([^\n]+)',
            r'Изготовитель:\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_year(self, content: str) -> Optional[int]:
        """Извлекает год выпуска"""
        patterns = [
            r'Дата выпуска:\s*\d{2}\.\d{2}\.(\d{4})',
            r'Год выпуска:\s*(\d{4})',
            r'Year of manufacture:\s*(\d{4})',
            r'Изготовлен[о]?\s*:?\s*(\d{4})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return int(match.group(1))
        return None
    
    def _extract_spark_safety(self, content: str) -> Optional[str]:
        """Извлекает класс искробезопасности"""
        patterns = [
            r'Взрывозащита:\s*([^\n]+)',
            r'Класс\s+искробезопасности:\s*([^\n]+)',
            r'Ex\s+([A-Z0-9\s]+)',
            r'Взрывобезопасность:\s*([^\n]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def _extract_technical_condition(self, content: str, file_name: str) -> Optional[str]:
        """Извлекает информацию о техническом состоянии"""
        # Ищем информацию об износе, повреждениях
        patterns = [
            r'([^\.]*износ[^\.]+\.)',
            r'([^\.]*поврежден[^\.]+\.)',
            r'([^\.]*состояние[^\.]+\.)',
            r'([^\.]*не являются искробезопасными[^\.]+\.)',
        ]
        
        conditions = []
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            conditions.extend(matches)
        
        if conditions:
            return '; '.join([c.strip() for c in conditions[:3]])
        
        # Если нет информации, возвращаем None
        return None
    
    def _extract_location(self, content: str, file_name: str) -> Optional[str]:
        """Извлекает информацию о расположении оборудования"""
        patterns = [
            r'лава\s+([0-9К-]+)',
            r'КШ\s+([0-9К-]+)',
            r'ВШ\s+([0-9К-]+)',
            r'участок\s+№?\s*([0-9]+)',
            r'выемочный\s+участок\s+([0-9К-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                location = match.group(1).strip()
                # Добавляем контекст
                if 'лава' in pattern:
                    return f"лава {location}"
                elif 'КШ' in pattern:
                    return f"конвейерный штрек {location}"
                elif 'ВШ' in pattern:
                    return f"вентиляционный штрек {location}"
                else:
                    return location
        
        return None
    
    def _extract_requirements(self, content: str) -> Optional[str]:
        """Извлекает особые требования завода-изготовителя"""
        patterns = [
            # Раздел 9.2.1
            r'Раздел\s+9\.2\.1[^\n]+\n"?([А-Я0-9\s.,!?-]+)"?',
            # Раздел 7
            r'Раздел\s+7[^\n]+\n"?([А-Я0-9\s.,!?-]+)"?',
            # Запрещается
            r'Запрещается[^\.]+\.',
            # Требования безопасности
            r'Требования\s+безопасности[^\.]+\.',
        ]
        
        requirements = []
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            for match in matches:
                req = self._clean_text(match)
                if len(req) > 20:  # Отсекаем слишком короткие фразы
                    requirements.append(req)
        
        if requirements:
            return '; '.join(requirements[:3])
        
        return None