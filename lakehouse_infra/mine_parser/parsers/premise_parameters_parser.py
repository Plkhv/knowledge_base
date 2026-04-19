# parsers/premise_parameters_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_premise_param_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser


class PremiseParametersParser(BaseParser):
    """
    Парсер для параметров выработок (вентиляция, дегазация, пылевзрывозащита).
    Объединяет данные из разных источников в единую таблицу premise_parameters.
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    # Норматив по негорючим веществам
    NORMATIVE_NONCOMBUSTIBLE = 85.0
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("premise_parameters")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'ventilation', 'dust_control', 'degassing', 'проветриван',
            'пылевзрыв', 'дегазац', 'vacuum_pump', 'velocity', 'моделирован'  # ← добавить
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с параметрами выработок.
        Определяет тип по имени файла и содержимому.
        """
        file_lower = file_name.lower()
        
        if 'ventilation' in file_lower or 'проветриван' in file_lower:
            records = self._parse_ventilation(content, file_name)
        elif 'dust' in file_lower or 'пыль' in file_lower:
            records = self._parse_dust_control(content, file_name)
        elif 'degassing' in file_lower or 'дегазац' in file_lower or 'vacuum' in file_lower:
            records = self._parse_degassing(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        # Добавляем incident_id
        for record in records:
            record['incident_id'] = self.INCIDENT_ID
        
        return records
    
    def _parse_ventilation(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит вентиляционные параметры"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Выработка', 'Расход', 'Скорость']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'выработк' in header or 'наименован' in header:
                indices['location'] = idx
            elif 'сечени' in header:
                indices['cross_section'] = idx
            elif 'расход' in header:
                indices['air_flow'] = idx
            elif 'скорост' in header:
                indices['air_velocity'] = idx
            elif 'ch4' in header or 'метан' in header:
                indices['ch4'] = idx
        
        # Извлекаем дату из заголовка
        date_match = re.search(r'Дата:\s*(\d{2}\.\d{2}\.\d{4})', content)
        measurement_date = date_match.group(1) if date_match else None
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            location = parts[indices['location']] if 'location' in indices and indices['location'] < len(parts) else None
            if not location:
                continue
            
            record = {
                'param_id': generate_premise_param_id(),
                'incident_id': self.INCIDENT_ID,
                'location': location,
                'measurement_date': DateParser.parse_to_str(measurement_date) if measurement_date else None,
                'param_type': 'ventilation',
                
                # Вентиляционные параметры
                'air_flow_m3_min': self._to_float(parts[indices['air_flow']]) if 'air_flow' in indices and indices['air_flow'] < len(parts) else None,
                'air_velocity_mps': self._to_float(parts[indices['air_velocity']]) if 'air_velocity' in indices and indices['air_velocity'] < len(parts) else None,
                'cross_section_m2': self._to_float(parts[indices['cross_section']]) if 'cross_section' in indices and indices['cross_section'] < len(parts) else None,
                'ch4_concentration_percent': self._to_float(parts[indices['ch4']]) if 'ch4' in indices and indices['ch4'] < len(parts) else None,
                
                # Остальные поля — NULL
                'leakage_coefficient': None,
                'distribution_coefficient': None,
                'gas_flow_m3_min': None,
                'ch4_flow_m3_min': None,
                'vacuum_pressure_mmH2O': None,
                'degassing_efficiency_percent': None,
                'inert_dust_applied_kg': None,
                'noncombustible_content_percent': None,
                'normative_noncombustible_percent': self.NORMATIVE_NONCOMBUSTIBLE,
                'is_compliant': None,
                'dust_removal_water_m3': None,
                'dust_removal_frequency': None,
                'water_spray_present': None,
                'water_spray_flow_l_min': None,
                
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    def _parse_dust_control(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит параметры пылевзрывозащиты"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Выработка', 'Осланцевание', 'Негорючие']):
                header_line = line
                break
        
        # Извлекаем периодичность
        frequency_match = re.search(r'Периодичность осланцевания:\s*([^\n]+)', content)
        dust_removal_frequency = frequency_match.group(1).strip() if frequency_match else None
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'выработк' in header:
                indices['location'] = idx
            elif 'осланцев' in header:
                indices['inert_dust'] = idx
            elif 'негорюч' in header:
                indices['noncombustible'] = idx
            elif 'норматив' in header:
                indices['normative'] = idx
            elif 'соответств' in header:
                indices['compliant'] = idx
            elif 'орошени' in header:
                indices['water_spray'] = idx
            elif 'расход' in header:
                indices['water_flow'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            location = parts[indices['location']] if 'location' in indices and indices['location'] < len(parts) else None
            if not location:
                continue
            
            # Определяем соответствие норме
            noncombustible = self._to_float(parts[indices['noncombustible']]) if 'noncombustible' in indices and indices['noncombustible'] < len(parts) else None
            is_compliant = None
            if noncombustible is not None:
                is_compliant = 1 if noncombustible >= self.NORMATIVE_NONCOMBUSTIBLE else 0
            
            # Наличие орошения
            water_spray_str = parts[indices['water_spray']] if 'water_spray' in indices and indices['water_spray'] < len(parts) else None
            water_spray_present = 1 if water_spray_str and water_spray_str.lower() in ['да', 'yes', '+'] else 0
            
            record = {
                'param_id': generate_premise_param_id(),
                'incident_id': self.INCIDENT_ID,
                'location': location,
                'measurement_date': None,  # квартальный отчет без конкретной даты
                'param_type': 'dust',
                
                # Параметры пылевзрывозащиты
                'inert_dust_applied_kg': self._to_float(parts[indices['inert_dust']]) if 'inert_dust' in indices and indices['inert_dust'] < len(parts) else None,
                'noncombustible_content_percent': noncombustible,
                'normative_noncombustible_percent': self.NORMATIVE_NONCOMBUSTIBLE,
                'is_compliant': is_compliant,
                'dust_removal_water_m3': None,
                'dust_removal_frequency': dust_removal_frequency,
                'water_spray_present': water_spray_present,
                'water_spray_flow_l_min': self._to_float(parts[indices['water_flow']]) if 'water_flow' in indices and indices['water_flow'] < len(parts) else None,
                
                # Остальные поля — NULL
                'air_flow_m3_min': None,
                'air_velocity_mps': None,
                'cross_section_m2': None,
                'ch4_concentration_percent': None,
                'leakage_coefficient': None,
                'distribution_coefficient': None,
                'gas_flow_m3_min': None,
                'ch4_flow_m3_min': None,
                'vacuum_pressure_mmH2O': None,
                'degassing_efficiency_percent': None,
                
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    # parsers/premise_parameters_parser.py - добавьте этот метод

    def _parse_degassing(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит параметры дегазации"""
        records = []
        lines = content.split('\n')
        
        # Парсим станционные замеры
        in_station_table = False
        for line in lines:
            if '|' in line:
                if 'Дата' in line and 'Время' in line and 'Расход' in line:
                    in_station_table = True
                    continue
                
                if in_station_table and '|' in line and not line.strip().startswith('---'):
                    parts = [p.strip() for p in line.split('|')]
                    if len(parts) >= 5:
                        date_str = parts[0] if len(parts) > 0 else None
                        time_str = parts[1] if len(parts) > 1 else None
                        gas_flow = self._to_float(parts[2]) if len(parts) > 2 else None
                        ch4 = self._to_float(parts[3]) if len(parts) > 3 else None
                        pressure = self._to_float(parts[4]) if len(parts) > 4 else None
                        
                        if date_str and time_str:
                            measurement_date = DateParser.parse_to_str(f"{date_str} {time_str}", '%Y-%m-%d %H:%M:%S')
                            
                            ch4_flow = None
                            if gas_flow is not None and ch4 is not None:
                                ch4_flow = round(gas_flow * (ch4 / 100), 2)
                            
                            record = {
                                'param_id': generate_premise_param_id(),
                                'incident_id': self.INCIDENT_ID,
                                'location': 'Вакуум-насосная станция (Западный ствол)',
                                'measurement_date': measurement_date,
                                'param_type': 'degassing',
                                'gas_flow_m3_min': gas_flow,
                                'ch4_concentration_percent': ch4,
                                'ch4_flow_m3_min': ch4_flow,
                                'vacuum_pressure_mmH2O': pressure,
                                '_source_file': file_name
                            }
                            records.append(record)
    
        # Парсим замеры на перемычках
        for line in lines:
            if '№1640' in line or '№1641' in line or '№1644' in line:
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 4:
                    location = parts[0] if len(parts) > 0 else None
                    gas_flow = self._to_float(parts[1]) if len(parts) > 1 else None
                    ch4 = self._to_float(parts[2]) if len(parts) > 2 else None
                    pressure = self._to_float(parts[3]) if len(parts) > 3 else None
                    
                    if location:
                        ch4_flow = None
                        if gas_flow is not None and ch4 is not None:
                            ch4_flow = round(gas_flow * (ch4 / 100), 2)
                        
                        record = {
                            'param_id': generate_premise_param_id(),
                            'incident_id': self.INCIDENT_ID,
                            'location': location,
                            'measurement_date': '2023-10-24',
                            'param_type': 'degassing',
                            'gas_flow_m3_min': gas_flow,
                            'ch4_concentration_percent': ch4,
                            'ch4_flow_m3_min': ch4_flow,
                            'vacuum_pressure_mmH2O': pressure,
                            '_source_file': file_name
                        }
                        records.append(record)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг параметров выработок"""
        records = []
        
        # Ищем упоминания выработок с параметрами
        patterns = [
            r'(ВШ\s+[0-9К-]+).*?(\d+\.?\d*)\s*м³/мин',
            r'(КШ\s+[0-9К-]+).*?(\d+\.?\d*)\s*м³/мин',
            r'(лава\s+[0-9К-]+).*?(\d+\.?\d*)\s*м³/мин',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                location = match[0]
                air_flow = self._to_float(match[1])
                
                record = {
                    'param_id': generate_premise_param_id(),
                    'incident_id': self.INCIDENT_ID,
                    'location': location,
                    'measurement_date': None,
                    'param_type': 'ventilation',
                    'air_flow_m3_min': air_flow,
                    '_source_file': file_name
                }
                records.append(record)
        
        return records