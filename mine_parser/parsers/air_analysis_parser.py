# parsers/air_analysis_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_air_sample_id
from text_cleaner import TextCleaner
from date_parser import DateParser


class AirAnalysisParser(BaseParser):
    """
    Парсер для лабораторных анализов воздуха.
    Поддерживает: протоколы анализов воздуха (ПАСС "Комир").
    """
    
    # ID инцидента (будет установлен post-hoc)
    INCIDENT_ID = "INC-2023-001"
    
    # Атмосферные значения для расчетов
    CO2_ATM = 0.03
    O2_ATM = 20.9
    CO_ATM = 0.0
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("air_analysis")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'air_analysis', 'pass_air', 'анализ воздуха', 
            'извещение', 'gas_analysis', 'газовый'
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с анализами воздуха.
        """
        records = []
        
        # Разбиваем на извещения
        notices = re.split(r'--- Извещение № (\d+) от (\d{2}\.\d{2}\.\d{4}) ---', content)
        
        for i in range(1, len(notices), 3):
            notice_num = notices[i]
            notice_date = notices[i + 1]
            notice_content = notices[i + 2]
            
            # Парсим таблицу в извещении
            table_records = self._parse_table(notice_content, notice_date, notice_num, file_name)
            records.extend(table_records)
        
        return records
    
    def _parse_table(self, content: str, notice_date: str, notice_num: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит таблицу с данными анализов"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Место отбора', 'CO2', 'O2', 'CO']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'место' in header:
                indices['sample_point'] = idx
            elif 'co2' in header:
                indices['co2'] = idx
            elif 'o2' in header:
                indices['o2'] = idx
            elif 'co' in header and 'co2' not in header:
                indices['co'] = idx
            elif 'h2' in header:
                indices['h2'] = idx
            elif 'ch4' in header:
                indices['ch4'] = idx
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем данные
            sample_point = parts[indices['sample_point']] if 'sample_point' in indices and indices['sample_point'] < len(parts) else None
            if not sample_point or sample_point.isdigit():
                continue
            
            co2 = self._to_float(parts[indices['co2']]) if 'co2' in indices and indices['co2'] < len(parts) else None
            o2 = self._to_float(parts[indices['o2']]) if 'o2' in indices and indices['o2'] < len(parts) else None
            co = self._to_float(parts[indices['co']]) if 'co' in indices and indices['co'] < len(parts) else None
            h2 = self._to_float(parts[indices['h2']]) if 'h2' in indices and indices['h2'] < len(parts) else None
            ch4 = self._to_float(parts[indices['ch4']]) if 'ch4' in indices and indices['ch4'] < len(parts) else None
            
            # Рассчитываем коэффициенты
            ratios = self._calculate_ratios(co2, o2, co)
            
            # Определяем вывод по пробе
            conclusion = self._determine_conclusion(co2, o2, co, ch4, ratios)
            
            # Парсим дату
            sample_dttm = DateParser.parse_to_str(notice_date)
            
            record = {
                'sample_id': generate_air_sample_id(),
                'incident_id': self.INCIDENT_ID,
                'sample_point': sample_point,
                'sample_dttm': sample_dttm,
                'co2_percent': co2,
                'o2_percent': o2,
                'ch4_percent': ch4,
                'co_percent': co,
                'h2_percent': h2,
                'analyst': None,
                'analyst_laboratory': 'ПАСС "Комир"',
                'r1_co2_o2_ratio': ratios['r1'],
                'r2_co_o2_ratio': ratios['r2'],
                'r3_co_co2_ratio': ratios['r3'],
                'conclusion': conclusion,
                '_source_file': file_name,
                '_notice_num': notice_num
            }
            
            records.append(record)
        
        return records
    
    def _calculate_ratios(self, co2: Optional[float], o2: Optional[float], co: Optional[float]) -> Dict[str, Optional[float]]:
        """
        Рассчитывает коэффициенты R1, R2, R3.
        
        R1 = ΔCO₂/ΔO₂ = (CO₂_проба - CO₂_атм) / (O₂_атм - O₂_проба)
        R2 = ΔCO/ΔO₂ = (CO_проба - CO_атм) / (O₂_атм - O₂_проба)
        R3 = ΔCO/ΔCO₂ = (CO_проба - CO_атм) / (CO₂_проба - CO₂_атм)
        """
        if co2 is None or o2 is None:
            return {'r1': None, 'r2': None, 'r3': None}
        
        delta_o2 = self.O2_ATM - o2
        
        # R1
        if delta_o2 != 0:
            r1 = (co2 - self.CO2_ATM) / delta_o2
        else:
            r1 = None
        
        # R2
        if co is not None and delta_o2 != 0:
            r2 = (co - self.CO_ATM) / delta_o2
        else:
            r2 = None
        
        # R3
        if co is not None and (co2 - self.CO2_ATM) != 0:
            r3 = (co - self.CO_ATM) / (co2 - self.CO2_ATM)
        else:
            r3 = None
        
        return {
            'r1': round(r1, 4) if r1 is not None else None,
            'r2': round(r2, 4) if r2 is not None else None,
            'r3': round(r3, 4) if r3 is not None else None
        }
    
    def _determine_conclusion(self, co2: Optional[float], o2: Optional[float], 
                              co: Optional[float], ch4: Optional[float],
                              ratios: Dict[str, Optional[float]]) -> Optional[str]:
        """Определяет вывод по пробе воздуха"""
        conclusions = []
        
        # Проверка на наличие метана
        if ch4 and ch4 > 0:
            conclusions.append(f"обнаружен метан (CH4: {ch4}%)")
        
        # Проверка на снижение кислорода
        if o2 and o2 < 18:
            conclusions.append(f"снижение содержания кислорода до {o2}%")
        
        # Проверка на признаки эндогенного пожара
        r1 = ratios.get('r1')
        r2 = ratios.get('r2')
        r3 = ratios.get('r3')
        
        if r1 is not None:
            if r1 < 0.4:
                conclusions.append("R1<0.4 - возможен эндогенный пожар")
            elif r1 > 0.6:
                conclusions.append("R1>0.6 - окислительные процессы")
        
        if r2 is not None and r2 > 0:
            conclusions.append(f"наличие CO (R2={r2})")
        
        if r3 is not None and r3 > 0:
            conclusions.append(f"соотношение CO/CO2 = {r3}")
        
        if not conclusions:
            return "норма"
        
        return '; '.join(conclusions)