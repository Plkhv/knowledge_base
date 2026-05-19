# parsers/air_analysis_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional

from parsers.base_parser import BaseParser
from utils.id_generator import generate_air_sample_id
from parsers.date_parser import DateParser


class AirAnalysisParser(BaseParser):
    """
    Парсер для лабораторных анализов воздуха.
    Поддерживает: протоколы анализов воздуха (ПАСС "Комир").
    """

    # Атмосферные значения для расчётов (константы методики)
    CO2_ATM = 0.03
    O2_ATM = 20.9
    CO_ATM = 0.0

    # ИСПРАВЛЕНИЕ: __init__ принимает incident_id первым аргументом,
    # как передаёт ParserFactory. В оригинале этого аргумента не было —
    # ParserFactory падал с TypeError при AirAnalysisParser(inc_id).
    def __init__(self, incident_id: Optional[str] = None, config_path: str = "./config"):
        super().__init__(incident_id, config_path)
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
        records = []

        # Разбиваем на извещения по разделителю
        # Формат: --- Извещение № 43 от 25.07.2023 ---
        parts = re.split(r'---\s*Извещение\s*№\s*(\d+)\s*от\s*(\d{2}\.\d{2}\.\d{4})\s*---', content)

        # parts[0] — заголовок файла, затем тройки: [номер, дата, содержимое]
        for i in range(1, len(parts), 3):
            if i + 2 >= len(parts):
                break
            notice_num  = parts[i].strip()
            notice_date = parts[i + 1].strip()
            notice_body = parts[i + 2]

            table_records = self._parse_table(notice_body, notice_date, notice_num, file_name)
            records.extend(table_records)

        return records

    def _parse_table(self, content: str, notice_date: str, notice_num: str,
                     file_name: str) -> List[Dict[str, Any]]:
        records = []
        lines = content.split('\n')

        # Ищем строку заголовков таблицы
        header_line = None
        header_idx = None
        for idx, line in enumerate(lines):
            if '|' in line and any(kw in line for kw in ['Место отбора', 'CO2', 'O2', 'CO']):
                header_line = line
                header_idx = idx
                break

        if not header_line:
            return []

        # Определяем индексы колонок по заголовкам
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        for idx, header in enumerate(headers):
            if 'место' in header:
                indices['sample_point'] = idx
            elif 'co2' in header:
                indices['co2'] = idx
            elif 'o2' in header and 'co2' not in header:
                indices['o2'] = idx
            elif header.strip() == 'co, %' or (header.strip() == 'co' and 'co2' not in header):
                indices['co'] = idx
            elif 'h2' in header:
                indices['h2'] = idx
            elif 'ch4' in header:
                indices['ch4'] = idx

        # Парсим строки данных (пропускаем разделители ---)
        for line in lines[header_idx + 1:]:
            if '|' not in line:
                continue
            stripped = line.strip()
            if stripped.startswith('---') or stripped.startswith('===') or stripped.startswith('|-'):
                continue

            parts = [p.strip() for p in line.split('|')]

            # Извлекаем место отбора
            sample_point = None
            if 'sample_point' in indices and indices['sample_point'] < len(parts):
                sp = parts[indices['sample_point']]
                # Пропускаем строки с номером пробы (только цифра) или пустые
                if sp and not sp.isdigit() and sp != '№ пробы':
                    sample_point = sp
            if not sample_point:
                continue

            co2 = self._to_float(parts[indices['co2']]) if 'co2' in indices and indices['co2'] < len(parts) else None
            o2  = self._to_float(parts[indices['o2']])  if 'o2'  in indices and indices['o2']  < len(parts) else None
            co  = self._to_float(parts[indices['co']])  if 'co'  in indices and indices['co']  < len(parts) else None
            h2  = self._to_float(parts[indices['h2']])  if 'h2'  in indices and indices['h2']  < len(parts) else None
            ch4 = self._to_float(parts[indices['ch4']]) if 'ch4' in indices and indices['ch4'] < len(parts) else None

            ratios     = self._calculate_ratios(co2, o2, co)
            conclusion = self._determine_conclusion(co2, o2, co, ch4, ratios)
            sample_dttm = DateParser.parse_to_str(notice_date)

            delta_o2  = round(self.O2_ATM - o2, 4)   if o2  is not None else None
            delta_co2 = round(co2 - self.CO2_ATM, 4) if co2 is not None else None
            delta_co  = round(co  - self.CO_ATM,  4) if co  is not None else None
            is_oxidation = (ratios['r1'] < 2.5) if ratios['r1'] is not None else None

            record = {
                'sample_id':           generate_air_sample_id(),
                'incident_id':         self.incident_id,
                'sample_point':        sample_point,
                'sample_dttm':         sample_dttm,
                'co2_percent':         co2,
                'o2_percent':          o2,
                'ch4_percent':         ch4,
                'co_percent':          co,
                'h2_percent':          h2,
                'analyst':             None,
                'analyst_laboratory':  'ПАСС "Комир"',
                'r1_o2_co2_ratio':     ratios['r1'],
                'r2_co_o2_ratio':      ratios['r2'],
                'r3_co_co2_ratio':     ratios['r3'],
                'delta_o2':            delta_o2,
                'delta_co2':           delta_co2,
                'delta_co':            delta_co,
                'is_oxidation':        is_oxidation,
                'conclusion':          conclusion,
                'source_file':         file_name
            }
            records.append(record)

        return records

    def _calculate_ratios(self, co2: Optional[float], o2: Optional[float],
                          co: Optional[float]) -> Dict[str, Optional[float]]:
        """
        Рассчитывает коэффициенты R1, R2, R3 по методике диагностики
        эндогенных пожаров (мат. модель ВКР, раздел 2.2.6):

        ИСПРАВЛЕНИЕ: в оригинале R1 был перевёрнут.
        Оригинал:  R1 = (CO2_проба - CO2_атм) / (O2_атм - O2_проба)  — это ΔCO₂/ΔO₂
        Правильно: R1 = ΔO₂ / ΔCO₂ = (O2_атм - O2_проба) / (CO2_проба - CO2_атм)

        R2 = ΔCO / ΔO₂ = (CO_проба - CO_атм) / (O2_атм - O2_проба)
        R3 = ΔCO / ΔCO₂ = (CO_проба - CO_атм) / (CO2_проба - CO2_атм)
        """
        if co2 is None or o2 is None:
            return {'r1': None, 'r2': None, 'r3': None}

        delta_o2  = self.O2_ATM  - o2
        delta_co2 = co2 - self.CO2_ATM

        # R1 = ΔO₂ / ΔCO₂
        r1 = (delta_o2 / delta_co2) if delta_co2 != 0 else None

        # R2 = ΔCO / ΔO₂
        if co is not None and delta_o2 != 0:
            r2 = (co - self.CO_ATM) / delta_o2
        else:
            r2 = None

        # R3 = ΔCO / ΔCO₂
        if co is not None and delta_co2 != 0:
            r3 = (co - self.CO_ATM) / delta_co2
        else:
            r3 = None

        return {
            'r1': round(r1, 4) if r1 is not None else None,
            'r2': round(r2, 4) if r2 is not None else None,
            'r3': round(r3, 4) if r3 is not None else None,
        }

    def _determine_conclusion(self, co2: Optional[float], o2: Optional[float],
                              co: Optional[float], ch4: Optional[float],
                              ratios: Dict[str, Optional[float]]) -> Optional[str]:
        conclusions = []

        if ch4 and ch4 > 0:
            conclusions.append(f"обнаружен метан (CH4: {ch4}%)")

        if o2 and o2 < 18:
            conclusions.append(f"снижение O2 до {o2}%")

        r1 = ratios.get('r1')
        r2 = ratios.get('r2')
        r3 = ratios.get('r3')

        # Пороговое значение R1_crit = 2.5 (мат. модель ВКР)
        if r1 is not None:
            if r1 < 2.5:
                conclusions.append(f"R1={r1} < 2.5 — признак окислительного процесса / эндогенного пожара")
            else:
                conclusions.append(f"R1={r1} ≥ 2.5 — норма")

        if r2 is not None and r2 > 0:
            conclusions.append(f"наличие CO (R2={r2})")

        if r3 is not None and r3 > 0:
            conclusions.append(f"CO/CO2 = {r3} (R3)")

        if not conclusions:
            return "норма"

        return '; '.join(conclusions)