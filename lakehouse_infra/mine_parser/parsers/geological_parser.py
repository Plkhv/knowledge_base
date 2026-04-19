# parsers/geological_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional
from pathlib import Path

from parsers.base_parser import BaseParser
from utils.id_generator import generate_geology_id
from utils.text_cleaner import TextCleaner
from parsers.date_parser import DateParser


class GeologicalParser(BaseParser):
    """
    Парсер для геологического строения.
    Поддерживает: структурные колонки, геологические разрезы, отчеты по газоносности.
    """
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("geological_structure")
    
    def supports(self, file_name: str) -> bool:
        name_lower = file_name.lower()
        return any(kw in name_lower for kw in [
            'structural', 'геологическ', 'колонк', 'разрез', 
            'газоносност', 'geological'  # ← добавить
        ])
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Парсит файл с геологическими данными.
        Возвращает список записей (по слоям/пластам).
        """
        file_lower = file_name.lower()
        
        if 'структурн' in file_lower or 'колонк' in file_lower:
            records = self._parse_structural_column(content, file_name)
        elif 'разрез' in file_lower:
            records = self._parse_geological_section(content, file_name)
        elif 'газоносност' in file_lower:
            records = self._parse_gas_content_report(content, file_name)
        else:
            records = self._parse_generic(content, file_name)
        
        return records
    
    def _parse_structural_column(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит структурную колонку"""
        records = []
        
        # Извлекаем общую информацию
        mine_name = self._extract_mine_name(content)
        panel_name = self._extract_panel_name(content)
        
        # Глубина
        depth_match = re.search(r'Глубина от поверхности:\s*(\d+)-(\d+)\s*м', content)
        depth_from = float(depth_match.group(1)) if depth_match else None
        depth_to = float(depth_match.group(2)) if depth_match else None
        
        # Текущая позиция в разрезе
        current_depth = depth_from if depth_from else 720
        
        # Разбиваем на секции
        sections = re.split(r'[-]{10,}', content)
        
        for section in sections:
            # Определяем тип слоя
            if 'КРОВЛЯ' in section:
                rock_type = 'кровля'
                layer_name = None
            elif 'ПЛАСТ К3' in section:
                layer_name = 'К3'
                rock_type = 'уголь'
            elif 'МЕЖПЛАСТЬЕ' in section:
                rock_type = 'межпластье'
                layer_name = None
            elif 'ПЛАСТ К2' in section:
                layer_name = 'К2'
                rock_type = 'уголь'
            elif 'ПОЧВА' in section:
                rock_type = 'почва'
                layer_name = None
            else:
                continue
            
            # Извлекаем мощность
            thickness = self._extract_thickness(section)
            
            # Извлекаем газоносность
            gas_content = self._extract_gas_content(section, layer_name)
            
            # Извлекаем описание
            description = self._extract_description(section)
            
            record = {
                'structure_id': generate_geology_id(),
                'mine_name': mine_name,
                'panel_name': panel_name,
                'layer_name': layer_name,
                'depth_from_m': current_depth,
                'depth_to_m': current_depth + thickness if thickness else None,
                'rock_type': rock_type,
                'thickness_m': thickness,
                'gas_content_m3_per_ton': gas_content,
                'description': description,
                'is_working_layer': 1 if layer_name == 'К3' else 0,
                'is_satellite': 1 if layer_name == 'К2' else 0,
                '_source_file': file_name
            }
            
            # Обновляем текущую глубину
            if thickness:
                current_depth += thickness
            
            records.append(record)
        
        return records
    
    def _parse_geological_section(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит геологический разрез (таблица с | разделителем)"""
        records = []
        lines = content.split('\n')
        
        # Ищем строку с заголовками
        header_line = None
        for line in lines:
            if '|' in line and any(keyword in line for keyword in ['Интервал', 'Порода', 'Мощность']):
                header_line = line
                break
        
        if not header_line:
            return []
        
        # Определяем индексы колонок
        headers = [h.strip().lower() for h in header_line.split('|')]
        indices = {}
        
        for idx, header in enumerate(headers):
            if 'интервал' in header:
                indices['interval'] = idx
            elif 'пород' in header:
                indices['rock_type'] = idx
            elif 'мощност' in header:
                indices['thickness'] = idx
            elif 'описани' in header:
                indices['description'] = idx
            elif 'газоносност' in header or 'ch4' in header:
                indices['gas_content'] = idx
        
        # Извлекаем общую информацию
        mine_name = self._extract_mine_name(content)
        panel_name = self._extract_panel_name(content)
        
        # Парсим строки данных
        for line in lines:
            if '|' not in line or line == header_line:
                continue
            if line.strip().startswith('---') or line.strip().startswith('==='):
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) < 3:
                continue
            
            # Извлекаем интервал
            interval_str = parts[indices['interval']] if 'interval' in indices and indices['interval'] < len(parts) else ""
            depth_from, depth_to = self._parse_interval(interval_str)
            
            # Извлекаем тип породы
            rock_type_raw = parts[indices['rock_type']] if 'rock_type' in indices and indices['rock_type'] < len(parts) else ""
            
            # Извлекаем мощность
            thickness = self._to_float(parts[indices['thickness']]) if 'thickness' in indices and indices['thickness'] < len(parts) else None
            
            # Извлекаем описание
            description = parts[indices['description']] if 'description' in indices and indices['description'] < len(parts) else None
            
            # Извлекаем газоносность
            gas_content = self._to_float(parts[indices['gas_content']]) if 'gas_content' in indices and indices['gas_content'] < len(parts) else None
            
            # Определяем название пласта
            layer_name = None
            is_working = 0
            is_satellite = 0
            
            if 'уголь' in rock_type_raw.lower():
                if 'К3' in content or 'К3' in rock_type_raw:
                    layer_name = 'К3'
                    is_working = 1
                elif 'К2' in content or 'К2' in rock_type_raw:
                    layer_name = 'К2'
                    is_satellite = 1
            
            record = {
                'structure_id': generate_geology_id(),
                'mine_name': mine_name,
                'panel_name': panel_name,
                'layer_name': layer_name,
                'depth_from_m': depth_from,
                'depth_to_m': depth_to,
                'rock_type': rock_type_raw,
                'thickness_m': thickness,
                'gas_content_m3_per_ton': gas_content,
                'description': description,
                'is_working_layer': is_working,
                'is_satellite': is_satellite,
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    def _parse_gas_content_report(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Парсит отчет по газоносности"""
        records = []
        
        # Извлекаем общую информацию
        mine_name = self._extract_mine_name(content)
        panel_name = self._extract_panel_name(content)
        
        # Вычисляем среднюю газоносность
        avg_match = re.search(r'Среднее значение CH4:\s*([\d\.]+)\s*м³/т', content)
        avg_gas = self._to_float(avg_match.group(1)) if avg_match else None
        
        # Создаем запись для пласта К3
        record = {
            'structure_id': generate_geology_id(),
            'mine_name': mine_name,
            'panel_name': panel_name,
            'layer_name': 'К3',
            'depth_from_m': None,
            'depth_to_m': None,
            'rock_type': 'уголь',
            'thickness_m': None,
            'gas_content_m3_per_ton': avg_gas,
            'description': self._extract_conclusion(content),
            'is_working_layer': 1,
            'is_satellite': 0,
            '_source_file': file_name
        }
        
        records.append(record)
        
        # Также создаем запись для пласта К2 (если есть данные)
        k2_match = re.search(r'К2.*?(\d+\.?\d*)\s*м³/т', content, re.IGNORECASE)
        if k2_match:
            record_k2 = {
                'structure_id': generate_geology_id(),
                'mine_name': mine_name,
                'panel_name': panel_name,
                'layer_name': 'К2',
                'depth_from_m': None,
                'depth_to_m': None,
                'rock_type': 'уголь',
                'thickness_m': None,
                'gas_content_m3_per_ton': self._to_float(k2_match.group(1)),
                'description': None,
                'is_working_layer': 0,
                'is_satellite': 1,
                '_source_file': file_name
            }
            records.append(record_k2)
        
        return records
    
    def _parse_generic(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """Универсальный парсинг геологических данных"""
        records = []
        
        mine_name = self._extract_mine_name(content)
        panel_name = self._extract_panel_name(content)
        
        # Ищем упоминания пластов
        layers = []
        
        if 'К3' in content:
            layers.append('К3')
        if 'К2' in content:
            layers.append('К2')
        
        for layer in layers:
            # Ищем газоносность
            gas_pattern = rf'{layer}.*?(\d+\.?\d*)\s*м³/т'
            gas_match = re.search(gas_pattern, content, re.IGNORECASE)
            gas_content = self._to_float(gas_match.group(1)) if gas_match else None
            
            # Ищем мощность
            thickness_pattern = rf'{layer}.*?мощность.*?(\d+\.?\d*)\s*м'
            thickness_match = re.search(thickness_pattern, content, re.IGNORECASE)
            thickness = self._to_float(thickness_match.group(1)) if thickness_match else None
            
            record = {
                'structure_id': generate_geology_id(),
                'mine_name': mine_name,
                'panel_name': panel_name,
                'layer_name': layer,
                'depth_from_m': None,
                'depth_to_m': None,
                'rock_type': 'уголь' if layer in ['К2', 'К3'] else 'порода',
                'thickness_m': thickness,
                'gas_content_m3_per_ton': gas_content,
                'description': None,
                'is_working_layer': 1 if layer == 'К3' else 0,
                'is_satellite': 1 if layer == 'К2' else 0,
                '_source_file': file_name
            }
            
            records.append(record)
        
        return records
    
    def _extract_mine_name(self, content: str) -> str:
        """Извлекает название шахты"""
        match = re.search(r'Шахта:\s*([^\n]+)', content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return 'им. Костенко'
    
    def _extract_panel_name(self, content: str) -> Optional[str]:
        """Извлекает название участка"""
        match = re.search(r'Участок:\s*([^\n]+)', content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None
    
    def _extract_thickness(self, text: str) -> Optional[float]:
        """Извлекает мощность слоя"""
        patterns = [
            r'Мощность:\s*(\d+\.?\d*)\s*м',
            r'Полная мощность:\s*(\d+\.?\d*)\s*м',
            r'Вынимаемая мощность:\s*(\d+\.?\d*)\s*м',
            r'(\d+\.?\d*)\s*м\s*(?:мощность|толщина)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._to_float(match.group(1))
        return None
    
    def _extract_gas_content(self, text: str, layer_name: Optional[str]) -> Optional[float]:
        """Извлекает газоносность"""
        patterns = [
            r'Газоносность:\s*(\d+\.?\d*)\s*м³/т',
            r'Газоносность\s+CH4:\s*(\d+\.?\d*)\s*м³/т',
            r'(\d+\.?\d*)\s*м³/т',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._to_float(match.group(1))
        
        # Если есть диапазон
        range_match = re.search(r'(\d+\.?\d*)-(\d+\.?\d*)\s*м³/т', text)
        if range_match:
            min_val = self._to_float(range_match.group(1))
            max_val = self._to_float(range_match.group(2))
            if min_val and max_val:
                return (min_val + max_val) / 2
        
        return None
    
    def _extract_description(self, text: str) -> Optional[str]:
        """Извлекает описание слоя"""
        # Ищем строки с описанием
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if line and not line.startswith('-') and not line.startswith('='):
                if any(keyword in line.lower() for keyword in ['угольные пачки', 'прослойки', 'строение']):
                    return line
        return None
    
    def _extract_conclusion(self, content: str) -> Optional[str]:
        """Извлекает заключение из отчета"""
        match = re.search(r'ЗАКЛЮЧЕНИЕ:\s*\n([^\n]+(?:\n[^\n]+)*)', content, re.IGNORECASE)
        if match:
            return TextCleaner.clean_whitespace(match.group(1))
        return None
    
    def _parse_interval(self, interval_str: str) -> tuple:
        """Парсит интервал вида '720.0-725.0'"""
        if not interval_str:
            return (None, None)
        
        parts = interval_str.split('-')
        if len(parts) == 2:
            return (self._to_float(parts[0]), self._to_float(parts[1]))
        return (None, None)