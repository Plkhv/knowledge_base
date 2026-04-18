# parsers/parser_factory.py
# -*- coding: utf-8 -*-

import json
import fnmatch
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import logging

from base_parser import BaseParser
from equipment_parser import EquipmentParser
from certificate_parser import CertificateParser
from maintenance_parser import MaintenanceParser
from sensor_reestr_parser import SensorReestrParser
from sensor_record_parser import SensorRecordParser
from air_analysis_parser import AirAnalysisParser
from gas_analysis_parser import GasAnalysisParser
from premise_parameters_parser import PremiseParametersParser
from equipment_issue_parser import EquipmentIssueParser
from geological_parser import GeologicalParser
from regulatory_parser import RegulatoryParser
from company_parser import CompanyParser
from premise_parser import PremiseParser
from employee_parser import EmployeeParser
from affected_areas_parser import AffectedAreasParser
from inspection_parser import InspectionParser
from incident_description_parser import IncidentDescriptionParser
from chronology_parser import ChronologyParser
from witness_parser import WitnessParser
from hypothesis_facts_parser import HypothesisFactsParser
from seismic_parser import SeismicParser
from expert_parser import ExpertParser

logger = logging.getLogger(__name__)


class ParserFactory:
    """
    Фабрика для выбора парсеров по имени файла.
    """
    
    def __init__(self, config_path: str = "./config"):
        self.config_path_str = config_path
        self.config_path = Path(config_path)
        
        # Загружаем ID инцидента
        self.incident_id = self._load_incident_id()
        
        # Регистрируем все парсеры
        self._register_parsers()
        
        # Загружаем routing конфигурацию
        self.routing = self._load_routing()
        
        # Кэш для уже загруженных файлов
        self._parsed_cache = {}
        
        logger.info(f"Загружено правил маршрутизации: {len(self.routing.get('routing_rules', []))}")
        logger.info(f"ID инцидента: {self.incident_id}")
    
    def _load_incident_id(self) -> str:
        """Загружает ID инцидента из конфига"""
        config_file = Path('mine_parser\\config\\incident_config.json')
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get("incident_id", "INC-UNKNOWN")
            except Exception as e:
                logger.warning(f"Ошибка загрузки incident_config.json: {e}")
                return "INC-UNKNOWN"
        logger.warning("incident_config.json не найден, использую INC-UNKNOWN")
        return "INC-UNKNOWN"
    
    def _register_parsers(self):
        """Регистрирует все доступные парсеры, передавая incident_id"""
        cp = self.config_path_str
        inc_id = self.incident_id
        
        self.parsers = {
            'equipment': EquipmentParser(inc_id),
            'certificate': CertificateParser(inc_id),
            'maintenance': MaintenanceParser(inc_id),
            'sensor_reestr': SensorReestrParser(inc_id),
            'sensor_record': SensorRecordParser(inc_id),
            'air_analysis': AirAnalysisParser(inc_id),
            'gas_analysis': GasAnalysisParser(inc_id),
            'premise_parameters': PremiseParametersParser(inc_id),
            'equipment_issue': EquipmentIssueParser(inc_id),
            'geological': GeologicalParser(inc_id),
            'regulatory': RegulatoryParser(inc_id),
            'company': CompanyParser(inc_id),
            'premise': PremiseParser(inc_id),
            'employee': EmployeeParser(inc_id),
            'affected_areas': AffectedAreasParser(inc_id),
            'inspection': InspectionParser(inc_id),
            'incident_description': IncidentDescriptionParser(inc_id),
            'chronology': ChronologyParser(inc_id),
            'witness': WitnessParser(inc_id),
            'seismic': SeismicParser(inc_id),
            'expert': ExpertParser(inc_id),
            'hypothesis_facts': HypothesisFactsParser(inc_id),
        }
    
    def _load_routing(self) -> Dict:
        """Загружает конфигурацию маршрутизации из файла"""
        possible_paths = [
            self.config_path / "routing" / "file_routing.json",
            self.config_path / "file_routing.json",
            Path("./config/routing/file_routing.json"),
            Path("./config/file_routing.json"),
        ]
        
        for routing_file in possible_paths:
            if routing_file.exists():
                try:
                    with open(routing_file, 'r', encoding='utf-8') as f:
                        routing = json.load(f)
                        logger.info(f"Загружен routing из {routing_file}")
                        return routing
                except Exception as e:
                    logger.warning(f"Ошибка загрузки {routing_file}: {e}")
        
        logger.warning("Файл file_routing.json не найден, использую дефолтную конфигурацию")
        return {
            "routing_rules": [],
            "default_parser": "hypothesis_facts",
            "default_table": "hypotesis_prove_facts"
        }
    
    def get_parsers_for_file(self, file_name: str) -> List[Tuple[BaseParser, str]]:
        """Возвращает список парсеров, подходящих для файла"""
        file_name_lower = file_name.lower()
        matched_parsers = []
        
        rules = sorted(
            self.routing.get("routing_rules", []),
            key=lambda x: x.get("priority", 0),
            reverse=True
        )
        
        for rule in rules:
            for pattern in rule.get("name_patterns", []):
                if fnmatch.fnmatch(file_name_lower, pattern.lower()):
                    parser_type = rule.get("parser_type")
                    output_table = rule.get("output_table")
                    if parser_type in self.parsers:
                        matched_parsers.append((self.parsers[parser_type], output_table))
                        logger.debug(f"{file_name} -> {parser_type} (by routing rule: {pattern})")
                        break
        
        if not matched_parsers:
            for parser in self.parsers.values():
                try:
                    if parser.supports(file_name):
                        matched_parsers.append((parser, parser.get_table_name()))
                        logger.debug(f"{file_name} -> {parser.get_table_name()} (by supports())")
                except Exception as e:
                    logger.warning(f"Error checking supports for {parser}: {e}")
        
        if not matched_parsers:
            default_parser = self.parsers.get('hypothesis_facts')
            if default_parser:
                matched_parsers.append((default_parser, 'hypotesis_prove_facts'))
                logger.debug(f"{file_name} -> hypotesis_prove_facts (default)")
        
        return matched_parsers
    
    def parse_file(self, file_path: str) -> Dict[str, Any]:
        """Парсит один файл всеми подходящими парсерами"""
        file_path_obj = Path(file_path)
        file_name = file_path_obj.name
        file_ext = file_path_obj.suffix.lower()
        
        cache_key = str(file_path_obj.absolute())
        if cache_key in self._parsed_cache:
            logger.debug(f"Using cached result for {file_name}")
            return self._parsed_cache[cache_key]
        
        if file_ext in ['.xlsx', '.xls']:
            content = None
        else:
            content = self._read_file_content(file_path_obj)
        
        parsers = self.get_parsers_for_file(file_name)
        results_by_table = {}
        
        for parser, output_table in parsers:
            try:
                if hasattr(parser, 'parse_file'):
                    records = parser.parse_file(str(file_path_obj))
                else:
                    records = parser.parse(content, file_name) if content is not None else []
                
                if records:
                    if output_table not in results_by_table:
                        results_by_table[output_table] = []
                    results_by_table[output_table].extend(records)
                    logger.debug(f"  {file_name} -> {output_table}: {len(records)} записей")
                    
            except Exception as e:
                logger.error(f"Ошибка парсинга файла {file_name} парсером {output_table}: {e}")
        
        self._parsed_cache[cache_key] = {
            'file_name': file_name,
            'file_path': str(file_path_obj),
            'results': results_by_table
        }
        
        return self._parsed_cache[cache_key]
    
    def _read_file_content(self, file_path: Path) -> str:
        """Читает содержимое файла с учётом кодировки"""
        file_ext = file_path.suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            return "None"
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='cp1251') as f:
                    return f.read()
            except Exception as e:
                logger.error(f"Не удалось прочитать файл {file_path}: {e}")
                return ""
    
    def get_all_files(self, directory: str, extensions: List[str] | None) -> List[Path]:
        """Рекурсивно находит все файлы в директории"""
        if extensions is None:
            extensions = ['.txt', '.csv', '.json', '.xml', '.xlsx', '.xls']
        
        dir_path = Path(directory)
        if not dir_path.exists():
            logger.warning(f"Directory not found: {directory}")
            return []
        
        files = []
        for ext in extensions:
            files.extend(dir_path.rglob(f"*{ext}"))
        
        return sorted(files)
    
    def parse_directory(self, directory: str, extensions: List[str] | None) -> Dict[str, List[Dict]]:
        """Рекурсивно парсит все файлы в директории"""
        results = {}
        files = self.get_all_files(directory, extensions)
        
        logger.info(f"Найдено файлов: {len(files)}")
        
        for file_path in files:
            try:
                parse_result = self.parse_file(str(file_path))
                results_by_table = parse_result.get('results', {})
                
                for table_name, records in results_by_table.items():
                    if table_name not in results:
                        results[table_name] = []
                    results[table_name].extend(records)
                
                tables_count = len(results_by_table)
                records_count = sum(len(r) for r in results_by_table.values())
                logger.info(f"  ✓ {file_path.relative_to(directory)} -> {tables_count} таблиц ({records_count} записей)")
                
            except Exception as e:
                logger.error(f"  ✗ {file_path.relative_to(directory)}: Ошибка - {str(e)}")
        
        return results
    
    def clear_cache(self):
        """Очищает кэш обработанных файлов"""
        self._parsed_cache.clear()
        logger.info("Кэш обработанных файлов очищен")