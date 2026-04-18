# parsers/__init__.py
# -*- coding: utf-8 -*-

"""
Пакет парсеров для извлечения данных из файлов.
Содержит парсеры для всех типов данных шахты им. Костенко.
"""

from base_parser import BaseParser
from parser_factory import ParserFactory

# Парсеры для формализованных данных
from equipment_parser import EquipmentParser
from certificate_parser import CertificateParser
from maintenance_parser import MaintenanceParser
from sensor_reestr_parser import SensorReestrParser
from sensor_record_parser import SensorRecordParser
from air_analysis_parser import AirAnalysisParser
from gas_analysis_parser import GasAnalysisParser
#from .experiment_parser import ExperimentParser
from premise_parameters_parser import PremiseParametersParser
from equipment_issue_parser import EquipmentIssueParser
from geological_parser import GeologicalParser
from regulatory_parser import RegulatoryParser
from company_parser import CompanyParser
from premise_parser import PremiseParser
from employee_parser import EmployeeParser
from affected_areas_parser import AffectedAreasParser
from inspection_parser import InspectionParser

# Парсеры для неформализованных данных
from incident_description_parser import IncidentDescriptionParser
from chronology_parser import ChronologyParser
from witness_parser import WitnessParser
from hypothesis_facts_parser import HypothesisFactsParser

__all__ = [
    # Базовые классы
    'BaseParser',
    'ParserFactory',
    
    # Парсеры для формализованных данных
    'EquipmentParser',
    'CertificateParser',
    'MaintenanceParser',
    'SensorReestrParser',
    'SensorRecordParser',
    'AirAnalysisParser',
    'GasAnalysisParser',
    #'ExperimentParser',
    'PremiseParametersParser',
    'EquipmentIssueParser',
    'GeologicalParser',
    'RegulatoryParser',
    'CompanyParser',
    'PremiseParser',
    'EmployeeParser',
    'AffectedAreasParser',
    'InspectionParser',
    
    # Парсеры для неформализованных данных
    'IncidentDescriptionParser',
    'ChronologyParser',
    'WitnessParser',
    'HypothesisFactsParser',
]