# utils/id_generator.py
# -*- coding: utf-8 -*-

from typing import Dict
import threading


class IDGenerator:
    """
    Генератор уникальных ID для всех таблиц.
    Поддерживает многопоточность (thread-safe).
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._counters = {}
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._counters: Dict[str, int] = {}
            self._initialized = True
    
    def reset(self, prefix: str, start: int = 0) -> None:
        """Сброс счетчика для префикса"""
        with self._lock:
            self._counters[prefix] = start
    
    def reset_all(self) -> None:
        """Сброс всех счетчиков"""
        with self._lock:
            self._counters.clear()
    
    def generate(self, prefix: str) -> str:
        """
        Генерирует ID вида PREFIX-XXXXX
        
        Args:
            prefix: Префикс (EQ, EXP, CALL, SENSOR, INC...)
        
        Returns:
            Строка ID (например, "EXP-00001")
        """
        with self._lock:
            if prefix not in self._counters:
                self._counters[prefix] = 0
            self._counters[prefix] += 1
            return f"{prefix}-{self._counters[prefix]:05d}"
    
    def get_counter(self, prefix: str) -> int:
        """Возвращает текущее значение счетчика"""
        with self._lock:
            return self._counters.get(prefix, 0)


# Глобальный экземпляр (удобно импортировать)
id_generator = IDGenerator()


# Удобные функции для каждой таблицы
def generate_expert_id() -> str:
    return id_generator.generate("EXP")

def generate_equipment_id() -> str:
    return id_generator.generate("EQ")

def generate_certificate_id() -> str:
    return id_generator.generate("CERT")

def generate_maintenance_id() -> str:
    return id_generator.generate("MT")

def generate_sensor_id() -> str:
    return id_generator.generate("SENSOR")

def generate_geology_id() -> str:
    return id_generator.generate("GEO")

def generate_inspection_id() -> str:
    return id_generator.generate("INSP")

def generate_call_id() -> str:
    return id_generator.generate("CALL")

def generate_statement_id() -> str:
    return id_generator.generate("WIT")

def generate_incident_id() -> str:
    return id_generator.generate("INC")

def generate_doc_id() -> str:
    return id_generator.generate("DOC")

def generate_seismic_id() -> str:
    """Генерирует ID для сейсмического события"""
    return id_generator.generate("SEIS")

def generate_employee_id() -> str:
    """Генерирует ID для сотрудника"""
    return id_generator.generate("EMP")

def generate_premise_id() -> str:
    """Генерирует ID для выработки/помещения"""
    return id_generator.generate("PREM")

def generate_company_id() -> str:
    """Генерирует ID для предприятия"""
    return id_generator.generate("COMP")

def generate_affected_area_id() -> str:
    """Генерирует ID для зоны поражения"""
    return id_generator.generate("AREA")

def generate_inspection_fact_id() -> str:
    """Генерирует ID для замечания при осмотре"""
    return id_generator.generate("INSP")

def generate_sensor_record_id() -> str:
    """Генерирует ID для записи датчика"""
    return id_generator.generate("SR")

def generate_air_sample_id() -> str:
    """Генерирует ID для пробы воздуха"""
    return id_generator.generate("AIR")

def generate_gas_measurement_id() -> str:
    """Генерирует ID для газового замера"""
    return id_generator.generate("GAS")

def generate_premise_param_id() -> str:
    """Генерирует ID для параметров выработки"""
    return id_generator.generate("PREM_PARAM")

def generate_issue_id() -> str:
    """Генерирует ID для записи выдачи оборудования"""
    return id_generator.generate("ISSUE")

def generate_event_id() -> str:
    """Генерирует ID для события в хронологии"""
    return id_generator.generate("EVENT")

def generate_fact_id() -> str:
    """Генерирует ID для факта гипотезы"""
    return id_generator.generate("FACT")