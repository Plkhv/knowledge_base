# utils/validators.py (дополнение)

from datetime import datetime
from typing import Optional

from .date_parser import DateParser 

class CertificateValidator:
    """Валидатор для сертификатов"""
    
    INCIDENT_DATE = datetime(2023, 10, 28)  # Дата аварии
    
    @staticmethod
    def is_valid_at_incident(issue_date: Optional[str], expiry_date: Optional[str]) -> int:
        """
        Определяет, был ли сертификат действителен на момент аварии.
        
        Returns:
            1 - действителен, 0 - не действителен
        """
        if not issue_date:
            return 0
        
        issue = DateParser.parse(issue_date)
        if not issue:
            return 0
        
        # Если дата выдачи после аварии
        if issue > CertificateValidator.INCIDENT_DATE:
            return 0
        
        # Если есть дата истечения
        if expiry_date:
            expiry = DateParser.parse(expiry_date)
            if expiry and expiry < CertificateValidator.INCIDENT_DATE:
                return 0
        
        return 1