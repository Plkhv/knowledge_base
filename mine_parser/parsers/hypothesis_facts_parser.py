# parsers/hypothesis_facts_parser.py
# -*- coding: utf-8 -*-

import re
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

from base_parser import BaseParser
from id_generator import generate_fact_id


class HypothesisFactsParser(BaseParser):
    """
    Парсер для извлечения фактов, подтверждающих гипотезы.
    Использует mawo-natasha для семантического поиска.
    """
    
    INCIDENT_ID = "INC-2023-001"
    
    # Гипотезы с ключевыми словами и семантическими векторами
    HYPOTHESES = {
        'HYP-001': {
            'name': 'Взрыв метана от механических искр',
            'keywords': ['искра', 'шлифмашинка', 'искрение', 'шлифовальная машинка', 'ИП-2014Б', 'инструмент'],
            'semantic_phrases': ['источник воспламенения', 'механические искры', 'искрообразование']
        },
        'HYP-002': {
            'name': 'Загазирование из-за нарушения дегазации',
            'keywords': ['газ', 'метан', 'загазирование', 'превышение', 'ППМК', 'CH4', 'СО'],
            'semantic_phrases': ['концентрация метана', 'газовыделение', 'скопление газа']
        },
        'HYP-003': {
            'name': 'Отсутствие орошения',
            'keywords': ['воды нет', 'орошение', 'включи воду', 'сухая', 'подача воды', 'не работает'],
            'semantic_phrases': ['отсутствие воды', 'система орошения', 'пожаротушение']
        },
        'HYP-004': {
            'name': 'Нарушение правил безопасности',
            'keywords': ['инструмент неисправен', 'огнетушитель не сработал', 'нарушение', 'не соблюдали'],
            'semantic_phrases': ['техника безопасности', 'нарушение правил', 'неисправность']
        },
        'HYP-005': {
            'name': 'Недостаточная вентиляция',
            'keywords': ['вентиляция', 'воздух', 'проветривание', 'застой', 'душно'],
            'semantic_phrases': ['проветривание выработки', 'воздухообмен', 'вентиляционная струя']
        },
    }
    
    def __init__(self, config_path: str = "./config"):
        super().__init__(config_path)
        self.set_table_name("hypotesis_prove_facts")
        self._init_mawo()
        self._semantic_vectors = {}
    

    def _init_mawo(self):
        """Инициализирует mawo-natasha для семантического анализа"""
        try:
            from mawo_natasha import RealRussianEmbedding
            # Отключаем автоматическую загрузку Navec
            self.embedding = RealRussianEmbedding(use_navec=False)  # ← use_navec=False
            self.mawo_available = True
            # Не вызываем _precompute_semantic_vectors() если не нужно
        except ImportError:
            self.mawo_available = False
            print("Warning: mawo-natasha not installed")
    
    def _get_embeddings_safe(self, doc) -> List:
        """
        Безопасное получение embeddings из документа.
        Обходит проблему типизации Pylance.
        """
        # Пробуем разные способы доступа к embeddings
        if hasattr(doc, 'embeddings'):
            return doc.embeddings
        elif hasattr(doc, 'vectors'):
            return doc.vectors
        elif hasattr(doc, 'get_embeddings'):
            result = doc.get_embeddings()
            return result if result else []
        else:
            # Пробуем получить через __dict__
            for attr in ['embeddings', 'vectors', '_embeddings']:
                if hasattr(doc, attr):
                    return getattr(doc, attr)
        return []
    
    def _precompute_semantic_vectors(self):
        """Предвычисляет семантические векторы для гипотез"""
        if not self.mawo_available:
            return
        
        for hyp_id, hyp_info in self.HYPOTHESES.items():
            vectors = []
            for phrase in hyp_info.get('semantic_phrases', []):
                try:
                    doc = self.embedding(phrase)
                    embeddings = self._get_embeddings_safe(doc)
                    if embeddings and len(embeddings) > 0:
                        # Берем первый вектор или усредняем
                        first_valid = embeddings[0]
                        if first_valid is not None:
                            vectors.append(first_valid)
                except Exception as e:
                    print(f"Error getting vector for phrase '{phrase}': {e}")
            
            if vectors:
                import numpy as np
                self._semantic_vectors[hyp_id] = np.mean(vectors, axis=0)
    
    def supports(self, file_name: str) -> bool:
        """Поддерживает любые текстовые файлы"""
        return Path(file_name).suffix.lower() in ['.txt', '.csv']
    
    def get_table_name(self) -> Optional[str]:
        return self._table_name
    
    def parse(self, content: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Извлекает факты для всех гипотез из текста.
        Использует: ключевые слова + семантическую близость.
        """
        records = []
        
        # Разбиваем на предложения
        sentences = self._split_into_sentences(content)
        
        for hyp_id, hyp_info in self.HYPOTHESES.items():
            # 1. Поиск по ключевым словам
            keyword_facts = self._search_by_keywords(sentences, hyp_info['keywords'], hyp_id, file_name)
            records.extend(keyword_facts)
            
            # 2. Семантический поиск (если доступен)
            if self.mawo_available and hyp_id in self._semantic_vectors:
                semantic_facts = self._search_by_semantic(sentences, hyp_id, file_name)
                records.extend(semantic_facts)
        
        # Удаляем дубликаты (по тексту факта)
        unique_records = {}
        for record in records:
            fact_text = record.get('fact_text', '')
            if fact_text and fact_text not in unique_records:
                unique_records[fact_text] = record
        
        return list(unique_records.values())
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """Разбивает текст на предложения"""
        # Убираем служебные символы
        text = re.sub(r'={70,}', '', text)
        text = re.sub(r'-{70,}', '', text)
        
        # Разбиваем по знакам препинания
        sentences = re.split(r'[.!?]', text)
        
        # Очищаем и фильтруем
        result = []
        for sent in sentences:
            sent = sent.strip()
            if len(sent) > 20:  # только содержательные предложения
                result.append(sent)
        
        return result
    
    def _search_by_keywords(self, sentences: List[str], keywords: List[str], 
                           hyp_id: str, file_name: str) -> List[Dict[str, Any]]:
        """Поиск фактов по ключевым словам"""
        facts = []
        
        for sentence in sentences:
            sentence_lower = sentence.lower()
            for keyword in keywords:
                if keyword.lower() in sentence_lower:
                    fact = {
                        'fact_id': generate_fact_id(),
                        'hypotesis_id': hyp_id,
                        'source_name': file_name,
                        'is_prove': 1,
                        'fact_text': sentence[:500],
                        'match_type': 'keyword',
                        'keyword': keyword,
                        '_source_file': file_name
                    }
                    facts.append(fact)
                    break  # одно предложение - один факт на гипотезу
        
        return facts
    
    def _search_by_semantic(self, sentences: List[str], hyp_id: str, 
                           file_name: str) -> List[Dict[str, Any]]:
        """Поиск фактов по семантической близости"""
        if not self.mawo_available or hyp_id not in self._semantic_vectors:
            return []
        
        import numpy as np
        
        facts = []
        hyp_vector = self._semantic_vectors[hyp_id]
        
        for sentence in sentences:
            try:
                # Получаем вектор предложения
                doc = self.embedding(sentence)
                embeddings = self._get_embeddings_safe(doc)
                
                if not embeddings:
                    continue
                
                # Фильтруем None значения и получаем валидные векторы
                valid_vectors = [v for v in embeddings if v is not None]
                if not valid_vectors:
                    continue
                
                # Усредняем векторы токенов
                sent_vector = np.mean(valid_vectors, axis=0)
                
                # Вычисляем косинусное сходство
                norm_sent = np.linalg.norm(sent_vector)
                norm_hyp = np.linalg.norm(hyp_vector)
                
                if norm_sent > 0 and norm_hyp > 0:
                    similarity = np.dot(sent_vector, hyp_vector) / (norm_sent * norm_hyp)
                else:
                    similarity = 0.0
                
                # Если сходство выше порога
                if similarity > 0.65:
                    fact = {
                        'fact_id': generate_fact_id(),
                        'hypotesis_id': hyp_id,
                        'source_name': file_name,
                        'is_prove': 1,
                        'fact_text': sentence[:500],
                        'match_type': 'semantic',
                        'similarity_score': round(float(similarity), 4),
                        '_source_file': file_name
                    }
                    facts.append(fact)
            except Exception as e:
                print(f"Error in semantic search for sentence '{sentence[:50]}...': {e}")
        
        return facts