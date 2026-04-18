#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Главный скрипт для рекурсивной обработки всех файлов.
Запуск: python run_parsers.py --input ./data --output ./output
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import logging

sys.path.insert(0, str(Path(__file__).parent))

from parser_factory import ParserFactory

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parsing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ParserRunner:
    """Управляет запуском всех парсеров"""
    
    def __init__(self, input_dir: str, output_dir: str, extensions: List[str] | None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.extensions = extensions or ['.txt', '.csv', '.json', '.xml', '.xlsx', '.xls']
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.factory = ParserFactory()
        
        # Результаты по таблицам
        self.results_by_table = {}
        
        # Статистика
        self.stats = {
            'files_processed': 0,
            'files_failed': 0,
            'total_records': 0,
            'by_table': {},
            'by_file': []
        }
    
    def run(self):
        """Запускает рекурсивную обработку всех файлов"""
        logger.info("=" * 70)
        logger.info("🚀 ЗАПУСК ПАРСЕРОВ")
        logger.info(f"📁 Входная директория: {self.input_dir}")
        logger.info(f"📁 Выходная директория: {self.output_dir}")
        logger.info(f"📄 Расширения: {self.extensions}")
        logger.info("=" * 70)
        
        # Рекурсивно находим все файлы
        files = self.factory.get_all_files(str(self.input_dir), self.extensions)
        logger.info(f"📄 Найдено файлов: {len(files)}")
        
        for file_path in files:
            self._process_file(file_path)
        
        # Сохраняем результаты
        self._save_all_results()
        
        # Выводим статистику
        self._print_summary()
    
    def _process_file(self, file_path: Path):
        """Обрабатывает один файл всеми подходящими парсерами"""
        relative_path = file_path.relative_to(self.input_dir)
        
        try:
            parse_result = self.factory.parse_file(str(file_path))
            results_by_table = parse_result.get('results', {})
            
            if results_by_table:
                total_records = 0
                tables_info = []
                
                for table_name, records in results_by_table.items():
                    if table_name not in self.results_by_table:
                        self.results_by_table[table_name] = []
                    self.results_by_table[table_name].extend(records)
                    
                    total_records += len(records)
                    tables_info.append(f"{table_name}({len(records)})")
                    
                    # Обновляем статистику по таблицам
                    if table_name not in self.stats['by_table']:
                        self.stats['by_table'][table_name] = 0
                    self.stats['by_table'][table_name] += len(records)
                
                # Обновляем общую статистику
                self.stats['files_processed'] += 1
                self.stats['total_records'] += total_records
                self.stats['by_file'].append({
                    'file': str(relative_path),
                    'tables': tables_info,
                    'records': total_records,
                    'status': 'success'
                })
                
                logger.info(f"  ✓ {relative_path} -> {', '.join(tables_info)}")
            else:
                logger.warning(f"  ⚠ {relative_path} -> записей не найдено")
                
        except Exception as e:
            self.stats['files_failed'] += 1
            self.stats['by_file'].append({
                'file': str(relative_path),
                'status': 'failed',
                'error': str(e)
            })
            logger.error(f"  ✗ {relative_path}: Ошибка - {str(e)}")
    
    def _save_all_results(self):
        """Сохраняет все результаты в JSON файлы"""
        for table_name, records in self.results_by_table.items():
            output_file = self.output_dir / f"{table_name}.json"
            
            # Если файл уже существует, загружаем и объединяем
            if output_file.exists():
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
                records = existing + records
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            logger.info(f"  💾 {table_name}: {len(records)} записей -> {output_file.name}")
    
    def _print_summary(self):
        """Выводит итоговую статистику"""
        logger.info("\n" + "=" * 70)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 70)
        logger.info(f"📄 Обработано файлов: {self.stats['files_processed']}")
        logger.info(f"❌ Ошибок: {self.stats['files_failed']}")
        logger.info(f"📝 Всего записей: {self.stats['total_records']}")
        
        if self.stats['by_table']:
            logger.info("\n📋 По таблицам:")
            for table_name, count in sorted(self.stats['by_table'].items()):
                logger.info(f"  • {table_name}: {count} записей")
        
        # Сохраняем статистику
        summary_file = self.output_dir / "_processing_summary.json"
        summary = {
            'timestamp': datetime.now().isoformat(),
            'input_directory': str(self.input_dir),
            'output_directory': str(self.output_dir),
            'extensions': self.extensions,
            'stats': self.stats
        }
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n💾 Статистика сохранена в {summary_file.name}")


def main():
    parser = argparse.ArgumentParser(description='Рекурсивная обработка файлов парсерами')
    parser.add_argument(
        '--input', '-i',
        type=str,
        default='mine_parser\\data',
        help='Путь к директории с входными файлами'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='./output',
        help='Путь к директории для сохранения результатов'
    )
    parser.add_argument(
        '--extensions', '-e',
        type=str,
        nargs='+',
        default=['.txt', '.csv', '.json', '.xml', '.xlsx', '.xls'],
        help='Расширения файлов для обработки'
    )
    parser.add_argument(
        '--single', '-s',
        type=str,
        help='Обработать только один файл (указывается путь)'
    )
    
    args = parser.parse_args()
    
    if args.single:
        process_single_file(args.single, args.output)
    else:
        runner = ParserRunner(args.input, args.output, args.extensions)
        runner.run()


def process_single_file(file_path: str, output_dir: str):
    """Обрабатывает один файл всеми подходящими парсерами"""
    factory = ParserFactory()
    result = factory.parse_file(file_path)
    results_by_table = result.get('results', {})
    
    if results_by_table:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        total_records = 0
        for table_name, records in results_by_table.items():
            output_file = output_path / f"{table_name}.json"
            
            if output_file.exists():
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
                records = existing + records
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Сохранено {len(records)} записей в {output_file}")
            total_records += len(records)
        
        print(f"\n📋 Итого: {total_records} записей в {len(results_by_table)} таблицах")
        
        # Показываем пример первой записи из первой таблицы
        first_table = list(results_by_table.keys())[0]
        first_records = results_by_table[first_table]
        if first_records:
            print(f"\n📋 Пример первой записи из таблицы {first_table}:")
            for key, value in first_records[0].items():
                if value and not key.startswith('_'):
                    value_str = str(value)[:100]
                    print(f"  {key}: {value_str}")
    else:
        print("⚠ Записей не найдено")


if __name__ == "__main__":
    main()