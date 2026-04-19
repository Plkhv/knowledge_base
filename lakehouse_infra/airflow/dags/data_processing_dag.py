"""
Оптимизированный DAG с параллельной обработкой и вставкой в Trino
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

# ============================================
# НАСТРОЙКА
# ============================================

PARSER_PATH = '/opt/airflow/mine_parser'
sys.path.insert(0, PARSER_PATH)

DATA_DIR = f"{PARSER_PATH}/data"
ARCHIVE_DIR = '/opt/airflow/archive'

# УМЕРЕННАЯ ПАРАЛЛЕЛИЗАЦИЯ
MAX_PARALLEL_FILES = 3  # 3 файла одновременно

MINIO_CONFIG = {
    'endpoint': 'http://minio:9000',
    'access_key': 'admin',
    'secret_key': 'password',
    'bucket': 'lakehouse',
}

TRINO_CONFIG = {
    'host': 'trino',
    'port': 8080,
    'user': 'trino',
    'catalog': 'iceberg',
    'schema': 'mine',
}

# ============================================
# Утилиты
# ============================================

def get_minio_client():
    from minio import Minio
    return Minio(
        MINIO_CONFIG['endpoint'].replace('http://', ''),
        access_key=MINIO_CONFIG['access_key'],
        secret_key=MINIO_CONFIG['secret_key'],
        secure=False,
    )


def upload_to_minio(file_path: str, object_name: str) -> str:
    client = get_minio_client()
    bucket = MINIO_CONFIG['bucket']
    
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    
    client.fput_object(bucket, object_name, file_path)
    return f"s3a://{bucket}/{object_name}"


def insert_to_trino(table_name: str, records: list):
    """Вставка записей в Iceberg через Trino"""
    if not records:
        return 0
    
    from trino.dbapi import connect
    
    try:
        conn = connect(
            host=TRINO_CONFIG['host'],
            port=TRINO_CONFIG['port'],
            user=TRINO_CONFIG['user'],
            catalog=TRINO_CONFIG['catalog'],
            schema=TRINO_CONFIG['schema'],
            http_scheme='http',
            request_timeout=120.0,
        )
        cursor = conn.cursor()
        inserted = 0
        
        for record in records:
            columns = list(record.keys())
            values = []
            
            for col in columns:
                val = record[col]
                if val is None:
                    values.append('NULL')
                elif isinstance(val, bool):
                    values.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    # Заменяем спец.значения на NULL
                    if val in [-100000000, -1.0, -100000000.0]:
                        values.append('NULL')
                    else:
                        values.append(str(val))
                else:
                    escaped = str(val).replace("'", "''")
                    values.append(f"'{escaped}'")
            
            columns_str = ', '.join(columns)
            values_str = ', '.join(values)
            
            sql = f"INSERT INTO {TRINO_CONFIG['catalog']}.{TRINO_CONFIG['schema']}.{table_name} ({columns_str}) VALUES ({values_str})"
            
            cursor.execute(sql)
            inserted += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        return inserted
        
    except Exception as e:
        logging.error(f"Trino insert error for {table_name}: {e}")
        return 0


def process_single_file(file_path: str, timestamp: str) -> Dict[str, Any]:
    """Обрабатывает один файл (загрузка + парсинг + вставка в Trino)"""
    try:
        from parser_factory import ParserFactory
        
        rel_path = os.path.relpath(file_path, DATA_DIR)
        object_name = f"raw/{timestamp}/{rel_path}"
        
        # 1. Загрузка в MinIO
        s3_path = upload_to_minio(file_path, object_name)
        logging.info(f"📤 Uploaded: {rel_path}")
        
        # 2. Парсинг файла
        factory = ParserFactory()
        result = factory.parse_file(file_path)
        results_by_table = result.get('results', {})
        
        if not results_by_table:
            logging.warning(f"No data extracted from {rel_path}")
            return {'file': rel_path, 'success': True, 'total_records': 0}
        
        # 3. Добавляем source_file и очищаем данные
        for table_name, records in results_by_table.items():
            for record in records:
                # Переименовываем _source_file в source_file
                if '_source_file' in record:
                    record['source_file'] = record['_source_file']
                    del record['_source_file']
                else:
                    record['source_file'] = s3_path
        
        # 4. Вставка в Trino
        load_results = {}
        for table_name, records in results_by_table.items():
            inserted = insert_to_trino(table_name, records)
            load_results[table_name] = inserted
            logging.info(f"   ✅ {table_name}: {inserted} records inserted")
        
        total_records = sum(len(r) for r in results_by_table.values())
        
        return {
            'file': rel_path,
            'success': True,
            'total_records': total_records,
            'tables': list(results_by_table.keys()),
            'load_results': load_results,
        }
        
    except Exception as e:
        rel_path = os.path.relpath(file_path, DATA_DIR)
        logging.error(f"❌ Error processing {rel_path}: {e}", exc_info=True)
        return {
            'file': rel_path,
            'success': False,
            'error': str(e),
        }


# ============================================
# Основные задачи
# ============================================

@task
def discover_files() -> list:
    """Обнаруживает новые файлы"""
    if not os.path.exists(DATA_DIR):
        logging.warning(f"Data directory {DATA_DIR} does not exist!")
        return []
    
    all_files = []
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.startswith('.') or file.startswith('_'):
                continue
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    
    if not all_files:
        raise AirflowSkipException("No files found")
    
    logging.info(f"📁 Found {len(all_files)} files to process")
    return all_files


@task
def process_files_parallel(files: List[str]) -> List[Dict]:
    """Параллельная обработка файлов с вставкой в Trino"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = []
    
    logging.info(f"🚀 Starting parallel processing of {len(files)} files with {MAX_PARALLEL_FILES} workers")
    
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FILES) as executor:
        future_to_file = {
            executor.submit(process_single_file, file_path, timestamp): file_path
            for file_path in files
        }
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result(timeout=300)
                results.append(result)
                if result['success']:
                    logging.info(f"✅ {result['file']}: {result['total_records']} records")
                else:
                    logging.error(f"❌ {result['file']}: {result.get('error', 'Unknown')}")
            except Exception as e:
                rel_path = os.path.relpath(file_path, DATA_DIR)
                logging.error(f"Failed to process {rel_path}: {e}")
                results.append({'file': rel_path, 'success': False, 'error': str(e)})
    
    successful = sum(1 for r in results if r['success'])
    logging.info(f"📊 Processed {successful}/{len(results)} files successfully")
    
    return results


@task
def archive_processed_files(process_results: List[Dict]):
    """Архивирует успешно обработанные файлы"""
    import shutil
    
    archived_count = 0
    for result in process_results:
        if result.get('success'):
            file_rel = result['file']
            original_path = os.path.join(DATA_DIR, file_rel)
            
            if os.path.exists(original_path):
                archive_date = datetime.now().strftime('%Y/%m/%d')
                archive_dir = Path(ARCHIVE_DIR) / archive_date
                archive_dir.mkdir(parents=True, exist_ok=True)
                
                archive_path = archive_dir / file_rel
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                
                shutil.move(original_path, archive_path)
                archived_count += 1
                logging.info(f"📦 Archived: {file_rel}")
    
    logging.info(f"Archived {archived_count} files")


@task
def generate_summary(process_results: List[Dict]) -> str:
    """Генерирует отчёт о выполнении"""
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_files': len(process_results),
        'successful': sum(1 for r in process_results if r.get('success')),
        'failed': sum(1 for r in process_results if not r.get('success')),
        'total_records': sum(r.get('total_records', 0) for r in process_results),
        'tables_summary': {},
        'failed_files': [],
    }
    
    for result in process_results:
        if result.get('success'):
            for table_name, count in result.get('load_results', {}).items():
                summary['tables_summary'][table_name] = summary['tables_summary'].get(table_name, 0) + count
        else:
            summary['failed_files'].append({
                'file': result.get('file'),
                'error': result.get('error', 'Unknown')
            })
    
    logging.info("=" * 60)
    logging.info("📊 PROCESSING SUMMARY")
    logging.info(f"✅ Successful files: {summary['successful']}/{summary['total_files']}")
    logging.info(f"📊 Total records: {summary['total_records']}")
    logging.info("📋 Tables loaded:")
    for table, count in summary['tables_summary'].items():
        logging.info(f"   - {table}: {count} records")
    if summary['failed_files']:
        logging.info("❌ Failed files:")
        for f in summary['failed_files'][:5]:
            logging.info(f"   - {f['file']}: {f['error'][:100]}")
    logging.info("=" * 60)
    
    return json.dumps(summary, indent=2)


# ============================================
# DAG
# ============================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'parallel_trino_loader',
    default_args=default_args,
    description='Параллельная обработка файлов с вставкой в Trino',
    catchup=False,
    max_active_runs=1,
    tags=['parallel', 'trino', 'iceberg'],
) as dag:
    
    files = discover_files()
    processed = process_files_parallel(files)
    archive = archive_processed_files(processed)
    summary = generate_summary(processed)
    
    files >> processed >> [archive, summary]