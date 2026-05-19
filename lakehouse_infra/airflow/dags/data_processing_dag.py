"""
DAG параллельной обработки файлов инцидента с вставкой в Trino/Iceberg.

Порядок выполнения:
  1. discover_files          — находит файлы в DATA_DIR по допустимым расширениям
  2. process_files_parallel  — параллельный парсинг + загрузка в MinIO + вставка в Trino
  3. archive_processed_files — перемещает файлы с реальными данными в архив
  4. generate_summary        — логирует итоговую статистику

Запуск: только вручную (schedule_interval=None).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

# ============================================
# НАСТРОЙКА
# ============================================

PARSER_PATH = '/opt/airflow/mine_parser'
sys.path.insert(0, PARSER_PATH)

DATA_DIR    = f"{PARSER_PATH}/data"
ARCHIVE_DIR = '/opt/airflow/archive'

MAX_PARALLEL_FILES = 3

# Допустимые расширения файлов для обработки
ALLOWED_EXTENSIONS = {'.txt', '.csv', '.json', '.xml', '.xlsx', '.xls'}

MINIO_CONFIG = {
    'endpoint':   'http://minio:9000',
    'access_key': 'admin',
    'secret_key': 'password',
    'bucket':     'lakehouse',
}

TRINO_CONFIG = {
    'host':    'trino',
    'port':    8080,
    'user':    'trino',
    'catalog': 'iceberg',
    'schema':  'mine',
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


def upload_to_minio(file_path: str, object_name: str, retries: int = 3) -> str:
    """
    Загружает файл в MinIO с повторными попытками.
    Новый клиент на каждую попытку — защита от NameResolutionError
    при временных сбоях Docker-сети под нагрузкой.
    """
    bucket   = MINIO_CONFIG['bucket']
    last_err = None

    for attempt in range(retries):
        try:
            client = get_minio_client()
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            client.fput_object(bucket, object_name, file_path)
            return f"s3a://{bucket}/{object_name}"
        except Exception as e:
            last_err = e
            wait = 2 ** attempt   # 1с → 2с → 4с
            logging.warning(
                f"MinIO upload attempt {attempt + 1}/{retries} failed: {e}. "
                f"Retry in {wait}s..."
            )
            time.sleep(wait)

    raise RuntimeError(f"MinIO upload failed after {retries} attempts: {last_err}")


def _build_values(record: dict) -> tuple:
    """
    Строит (columns_str, values_str) из записи для INSERT INTO Trino.

    Правила:
    - Поля с префиксом '_' пропускаются (внутренние поля парсеров).
    - Sentinel-значения (-100000000, -1000000000) → NULL.
    - bool → TRUE / FALSE (Trino не принимает 0/1 для BOOLEAN).
    - Строки экранируются удвоением одиночных кавычек.
    """
    SENTINEL = {-100000000, -1000000000, -100000000.0, -1000000000.0}
    columns, values = [], []

    for col, val in record.items():
        if col.startswith('_'):
            continue
        columns.append(col)

        if val is None:
            values.append('NULL')
        elif isinstance(val, bool):
            values.append('TRUE' if val else 'FALSE')
        elif isinstance(val, (int, float)) and val in SENTINEL:
            values.append('NULL')
        elif isinstance(val, (int, float)):
            values.append(str(val))
        else:
            # category в expert_dictionary хранится как VARCHAR — приводим к строке
            if col == 'category' and val is not None:
                val = str(val)
            escaped = str(val).replace("'", "''")
            values.append(f"'{escaped}'")

    return ', '.join(columns), ', '.join(values)


def insert_to_trino(table_name: str, records: list, retries: int = 3) -> int:
    """
    Построчная вставка записей в Iceberg через Trino.

    Особенности:
    - Новое соединение на каждую строку — Trino не поддерживает транзакции,
      conn.commit() убран.
    - Retry при Nessie INTERNAL_ERROR (race condition параллельных коммитов).
    - Row-level skip при COLUMN_NOT_FOUND / TYPE_MISMATCH — одна плохая строка
      не роняет всю таблицу.
    - Прочие ошибки логируются и пропускаются без retry.
    """
    if not records:
        return 0

    from trino.dbapi import connect

    def make_conn():
        return connect(
            host=TRINO_CONFIG['host'],
            port=TRINO_CONFIG['port'],
            user=TRINO_CONFIG['user'],
            catalog=TRINO_CONFIG['catalog'],
            schema=TRINO_CONFIG['schema'],
        )

    inserted = 0

    for record in records:
        columns_str, values_str = _build_values(record)
        if not columns_str:
            continue

        sql = (
            f"INSERT INTO {TRINO_CONFIG['catalog']}.{TRINO_CONFIG['schema']}.{table_name} "
            f"({columns_str}) VALUES ({values_str})"
        )

        last_err = None
        for attempt in range(retries):
            conn = cursor = None
            try:
                conn   = make_conn()
                cursor = conn.cursor()
                cursor.execute(sql)
                # Намеренно без conn.commit() — Trino не поддерживает транзакции
                inserted += 1
                last_err = None
                break
            except Exception as e:
                err_str = str(e)
                last_err = e

                if 'INTERNAL_ERROR' in err_str and (
                    'nessie' in err_str.lower() or 'commit' in err_str.lower()
                ):
                    # Nessie commit conflict — временная проблема, retry с задержкой
                    wait = attempt + 1
                    logging.warning(
                        f"Nessie conflict on {table_name}, "
                        f"retry {attempt + 1}/{retries} in {wait}s"
                    )
                    time.sleep(wait)
                elif any(x in err_str for x in [
                    'COLUMN_NOT_FOUND', 'TYPE_MISMATCH', 'does not exist'
                ]):
                    # Ошибка схемы — повторять бессмысленно, пропускаем строку
                    logging.warning(
                        f"Schema error on {table_name}, skipping row: {err_str[:200]}"
                    )
                    last_err = None
                    break
                else:
                    # Прочие ошибки — не повторяем
                    break
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

        if last_err:
            logging.error(
                f"Trino insert error for {table_name}: "
                f"{type(last_err).__name__}: {last_err}"
            )

    return inserted


def process_single_file(file_path: str, timestamp: str) -> Dict[str, Any]:
    """
    Обрабатывает один файл: загрузка в MinIO → парсинг → вставка в Trino.

    ParserFactory создаётся внутри потока — каждый поток получает
    свой экземпляр и свой кэш, что избегает гонок при параллельной обработке.
    """
    from parser_factory import ParserFactory

    rel_path    = os.path.relpath(file_path, DATA_DIR)
    object_name = f"raw/{timestamp}/{rel_path}"

    try:
        # 1. Загрузка в MinIO
        s3_path = upload_to_minio(file_path, object_name)
        logging.info(f"📤 Uploaded: {rel_path}")

        # 2. Парсинг
        factory          = ParserFactory()
        result           = factory.parse_file(file_path)
        results_by_table = result.get('results', {})

        if not results_by_table:
            logging.warning(f"⚠️  No data extracted from {rel_path}")
            return {
                'file':          rel_path,
                'success':       True,
                'no_data':       True,
                'total_records': 0,
            }

        # 3. Проставляем source_file.
        #    setdefault — не перезаписываем значения уже выставленные парсером
        #    (например, seismic_parser пишет 'seismic_data' как маркер источника).
        for records in results_by_table.values():
            for record in records:
                record.setdefault('source_file', s3_path)

        # 4. Вставка в Trino
        load_results = {}
        for table_name, records in results_by_table.items():
            inserted               = insert_to_trino(table_name, records)
            load_results[table_name] = inserted
            logging.info(f"   ✅ {table_name}: {inserted}/{len(records)} records inserted")

        total_records = sum(len(r) for r in results_by_table.values())

        return {
            'file':          rel_path,
            'success':       True,
            'no_data':       False,
            'total_records': total_records,
            'tables':        list(results_by_table.keys()),
            'load_results':  load_results,
        }

    except Exception as e:
        logging.error(f"❌ Error processing {rel_path}: {e}", exc_info=True)
        return {
            'file':    rel_path,
            'success': False,
            'error':   str(e),
        }


# ============================================
# Задачи DAG
# ============================================

@task
def discover_files() -> list:
    """
    Рекурсивно находит файлы в DATA_DIR.
    Фильтрует: скрытые файлы, файлы с '_', расширения вне ALLOWED_EXTENSIONS.
    """
    if not os.path.exists(DATA_DIR):
        logging.warning(f"Data directory {DATA_DIR} does not exist!")
        raise AirflowSkipException(f"DATA_DIR not found: {DATA_DIR}")

    all_files = []
    skipped   = []

    for root, dirs, files in os.walk(DATA_DIR):
        # Не заходим в скрытые папки
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        for file in files:
            if file.startswith('.') or file.startswith('_'):
                continue
            ext = Path(file).suffix.lower()
            if ext not in ALLOWED_EXTENSIONS:
                skipped.append(file)
                continue
            all_files.append(os.path.join(root, file))

    if skipped:
        logging.info(f"⏭️  Skipped {len(skipped)} files with unsupported extensions")

    if not all_files:
        raise AirflowSkipException("No files with supported extensions found in DATA_DIR")

    logging.info(f"📁 Found {len(all_files)} files to process")
    return all_files


@task
def process_files_parallel(files: List[str]) -> List[Dict]:
    """Параллельная обработка файлов (MAX_PARALLEL_FILES потоков)."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results   = []

    logging.info(
        f"🚀 Processing {len(files)} files "
        f"with {MAX_PARALLEL_FILES} parallel workers"
    )

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FILES) as executor:
        future_to_file = {
            executor.submit(process_single_file, fp, timestamp): fp
            for fp in files
        }

        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            rel_path  = os.path.relpath(file_path, DATA_DIR)
            try:
                result = future.result(timeout=300)
                results.append(result)
                if result['success']:
                    n = result['total_records']
                    tag = "✅" if n > 0 else "⚠️  (no data)"
                    logging.info(f"{tag} {result['file']}: {n} records")
                else:
                    logging.error(f"❌ {result['file']}: {result.get('error', 'Unknown')}")
            except Exception as e:
                logging.error(f"❌ Future failed for {rel_path}: {e}")
                results.append({'file': rel_path, 'success': False, 'error': str(e)})

    successful = sum(1 for r in results if r.get('success'))
    logging.info(f"📊 Completed: {successful}/{len(results)} files successful")

    return results


@task
def archive_processed_files(process_results: List[Dict]):
    """
    Перемещает в архив только файлы, из которых были реально извлечены данные
    (success=True И total_records > 0).
    Файлы без данных остаются в DATA_DIR для ручной проверки.
    """
    import shutil

    archived_count  = 0
    skipped_no_data = 0

    for result in process_results:
        if not result.get('success'):
            continue

        if result.get('no_data') or result.get('total_records', 0) == 0:
            logging.warning(
                f"⚠️  Not archiving {result['file']} — no records extracted"
            )
            skipped_no_data += 1
            continue

        file_rel      = result['file']
        original_path = os.path.join(DATA_DIR, file_rel)

        if not os.path.exists(original_path):
            continue

        archive_date = datetime.now().strftime('%Y/%m/%d')
        archive_path = Path(ARCHIVE_DIR) / archive_date / file_rel
        archive_path.parent.mkdir(parents=True, exist_ok=True)

        shutil.move(original_path, archive_path)
        archived_count += 1
        logging.info(f"📦 Archived: {file_rel}")

    logging.info(
        f"Archive complete: {archived_count} archived, "
        f"{skipped_no_data} left in DATA_DIR (no data)"
    )


@task
def generate_summary(process_results: List[Dict]) -> str:
    """Генерирует и логирует итоговый отчёт прогона."""
    no_data_files = [
        r['file'] for r in process_results
        if r.get('success') and r.get('total_records', 0) == 0
    ]

    summary = {
        'timestamp':    datetime.now().isoformat(),
        'total_files':  len(process_results),
        'successful':   sum(1 for r in process_results if r.get('success')),
        'failed':       sum(1 for r in process_results if not r.get('success')),
        'no_data':      len(no_data_files),
        'total_records': sum(r.get('total_records', 0) for r in process_results),
        'tables_summary': {},
        'failed_files':  [],
        'no_data_files': no_data_files,
    }

    for result in process_results:
        if result.get('success'):
            for table_name, count in result.get('load_results', {}).items():
                summary['tables_summary'][table_name] = (
                    summary['tables_summary'].get(table_name, 0) + count
                )
        else:
            summary['failed_files'].append({
                'file':  result.get('file'),
                'error': result.get('error', 'Unknown'),
            })

    logging.info("=" * 60)
    logging.info("📊 PROCESSING SUMMARY")
    logging.info(f"   Total files:   {summary['total_files']}")
    logging.info(f"   Successful:    {summary['successful']}")
    logging.info(f"   Failed:        {summary['failed']}")
    logging.info(f"   No data:       {summary['no_data']}")
    logging.info(f"   Total records: {summary['total_records']}")

    if summary['tables_summary']:
        logging.info("📋 Records per table:")
        for table, count in sorted(summary['tables_summary'].items()):
            logging.info(f"   - {table}: {count}")

    if no_data_files:
        logging.warning("⚠️  Files with no extracted data (left in DATA_DIR):")
        for f in no_data_files:
            logging.warning(f"   - {f}")

    if summary['failed_files']:
        logging.error("❌ Failed files:")
        for f in summary['failed_files'][:10]:
            logging.error(f"   - {f['file']}: {f['error'][:120]}")

    logging.info("=" * 60)

    return json.dumps(summary, indent=2, ensure_ascii=False)


# ============================================
# DAG
# ============================================

default_args = {
    'owner':           'data_engineering',
    'depends_on_past': False,
    'start_date':      datetime(2024, 1, 1),
    'retries':         1,
    'retry_delay':     timedelta(minutes=3),
}

with DAG(
    'parallel_trino_loader',
    default_args=default_args,
    description='Параллельная обработка файлов инцидента → MinIO → Trino/Iceberg',
    schedule_interval=None,   # только ручной запуск
    catchup=False,
    max_active_runs=1,
    tags=['parallel', 'trino', 'iceberg'],
) as dag:

    files     = discover_files()
    processed = process_files_parallel(files)
    archive   = archive_processed_files(processed)
    summary   = generate_summary(processed)

    files >> processed >> [archive, summary]