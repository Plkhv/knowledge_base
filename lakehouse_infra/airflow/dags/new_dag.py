"""
DAG параллельной обработки файлов инцидента с вставкой в Trino/Iceberg.

Ключевые оптимизации:
  - Batch INSERT: одно соединение на таблицу вместо N соединений на строку.
    10 275 строк реестра экспертов = 11 запросов вместо 10 275 соединений.
  - ParserFactory-синглтон: создаётся один раз до ThreadPoolExecutor,
    передаётся в каждый поток — 22 парсера инициализируются один раз,
    а не по 22 на каждый воркер.
  - Фильтрация файлов по расширению до начала обработки.
  - Файлы без данных не архивируются — остаются для ручной проверки.

Порядок выполнения:
  1. discover_files          — находит файлы в DATA_DIR
  2. process_files_parallel  — парсинг + MinIO + batch Trino INSERT
  3. archive_processed_files — архивирует только файлы с данными
  4. generate_summary        — итоговая статистика

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
from typing import List, Dict, Any, Optional

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

# Максимум строк в одном VALUES-выражении.
# При очень широких строках (много TEXT-полей) уменьшить до 500.
BATCH_SIZE = 1000

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
# MinIO
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
            wait = 2 ** attempt  # 1с -> 2с -> 4с
            logging.warning(
                f"MinIO upload attempt {attempt + 1}/{retries} failed: {e}. "
                f"Retry in {wait}s..."
            )
            time.sleep(wait)

    raise RuntimeError(f"MinIO upload failed after {retries} attempts: {last_err}")


# ============================================
# Trino — batch INSERT
# ============================================

SENTINEL = {-100000000, -1000000000, -100000000.0, -1000000000.0}


def _val_to_sql(col: str, val) -> str:
    """Преобразует одно значение поля в SQL-литерал для VALUES."""
    if val is None:
        return 'NULL'
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    if isinstance(val, (int, float)):
        return 'NULL' if val in SENTINEL else str(val)
    # строки
    if col == 'category':
        val = str(val)   # expert_dictionary: category VARCHAR
    return "'" + str(val).replace("'", "''") + "'"


def _build_row(record: dict) -> Optional[tuple]:
    """
    Возвращает (columns_list, values_list) для одной записи.
    Поля с префиксом '_' пропускаются как внутренние.
    Возвращает None если запись полностью пустая.
    """
    columns, values = [], []
    for col, val in record.items():
        if col.startswith('_'):
            continue
        columns.append(col)
        values.append(_val_to_sql(col, val))
    return (columns, values) if columns else None


def _make_trino_conn():
    from trino.dbapi import connect
    return connect(
        host=TRINO_CONFIG['host'],
        port=TRINO_CONFIG['port'],
        user=TRINO_CONFIG['user'],
        catalog=TRINO_CONFIG['catalog'],
        schema=TRINO_CONFIG['schema'],
    )


def insert_to_trino_batch(table_name: str, records: list,
                          batch_size: int = BATCH_SIZE,
                          retries: int = 3) -> int:
    """
    Batch INSERT в Iceberg через Trino.

    Вместо N соединений (по одному на строку) делаем ceil(N / batch_size)
    запросов вида:
        INSERT INTO t (c1, c2, ...) VALUES
          (v1, v2, ...),
          (v1, v2, ...),
          ...

    Деградация при ошибке схемы:
      Если батч упал с TYPE_MISMATCH / COLUMN_NOT_FOUND — переходим
      на построчную вставку внутри этого батча, пропуская плохие строки.
      Остальные батчи продолжают работать как обычно.
    """
    if not records:
        return 0

    # Подготовка: приводим все записи к единому набору колонок
    prepared: List[List[str]] = []
    reference_columns: Optional[List[str]] = None

    for record in records:
        row = _build_row(record)
        if row is None:
            continue
        columns, values = row

        if reference_columns is None:
            reference_columns = columns
        elif columns != reference_columns:
            # Дополняем/переупорядочиваем под эталонные колонки
            col_map = dict(zip(columns, values))
            values  = [col_map.get(c, 'NULL') for c in reference_columns]

        prepared.append(values)

    if not prepared or reference_columns is None:
        return 0

    cols_str   = ', '.join(reference_columns)
    full_table = (
        f"{TRINO_CONFIG['catalog']}.{TRINO_CONFIG['schema']}.{table_name}"
    )
    inserted = 0

    for batch_start in range(0, len(prepared), batch_size):
        batch    = prepared[batch_start:batch_start + batch_size]
        rows_sql = ',\n  '.join(
            '(' + ', '.join(row_vals) + ')' for row_vals in batch
        )
        sql = f"INSERT INTO {full_table} ({cols_str}) VALUES\n  {rows_sql}"

        last_err     = None
        schema_error = False

        for attempt in range(retries):
            conn = cursor = None
            try:
                conn   = _make_trino_conn()
                cursor = conn.cursor()
                cursor.execute(sql)
                inserted += len(batch)
                last_err  = None
                break

            except Exception as e:
                err_str  = str(e)
                last_err = e

                if 'INTERNAL_ERROR' in err_str and (
                    'nessie' in err_str.lower() or 'commit' in err_str.lower()
                ):
                    wait = attempt + 1
                    logging.warning(
                        f"Nessie conflict on {table_name} "
                        f"rows {batch_start}-{batch_start + len(batch)}, "
                        f"retry {attempt + 1}/{retries} in {wait}s"
                    )
                    time.sleep(wait)

                elif any(x in err_str for x in [
                    'COLUMN_NOT_FOUND', 'TYPE_MISMATCH', 'does not exist'
                ]):
                    logging.warning(
                        f"Schema error on {table_name} batch — "
                        f"falling back to row-by-row: {err_str[:200]}"
                    )
                    schema_error = True
                    last_err     = None
                    break

                else:
                    logging.error(
                        f"Batch insert error on {table_name} "
                        f"rows {batch_start}-{batch_start + len(batch)}: "
                        f"{type(e).__name__}: {err_str[:300]}"
                    )
                    last_err = None
                    break

            finally:
                if cursor:
                    try: cursor.close()
                    except Exception: pass
                if conn:
                    try: conn.close()
                    except Exception: pass

        if last_err:
            logging.error(
                f"Batch failed permanently on {table_name}: "
                f"{type(last_err).__name__}: {last_err}"
            )

        # Деградация до построчной вставки для проблемного батча
        if schema_error:
            for row_vals in batch:
                row_sql = (
                    f"INSERT INTO {full_table} ({cols_str}) "
                    f"VALUES ({', '.join(row_vals)})"
                )
                conn = cursor = None
                try:
                    conn   = _make_trino_conn()
                    cursor = conn.cursor()
                    cursor.execute(row_sql)
                    inserted += 1
                except Exception as e:
                    err_str = str(e)
                    if any(x in err_str for x in [
                        'COLUMN_NOT_FOUND', 'TYPE_MISMATCH', 'does not exist'
                    ]):
                        logging.warning(
                            f"Skipping bad row in {table_name}: {err_str[:150]}"
                        )
                    else:
                        logging.error(
                            f"Row insert error in {table_name}: "
                            f"{type(e).__name__}: {err_str[:150]}"
                        )
                finally:
                    if cursor:
                        try: cursor.close()
                        except Exception: pass
                    if conn:
                        try: conn.close()
                        except Exception: pass

    return inserted


# ============================================
# Обработка одного файла
# ============================================

def process_single_file(args: tuple) -> Dict[str, Any]:
    """
    Обрабатывает один файл: MinIO → парсинг → batch INSERT в Trino.

    Принимает кортеж (file_path, timestamp, factory).
    factory — синглтон, созданный один раз в process_files_parallel.
    parse_file() thread-safe: работает с отдельным файлом,
    не меняет общего состояния фабрики.
    """
    file_path, timestamp, factory = args
    rel_path    = os.path.relpath(file_path, DATA_DIR)
    object_name = f"raw/{timestamp}/{rel_path}"

    try:
        # 1. Загрузка в MinIO
        s3_path = upload_to_minio(file_path, object_name)
        logging.info(f"Uploaded: {rel_path}")

        # 2. Парсинг
        result           = factory.parse_file(file_path)
        results_by_table = result.get('results', {})

        if not results_by_table:
            logging.warning(f"No data extracted from {rel_path}")
            return {
                'file':          rel_path,
                'success':       True,
                'no_data':       True,
                'total_records': 0,
            }

        # 3. source_file: setdefault не перезаписывает значение парсера
        for records in results_by_table.values():
            for record in records:
                record.setdefault('source_file', s3_path)

        # 4. Batch INSERT
        load_results = {}
        for table_name, records in results_by_table.items():
            inserted = insert_to_trino_batch(table_name, records)
            load_results[table_name] = inserted
            logging.info(
                f"  {table_name}: {inserted}/{len(records)} inserted"
            )

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
        logging.error(f"Error processing {rel_path}: {e}", exc_info=True)
        return {'file': rel_path, 'success': False, 'error': str(e)}


# ============================================
# Задачи DAG
# ============================================

@task
def discover_files() -> list:
    """
    Рекурсивно находит файлы в DATA_DIR.
    Фильтрует скрытые файлы и расширения вне ALLOWED_EXTENSIONS.
    """
    if not os.path.exists(DATA_DIR):
        raise AirflowSkipException(f"DATA_DIR not found: {DATA_DIR}")

    all_files = []
    skipped   = []

    for root, dirs, files in os.walk(DATA_DIR):
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
        logging.info(f"Skipped {len(skipped)} files with unsupported extensions")
    if not all_files:
        raise AirflowSkipException("No files with supported extensions found")

    logging.info(f"Found {len(all_files)} files to process")
    return all_files


@task
def process_files_parallel(files: List[str]) -> List[Dict]:
    """
    Параллельная обработка файлов.

    ParserFactory создаётся ОДИН РАЗ до ThreadPoolExecutor.
    Все 22 парсера инициализируются один раз — включая mawo-импорты.
    factory.parse_file() thread-safe: каждый поток работает
    со своим файлом и не меняет общего состояния фабрики.
    """
    from parser_factory import ParserFactory

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logging.info("Initializing ParserFactory (once for all workers)...")
    factory = ParserFactory()
    logging.info(f"ParserFactory ready, {len(factory.parsers)} parsers registered")
    logging.info(
        f"Processing {len(files)} files, "
        f"{MAX_PARALLEL_FILES} workers, batch_size={BATCH_SIZE}"
    )

    task_args = [(fp, timestamp, factory) for fp in files]
    results   = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FILES) as executor:
        future_to_path = {
            executor.submit(process_single_file, args): args[0]
            for args in task_args
        }

        for future in as_completed(future_to_path):
            file_path = future_to_path[future]
            rel_path  = os.path.relpath(file_path, DATA_DIR)
            try:
                result = future.result(timeout=300)
                results.append(result)
                if result['success']:
                    n   = result['total_records']
                    tag = "OK" if n > 0 else "OK (no data)"
                    logging.info(f"{tag}: {result['file']} — {n} records")
                else:
                    logging.error(
                        f"FAILED: {result['file']} — "
                        f"{result.get('error', 'unknown error')}"
                    )
            except Exception as e:
                logging.error(f"Future failed: {rel_path} — {e}")
                results.append({
                    'file': rel_path, 'success': False, 'error': str(e)
                })

    ok = sum(1 for r in results if r.get('success'))
    logging.info(f"Completed: {ok}/{len(results)} successful")
    return results


@task
def archive_processed_files(process_results: List[Dict]):
    """
    Архивирует файлы с реальными данными (success=True, total_records>0).
    Файлы без данных остаются в DATA_DIR для ручной проверки.
    """
    import shutil

    archived = no_data = 0

    for result in process_results:
        if not result.get('success'):
            continue
        if result.get('no_data') or result.get('total_records', 0) == 0:
            logging.warning(f"Not archiving {result['file']} — no data")
            no_data += 1
            continue

        file_rel      = result['file']
        original_path = os.path.join(DATA_DIR, file_rel)
        if not os.path.exists(original_path):
            continue

        archive_path = (
            Path(ARCHIVE_DIR)
            / datetime.now().strftime('%Y/%m/%d')
            / file_rel
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(original_path, archive_path)
        archived += 1
        logging.info(f"Archived: {file_rel}")

    logging.info(
        f"Archive done: {archived} archived, "
        f"{no_data} left in DATA_DIR"
    )


@task
def generate_summary(process_results: List[Dict]) -> str:
    """Итоговый отчёт прогона."""
    no_data_files = [
        r['file'] for r in process_results
        if r.get('success') and r.get('total_records', 0) == 0
    ]

    summary = {
        'timestamp':      datetime.now().isoformat(),
        'total_files':    len(process_results),
        'successful':     sum(1 for r in process_results if r.get('success')),
        'failed':         sum(1 for r in process_results if not r.get('success')),
        'no_data':        len(no_data_files),
        'total_records':  sum(r.get('total_records', 0) for r in process_results),
        'tables_summary': {},
        'failed_files':   [],
        'no_data_files':  no_data_files,
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
    logging.info("PROCESSING SUMMARY")
    logging.info(f"  Files:    {summary['total_files']} total, "
                 f"{summary['successful']} ok, "
                 f"{summary['failed']} failed, "
                 f"{summary['no_data']} no data")
    logging.info(f"  Records:  {summary['total_records']}")

    if summary['tables_summary']:
        logging.info("  Tables:")
        for t, n in sorted(summary['tables_summary'].items()):
            logging.info(f"    {t}: {n}")

    if no_data_files:
        logging.warning("  No data (left in DATA_DIR):")
        for f in no_data_files:
            logging.warning(f"    {f}")

    if summary['failed_files']:
        logging.error("  Failed:")
        for f in summary['failed_files'][:10]:
            logging.error(f"    {f['file']}: {f['error'][:120]}")

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
    'parallel_trino_loader_opt',
    default_args=default_args,
    description=(
        'Файлы инцидента -> MinIO -> Trino/Iceberg. '
        'Batch INSERT, singleton ParserFactory.'
    ),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['parallel', 'trino', 'iceberg'],
) as dag:

    files     = discover_files()
    processed = process_files_parallel(files)
    archive   = archive_processed_files(processed)
    summary   = generate_summary(processed)

    files >> processed >> [archive, summary]