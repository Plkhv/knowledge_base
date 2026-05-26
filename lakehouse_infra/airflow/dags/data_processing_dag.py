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
  5. trigger_metrics_dag     — запускает metrics_calculator автоматически

Запуск: вручную через Airflow UI (schedule_interval=None).
После завершения автоматически триггерит metrics_calculator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

# Графические форматы — не парсятся, только регистрируются в graphic_reestr
GRAPHIC_EXTENSIONS = {'.dwg', '.jpg', '.jpeg', '.png', '.pdf', '.tif', '.tiff', '.bmp', '.svg'}

# Все поддерживаемые расширения = текстовые + графические
ALL_EXTENSIONS = ALLOWED_EXTENSIONS | GRAPHIC_EXTENSIONS

# Максимум строк в одном VALUES-выражении.
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
            wait = 2 ** attempt
            logging.warning(f"MinIO attempt {attempt+1}/{retries} failed: {e}. Retry in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"MinIO upload failed after {retries} attempts: {last_err}")


# ============================================
# Trino — batch INSERT
# ============================================

SENTINEL = {-100000000, -1000000000, -100000000.0, -1000000000.0}

# Колонки типа TIMESTAMP в DDL — строки вставляются как TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
TIMESTAMP_COLS = {
    'event_dttm', 'record_dttm', 'maintenance_date', 'measurement_date',
    'inspection_date', 'statement_datetime', 'conclusion_dttm',
    'experiment_date', 'created_at', 'updated_at',
    'calculation_date', 'calculated_at', 'measure_dttm', 'measurement_dttm',
    'sample_dttm',
}


def _val_to_sql(col: str, val) -> str:
    if val is None:
        return 'NULL'
    if isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    if col == 'category':
        return "'" + str(val).replace("'", "''") + "'"
    if isinstance(val, (int, float)):
        return 'NULL' if val in SENTINEL else str(val)
    
    # TIMESTAMP колонки с валидацией
    if col in TIMESTAMP_COLS:
        if val is None or str(val).strip() in ('', 'None', 'null'):
            return 'NULL'
        ts = str(val).strip()
        # Убираем часовой пояс если есть
        ts = ts.split('+')[0].split('Z')[0].split('.')[0]
        # Проверяем базовый формат
        if len(ts) >= 19 and ts[4] == '-' and ts[7] == '-':
            return f"TIMESTAMP '{ts}'"
        else:
            # Если формат неправильный - вставляем как строку
            logging.warning(f"Invalid timestamp format for {col}: {ts}, inserting as string")
            return "'" + ts.replace("'", "''") + "'"
    
    # Обычные строки
    return "'" + str(val).replace("'", "''") + "'"


def _build_row(record: dict) -> Optional[tuple]:
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
    if not records:
        return 0

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
            col_map = dict(zip(columns, values))
            values  = [col_map.get(c, 'NULL') for c in reference_columns]
        prepared.append(values)

    if not prepared or reference_columns is None:
        return 0

    cols_str   = ', '.join(reference_columns)
    full_table = f"{TRINO_CONFIG['catalog']}.{TRINO_CONFIG['schema']}.{table_name}"
    inserted   = 0

    for batch_start in range(0, len(prepared), batch_size):
        batch    = prepared[batch_start:batch_start + batch_size]
        rows_sql = ',\n  '.join('(' + ', '.join(v) + ')' for v in batch)
        sql      = f"INSERT INTO {full_table} ({cols_str}) VALUES\n  {rows_sql}"

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
                    logging.warning(f"Nessie conflict {table_name}, retry {attempt+1} in {wait}s")
                    time.sleep(wait)
                elif any(x in err_str for x in ['COLUMN_NOT_FOUND', 'TYPE_MISMATCH', 'does not exist']):
                    logging.warning(f"Schema error {table_name} — row-by-row fallback: {err_str[:200]}")
                    schema_error = True
                    last_err     = None
                    break
                else:
                    logging.error(f"Batch error {table_name}: {type(e).__name__}: {err_str[:300]}")
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
            logging.error(f"Batch permanently failed {table_name}: {last_err}")

        if schema_error:
            for row_vals in batch:
                row_sql = f"INSERT INTO {full_table} ({cols_str}) VALUES ({', '.join(row_vals)})"
                conn = cursor = None
                try:
                    conn   = _make_trino_conn()
                    cursor = conn.cursor()
                    cursor.execute(row_sql)
                    inserted += 1
                except Exception as e:
                    err_str = str(e)
                    if any(x in err_str for x in ['COLUMN_NOT_FOUND', 'TYPE_MISMATCH', 'does not exist']):
                        logging.warning(f"Skipping bad row {table_name}: {err_str[:150]}")
                    else:
                        logging.error(f"Row error {table_name}: {err_str[:150]}")
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
    file_path, timestamp, factory = args
    rel_path    = os.path.relpath(file_path, DATA_DIR)
    object_name = f"raw/{timestamp}/{rel_path}"

    try:
        s3_path = upload_to_minio(file_path, object_name)
        logging.info(f"Uploaded: {rel_path}")

        result           = factory.parse_file(file_path)
        results_by_table = result.get('results', {})

        if not results_by_table:
            logging.warning(f"No data extracted from {rel_path}")
            return {'file': rel_path, 'success': True, 'no_data': True, 'total_records': 0}

        # source_file — полный S3-путь; link для graphic_reestr = source_file
        for table_name, records in results_by_table.items():
            for record in records:
                record.setdefault('source_file', s3_path)
                if table_name == 'graphic_reestr' and record.get('link') is None:
                    record['link'] = s3_path

        load_results = {}
        for table_name, records in results_by_table.items():
            inserted = insert_to_trino_batch(table_name, records)
            load_results[table_name] = inserted
            logging.info(f"  {table_name}: {inserted}/{len(records)} inserted")

        total_records = sum(len(r) for r in results_by_table.values())
        return {
            'file': rel_path, 'success': True, 'no_data': False,
            'total_records': total_records,
            'tables': list(results_by_table.keys()),
            'load_results': load_results,
        }

    except Exception as e:
        logging.error(f"Error processing {rel_path}: {e}", exc_info=True)
        return {'file': rel_path, 'success': False, 'error': str(e)}


# ============================================
# Задачи DAG
# ============================================

@task
def discover_files() -> list:
    if not os.path.exists(DATA_DIR):
        raise AirflowSkipException(f"DATA_DIR not found: {DATA_DIR}")

    all_files, skipped = [], []
    for root, dirs, files in os.walk(DATA_DIR):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for file in files:
            if file.startswith('.') or file.startswith('_'):
                continue
            ext = Path(file).suffix.lower()
            if ext not in ALL_EXTENSIONS:
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
    from parser_factory import ParserFactory

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    logging.info("Initializing ParserFactory (once for all workers)...")
    factory = ParserFactory()
    logging.info(f"ParserFactory ready, {len(factory.parsers)} parsers registered")
    logging.info(f"Processing {len(files)} files, {MAX_PARALLEL_FILES} workers, batch_size={BATCH_SIZE}")

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
                    logging.error(f"FAILED: {result['file']} — {result.get('error', 'unknown error')}")
            except Exception as e:
                logging.error(f"Future failed: {rel_path} — {e}")
                results.append({'file': rel_path, 'success': False, 'error': str(e)})

    ok = sum(1 for r in results if r.get('success'))
    logging.info(f"Completed: {ok}/{len(results)} successful")
    return results


@task
def archive_processed_files(process_results: List[Dict]):
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
            Path(ARCHIVE_DIR) / datetime.now().strftime('%Y/%m/%d') / file_rel
        )
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(original_path, archive_path)
        archived += 1
        logging.info(f"Archived: {file_rel}")
    logging.info(f"Archive done: {archived} archived, {no_data} left in DATA_DIR")


@task
def generate_summary(process_results: List[Dict]) -> str:
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
                'file': result.get('file'), 'error': result.get('error', 'Unknown'),
            })

    logging.info("=" * 60)
    logging.info("PROCESSING SUMMARY")
    logging.info(f"  Files:    {summary['total_files']} total, "
                 f"{summary['successful']} ok, {summary['failed']} failed, {summary['no_data']} no data")
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
    'data_processing_dag',
    default_args=default_args,
    description=(
        'Файлы инцидента -> MinIO -> Trino/Iceberg. '
        'По завершении автоматически запускает metrics_calculator.'
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

    # После успешного завершения обработки автоматически запускает
    # DAG расчёта метрик. wait_for_completion=False — не блокирует,
    # metrics_calculator выполняется асинхронно.
    trigger_metrics = TriggerDagRunOperator(
        task_id='trigger_metrics_calculator',
        trigger_dag_id='metrics_calculator',
        wait_for_completion=False,
        reset_dag_run=True,     # если DAG уже был запущен — сбрасывает и запускает заново
        poke_interval=30,
    )

    files >> processed >> [archive, summary] >> trigger_metrics