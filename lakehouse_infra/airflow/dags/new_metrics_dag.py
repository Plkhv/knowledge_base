"""
metrics_dag.py
==============
DAG расчёта показателей через Trino/Iceberg — без Spark, без Jupyter.

Trino выполняет CTAS (CREATE TABLE AS SELECT) прямо поверх Iceberg-таблиц,
которые уже загружены data_processing_dag.

Порядок выполнения:
  1. check_source_data   — проверяет что источники не пустые
  2. compute_metrics     — выполняет все расчётные SQL через Trino
  3. validate_results    — проверяет что таблицы метрик заполнены
  4. notify_summary      — логирует итоговую статистику

Запуск: только вручную, после parallel_trino_loader.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
import logging
import json
from typing import Dict, Any, List

# ── Конфигурация ──────────────────────────────────────────────────────────────

TRINO_HOST   = "trino"
TRINO_PORT   = 8080
TRINO_SCHEMA = "mine"
CATALOG      = "iceberg"
FULL_SCHEMA  = f"{CATALOG}.{TRINO_SCHEMA}"

SOURCE_TABLES = [
    "incident_description",
    "air_analysis",
    "premise_parameters",
    "affected_areas",
]

RESULT_TABLES = [
    "incident_metrics",
    "ventilation_metrics",
    "fire_metrics",
    "degassing_metrics",
    "dust_metrics",
    "incident_metrics_summary",
]

# Атмосферные константы (методика диагностики эндогенных пожаров)
R1_CRIT = 2.5

# ── Trino-утилиты ─────────────────────────────────────────────────────────────

def make_conn():
    from trino.dbapi import connect
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog=CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
        request_timeout=120.0,
    )


def run_sql(sql: str, description: str = "") -> int:
    """
    Выполняет один SQL-запрос через Trino.
    Возвращает количество затронутых строк (для SELECT COUNT — результат).
    """
    conn = cursor = None
    try:
        conn   = make_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        # Для SELECT COUNT
        if sql.strip().upper().startswith("SELECT"):
            row = cursor.fetchone()
            return row[0] if row else 0
        return 0
    finally:
        if cursor:
            try: cursor.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


def count_table(table: str) -> int:
    return run_sql(f"SELECT COUNT(*) FROM {FULL_SCHEMA}.{table}")


def drop_and_create(table: str, select_sql: str):
    """
    Пересоздаёт таблицу метрик через CTAS.
    DROP IF EXISTS + CREATE AS SELECT — идемпотентно, можно перезапускать.
    """
    run_sql(f"DROP TABLE IF EXISTS {FULL_SCHEMA}.{table}")
    ctas = f"""
CREATE TABLE {FULL_SCHEMA}.{table}
WITH (format = 'PARQUET')
AS
{select_sql}
"""
    run_sql(ctas, description=table)
    n = count_table(table)
    logging.info(f"  {table}: {n} rows written")
    return n


# ── SQL-расчёты ───────────────────────────────────────────────────────────────

def sql_incident_metrics() -> str:
    """
    M_inc: общие показатели инцидента.
    Формулы (17),(18) раздела 2.2.6 ВКР.
    """
    return f"""
SELECT
    inc.incident_id,
    COALESCE(inc.fatalities, 0) + COALESCE(inc.injuries, 0)  AS total_victims,
    inc.fatalities,
    inc.injuries,
    aff.affected_area_length_m,
    aff.affected_areas_count,
    inc.blast_pressure_mpa,
    inc.blast_wave_speed_mps,
    inc.economic_damage,
    COALESCE(seis.has_seismic_event, FALSE)                   AS is_seismic_event,
    CURRENT_TIMESTAMP                                          AS calculated_at
FROM {FULL_SCHEMA}.incident_description inc

LEFT JOIN (
    SELECT
        incident_id,
        SUM(length_m)  AS affected_area_length_m,
        COUNT(*)       AS affected_areas_count
    FROM {FULL_SCHEMA}.affected_areas
    GROUP BY incident_id
) aff ON inc.incident_id = aff.incident_id

LEFT JOIN (
    SELECT incident_id, TRUE AS has_seismic_event
    FROM {FULL_SCHEMA}.seismic_event
    GROUP BY incident_id
) seis ON inc.incident_id = seis.incident_id
"""


def sql_ventilation_metrics() -> str:
    """M_vent: вентиляционные показатели из premise_parameters."""
    return f"""
SELECT
    incident_id,
    location                                               AS premise_id,
    measurement_date                                       AS measurement_dttm,
    air_velocity_mps                                       AS air_speed_fact,
    air_flow_m3_min                                        AS airflow_in,
    cross_section_m2,
    0.5                                                    AS air_speed_norm,
    GREATEST(0.0,
        (0.5 - COALESCE(air_velocity_mps, 0.0)) / 0.5 * 100.0
    )                                                      AS delta_v_pct,
    leakage_coefficient                                    AS k_leak,
    distribution_coefficient                               AS k_distr,
    COALESCE(air_velocity_mps, 0.0) >= 0.5                AS is_ventilation_valid,
    CURRENT_TIMESTAMP                                      AS calculated_at
FROM {FULL_SCHEMA}.premise_parameters
WHERE param_type = 'ventilation'
"""


def sql_fire_metrics() -> str:
    """
    M_fire: показатели пожарной опасности.
    Читаем уже посчитанные delta/ratio из air_analysis —
    парсер записывает их при загрузке.
    """
    return f"""
SELECT
    incident_id,
    sample_id,
    sample_point,
    sample_dttm,
    co2_percent,
    o2_percent,
    co_percent,
    ch4_percent,
    h2_percent,
    delta_o2,
    delta_co2,
    delta_co,
    r1_o2_co2_ratio,
    r2_co_o2_ratio,
    r3_co_co2_ratio,
    is_oxidation,
    conclusion,
    CURRENT_TIMESTAMP AS calculated_at
FROM {FULL_SCHEMA}.air_analysis
"""


def sql_degassing_metrics() -> str:
    """Показатели дегазации: эффективность η_deg = Q_газ / Q_CH4 × 100%."""
    return f"""
SELECT
    incident_id,
    location                                               AS premise_id,
    measurement_date                                       AS measurement_dttm,
    gas_flow_m3_min                                        AS airflow_mix_m3min,
    ch4_concentration_percent                              AS ch4_percent,
    vacuum_pressure_mmH2O                                  AS negative_pressure_mm,
    gas_flow_m3_min                                        AS q_gas_ch4_m3min,
    ch4_flow_m3_min                                        AS ch4_total_m3min,
    CASE
        WHEN ch4_flow_m3_min IS NOT NULL AND ch4_flow_m3_min > 0
        THEN gas_flow_m3_min / ch4_flow_m3_min * 100.0
        ELSE NULL
    END                                                    AS eta_deg_pct,
    CURRENT_TIMESTAMP                                      AS calculated_at
FROM {FULL_SCHEMA}.premise_parameters
WHERE param_type = 'degassing'
"""


def sql_dust_metrics() -> str:
    """Показатели пылевзрывозащиты."""
    return f"""
SELECT
    incident_id,
    location                                               AS premise_id,
    measurement_date                                       AS measurement_dttm,
    'coal_dust'                                            AS dust_type,
    noncombustible_content_percent                         AS c_noncomb_pct,
    85.0                                                   AS c_norm_pct,
    is_compliant                                           AS is_compliant_dust,
    cross_section_m2,
    CAST(NULL AS DOUBLE)                                   AS length_m,
    CURRENT_TIMESTAMP                                      AS calculated_at
FROM {FULL_SCHEMA}.premise_parameters
WHERE param_type = 'dust'
"""


def sql_incident_summary() -> str:
    """Сводная таблица S(uᵢ) — агрегат всех расчётных показателей."""
    return f"""
SELECT
    m.incident_id,
    -- M_inc
    m.total_victims,
    m.fatalities,
    m.injuries,
    m.affected_area_length_m,
    m.affected_areas_count,
    m.blast_pressure_mpa,
    m.blast_wave_speed_mps,
    m.is_seismic_event,
    -- M_vent (агрегаты)
    v.avg_delta_v_pct,
    v.avg_k_leak,
    v.avg_k_distr,
    NOT v.any_vent_violation                               AS ventilation_compliant,
    -- M_fire (агрегаты)
    f.min_r1,
    f.max_r2,
    f.oxidation_detected,
    f.oxidation_sample_count,
    CURRENT_TIMESTAMP                                      AS calculated_at

FROM {FULL_SCHEMA}.incident_metrics m

LEFT JOIN (
    SELECT
        incident_id,
        AVG(delta_v_pct)          AS avg_delta_v_pct,
        AVG(k_leak)               AS avg_k_leak,
        AVG(k_distr)              AS avg_k_distr,
        BOOL_AND(is_ventilation_valid) AS any_vent_violation
    FROM {FULL_SCHEMA}.ventilation_metrics
    GROUP BY incident_id
) v ON m.incident_id = v.incident_id

LEFT JOIN (
    SELECT
        incident_id,
        MIN(r1_o2_co2_ratio)                      AS min_r1,
        MAX(r2_co_o2_ratio)                       AS max_r2,
        BOOL_OR(is_oxidation)                     AS oxidation_detected,
        COUNT(*) FILTER (WHERE is_oxidation)      AS oxidation_sample_count
    FROM {FULL_SCHEMA}.fire_metrics
    GROUP BY incident_id
) f ON m.incident_id = f.incident_id
"""


# ── Задачи DAG ────────────────────────────────────────────────────────────────

@task
def check_source_data() -> Dict[str, int]:
    """Проверяет Trino и наличие данных в таблицах-источниках."""
    try:
        conn = make_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        logging.info("Trino connection OK")
    except Exception as e:
        raise ConnectionError(f"Cannot reach Trino at {TRINO_HOST}:{TRINO_PORT}: {e}")

    counts       = {}
    empty_tables = []

    for table in SOURCE_TABLES:
        try:
            n = count_table(table)
        except Exception as e:
            logging.warning(f"Cannot count {table}: {e}")
            n = 0
        counts[table] = n
        logging.info(f"  {'OK' if n > 0 else 'EMPTY'} {table}: {n} rows")
        if n == 0:
            empty_tables.append(table)

    if empty_tables:
        raise AirflowSkipException(
            f"Source tables empty: {empty_tables}. Run parallel_trino_loader first."
        )

    return counts


@task
def compute_metrics(source_counts: Dict[str, int]) -> Dict[str, int]:
    """
    Выполняет все расчёты через Trino SQL (CTAS).
    Каждая таблица метрик пересоздаётся — идемпотентно.
    Порядок важен: incident_summary читает из остальных метрик-таблиц.
    """
    steps = [
        ("incident_metrics",        sql_incident_metrics()),
        ("ventilation_metrics",     sql_ventilation_metrics()),
        ("fire_metrics",            sql_fire_metrics()),
        ("degassing_metrics",       sql_degassing_metrics()),
        ("dust_metrics",            sql_dust_metrics()),
        ("incident_metrics_summary", sql_incident_summary()),  # последняя
    ]

    results  = {}
    errors   = []

    for table, select_sql in steps:
        try:
            logging.info(f"Computing {table}...")
            n = drop_and_create(table, select_sql)
            results[table] = n
        except Exception as e:
            logging.error(f"FAILED {table}: {type(e).__name__}: {e}")
            errors.append((table, str(e)))
            results[table] = 0

    if errors:
        # Не роняем DAG — validate_results покажет что пустое
        logging.warning(f"Completed with {len(errors)} errors:")
        for t, e in errors:
            logging.warning(f"  {t}: {e}")
    else:
        logging.info("All metrics computed successfully")

    return results


@task
def validate_results(compute_results: Dict[str, int]) -> Dict[str, int]:
    """Проверяет что таблицы метрик заполнены."""
    counts   = {}
    warnings = []

    for table in RESULT_TABLES:
        n = compute_results.get(table, 0)
        counts[table] = n
        logging.info(f"  {'OK' if n > 0 else 'EMPTY'} {table}: {n} rows")
        if n == 0:
            warnings.append(table)

    if warnings:
        logging.warning(f"Empty metrics tables: {warnings}")

    return counts


@task
def notify_summary(source_counts: Dict[str, int],
                   result_counts:  Dict[str, int]) -> str:
    """Итоговый отчёт."""
    summary = {
        "timestamp":          datetime.now().isoformat(),
        "source_tables":      source_counts,
        "result_tables":      result_counts,
        "total_metrics_rows": sum(result_counts.values()),
    }

    logging.info("=" * 60)
    logging.info("METRICS SUMMARY (via Trino SQL)")
    logging.info(f"  Source rows:  {sum(source_counts.values())}")
    logging.info(f"  Metrics rows: {summary['total_metrics_rows']}")
    for table, n in result_counts.items():
        logging.info(f"    {'OK' if n > 0 else 'EMPTY'} {table}: {n}")
    logging.info("=" * 60)

    return json.dumps(summary, indent=2, ensure_ascii=False)


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":           "data_engineering",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=2),
}

with DAG(
    "new_metric_dag",
    default_args=default_args,
    description="Расчёт M_inc/M_vent/M_fire через Trino SQL → Iceberg (без Spark)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["trino", "metrics", "iceberg"],
) as dag:

    source  = check_source_data()
    compute = compute_metrics(source)
    results = validate_results(compute)
    summary = notify_summary(source, results)

    source >> compute >> results >> summary