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

    Схема incident_metrics:
      metric_id, incident_id, total_victims, fatalities_count, injuries_count,
      blast_pressure_mpa, blast_wave_speed_mps, affected_area_length_m,
      affected_areas_count, destroyed_structures_count, economic_damage,
      is_seismic_event, calculation_date, source_file

    Примечание: seismic_event не содержит incident_id —
    is_seismic_event определяем как наличие хоть одной записи в таблице.
    """
    return f"""
SELECT
    CAST(uuid() AS VARCHAR)                                    AS metric_id,
    inc.incident_id,
    COALESCE(inc.fatalities, 0) + COALESCE(inc.injuries, 0)   AS total_victims,
    inc.fatalities                                             AS fatalities_count,
    inc.injuries                                               AS injuries_count,
    inc.blast_pressure_mpa,
    inc.blast_wave_speed_mps,
    aff.affected_area_length_m,
    aff.affected_areas_count,
    CAST(NULL AS INTEGER)                                      AS destroyed_structures_count,
    inc.economic_damage,
    (SELECT COUNT(*) FROM {FULL_SCHEMA}.seismic_event) > 0    AS is_seismic_event,
    CURRENT_TIMESTAMP                                          AS calculation_date,
    inc.source_file
FROM {FULL_SCHEMA}.incident_description inc

LEFT JOIN (
    SELECT
        incident_id,
        SUM(length_m)  AS affected_area_length_m,
        COUNT(*)       AS affected_areas_count
    FROM {FULL_SCHEMA}.affected_areas
    GROUP BY incident_id
) aff ON inc.incident_id = aff.incident_id
"""


def sql_ventilation_metrics() -> str:
    """
    M_vent: вентиляционные показатели из premise_parameters.

    Схема ventilation_metrics:
      metric_id, incident_id, air_velocity_fact_mps, air_velocity_min_mps,
      air_velocity_norm_mps, velocity_deficit_percent, airflow_in_m3min,
      airflow_out_m3min, airflow_total_m3min, airflow_lava_m3min,
      leakage_coefficient, distribution_coefficient, is_ventilation_valid,
      calculation_date, source_file
    """
    return f"""
SELECT
    CAST(uuid() AS VARCHAR)                                     AS metric_id,
    incident_id,
    location                                                    AS premise_id,
    measurement_date                                            AS measurement_dttm,
    air_velocity_mps                                            AS air_velocity_fact_mps,
    CAST(NULL AS DOUBLE)                                        AS air_velocity_min_mps,
    0.5                                                         AS air_velocity_norm_mps,
    GREATEST(0.0,
        (0.5 - COALESCE(air_velocity_mps, 0.0)) / 0.5 * 100.0
    )                                                           AS velocity_deficit_percent,
    air_flow_m3_min                                             AS airflow_in_m3min,
    CAST(NULL AS DOUBLE)                                        AS airflow_out_m3min,
    CAST(NULL AS DOUBLE)                                        AS airflow_total_m3min,
    CAST(NULL AS DOUBLE)                                        AS airflow_lava_m3min,
    leakage_coefficient,
    distribution_coefficient,
    COALESCE(air_velocity_mps, 0.0) >= 0.5                     AS is_ventilation_valid,
    CURRENT_TIMESTAMP                                           AS calculation_date,
    source_file
FROM {FULL_SCHEMA}.premise_parameters
WHERE param_type = 'ventilation'
"""


def sql_fire_metrics() -> str:
    """
    M_fire: агрегированные пожарные показатели по инциденту.

    Схема fire_metrics:
      metric_id, incident_id, r1_o2_co2_ratio, r2_co_o2_ratio, r3_co_co2_ratio,
      delta_o2, delta_co2, delta_co, critical_r1_threshold,
      is_oxidation_detected, fire_duration_minutes, fire_spread_speed_mps,
      max_co_ppm, calculation_date, source_file

    Агрегируем по incident_id: min R1, max R2/R3, среднее delta,
    is_oxidation_detected = хотя бы одна проба с признаком окисления.
    """
    return f"""
SELECT
    CAST(uuid() AS VARCHAR)             AS metric_id,
    incident_id,
    MIN(r1_o2_co2_ratio)                AS r1_o2_co2_ratio,
    MAX(r2_co_o2_ratio)                 AS r2_co_o2_ratio,
    MAX(r3_co_co2_ratio)                AS r3_co_co2_ratio,
    AVG(delta_o2)                       AS delta_o2,
    AVG(delta_co2)                      AS delta_co2,
    AVG(delta_co)                       AS delta_co,
    2.5                                 AS critical_r1_threshold,
    BOOL_OR(is_oxidation)               AS is_oxidation_detected,
    CAST(NULL AS INTEGER)               AS fire_duration_minutes,
    CAST(NULL AS DOUBLE)                AS fire_spread_speed_mps,
    CAST(NULL AS DOUBLE)                AS max_co_ppm,
    CURRENT_TIMESTAMP                   AS calculation_date,
    MIN(source_file)                    AS source_file
FROM {FULL_SCHEMA}.air_analysis
GROUP BY incident_id
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
    """
    Сводная таблица S(uᵢ) — агрегат всех расчётных показателей.

    Схема incident_metrics_summary:
      incident_id, total_victims, fatalities, injuries,
      affected_area_length_m, affected_areas_count,
      blast_pressure_mpa, blast_wave_speed_mps, is_seismic_event,
      avg_delta_v_pct, avg_k_leak, avg_k_distr, ventilation_compliant,
      min_r1, max_r2, oxidation_detected, oxidation_sample_count, calculated_at
    """
    return f"""
SELECT
    m.incident_id,
    m.total_victims,
    m.fatalities_count                                     AS fatalities,
    m.injuries_count                                       AS injuries,
    m.affected_area_length_m,
    m.affected_areas_count,
    m.blast_pressure_mpa,
    m.blast_wave_speed_mps,
    m.is_seismic_event,
    v.avg_velocity_deficit_pct                             AS avg_delta_v_pct,
    v.avg_k_leak,
    v.avg_k_distr,
    v.all_ventilation_valid                                AS ventilation_compliant,
    f.r1_o2_co2_ratio                                      AS min_r1,
    f.r2_co_o2_ratio                                       AS max_r2,
    f.is_oxidation_detected                                AS oxidation_detected,
    CAST(NULL AS BIGINT)                                   AS oxidation_sample_count,
    CURRENT_TIMESTAMP                                      AS calculated_at

FROM {FULL_SCHEMA}.incident_metrics m

LEFT JOIN (
    SELECT
        incident_id,
        AVG(velocity_deficit_percent)       AS avg_velocity_deficit_pct,
        AVG(leakage_coefficient)            AS avg_k_leak,
        AVG(distribution_coefficient)       AS avg_k_distr,
        BOOL_AND(is_ventilation_valid)      AS all_ventilation_valid
    FROM {FULL_SCHEMA}.ventilation_metrics
    GROUP BY incident_id
) v ON m.incident_id = v.incident_id

LEFT JOIN (
    SELECT
        incident_id,
        r1_o2_co2_ratio,
        r2_co_o2_ratio,
        is_oxidation_detected
    FROM {FULL_SCHEMA}.fire_metrics
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
    "new_metrics_calculator",
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
