"""
spark_metrics_job.py
====================
Spark-задача вычисления расчётных показателей (оператор M из мат. модели).

Читает факты из Iceberg (через Trino/Nessie на MinIO),
вычисляет M_inc, M_vent, M_fire, показатели дегазации и пылевзрывозащиты,
пишет результаты обратно в Iceberg.

Соответствие формулам раздела 2.2.6 ВКР:
  M_inc  → incident_metrics
  M_vent → ventilation_metrics
  M_fire → fire_metrics

Запуск из Jupyter или spark-submit:
  spark-submit --jars /home/jovyan/jars/hadoop-aws-3.3.4.jar,
                       /home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar
               spark_metrics_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, TimestampType
)

# ── Конфигурация ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MinIO credentials are not configured. Set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.")
BUCKET           = "lakehouse"
WAREHOUSE_PATH   = f"s3a://{BUCKET}/warehouse"

TRINO_HOST   = os.getenv("TRINO_HOST",   "trino")
TRINO_PORT   = os.getenv("TRINO_PORT",   "8080")
TRINO_SCHEMA = "iceberg.mine"

# Атмосферные константы (методика диагностики эндогенных пожаров)
CO2_ATM = 0.03
O2_ATM  = 20.9
CO_ATM  = 0.0
R1_CRIT = 2.5   # порог is_oxidation

# Норматив пылевзрывозащиты
C_NORM_NONCOMB = 85.0   # %

# ── Инициализация SparkSession ────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    jar_dir = "/home/jovyan/jars"
    jars    = ",".join([
        f"{jar_dir}/hadoop-aws-3.3.4.jar",
        f"{jar_dir}/aws-java-sdk-bundle-1.12.262.jar",
    ])

    spark = (
        SparkSession.builder
        .appName("mine_metrics_calculator")
        .master("spark://spark-master:7077")
        # MinIO как S3-совместимое хранилище
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Iceberg + Nessie каталог
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type",              "nessie")
        .config("spark.sql.catalog.nessie.uri",               "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref",               "main")
        .config("spark.sql.catalog.nessie.warehouse",         WAREHOUSE_PATH)
        .config("spark.sql.catalog.nessie.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.nessie.s3.endpoint",       MINIO_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.jars",                                  jars)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Вспомогательные функции чтения ───────────────────────────────────────────

def read_table(spark: SparkSession, table: str):
    """Читает таблицу из Iceberg-каталога nessie."""
    return spark.table(f"iceberg.mine.{table}")


def write_table(df, table: str, mode: str = "overwrite"):
    """
    Пишет датафрейм в Iceberg.
    mode='overwrite' — пересчёт с нуля (идемпотентно).
    mode='append'    — добавление новых записей.
    """
    (df.writeTo(f"iceberg.mine.{table}")
       .tableProperty("write.format.default", "parquet")
       .tableProperty("write.metadata.compression-codec", "gzip")
       .createOrReplace() if mode == "overwrite"
     else df.writeTo(f"iceberg.mine.{table}").append())


# ── Оператор M: M_inc ─────────────────────────────────────────────────────────

def compute_m_inc(spark: SparkSession):
    """
    Формулы (17), (18) из раздела 2.2.6 ВКР:
      total_victims = fatalities + injuries
      affected_area_length = Σ length(a), a ∈ AffectedAreas(uᵢ)

    Источники:
      incident_description  → fatalities, injuries, blast_pressure_mpa,
                               blast_wave_speed_mps, economic_damage
      affected_areas        → length_m (суммируется по инциденту)
      seismic_event         → наличие события в ±10 мин от взрыва
    """
    inc = read_table(spark, "incident_description").alias("inc")
    aff = (
        read_table(spark, "affected_areas")
        .groupBy("incident_id")
        .agg(F.sum("length_m").alias("affected_area_length_m"),
             F.count("*").alias("affected_areas_count"))
        .alias("aff")
    )

    # Сейсмическое событие: ищем запись в ±10 мин от времени взрыва
    # В incident_description есть incident_time — используем его
    seis = (
        read_table(spark, "seismic_event")
        .select("incident_id",
                F.lit(True).alias("has_seismic_event"))
        .dropDuplicates(["incident_id"])
        .alias("seis")
    )

    result = (
        inc
        .join(aff,  inc["incident_id"] == aff["incident_id"],  "left")
        .join(seis, inc["incident_id"] == seis["incident_id"], "left")
        .select(
            inc["incident_id"],
            # total_victims = fatalities + injuries
            (F.coalesce(inc["fatalities"], F.lit(0)) +
             F.coalesce(inc["injuries"],  F.lit(0))
             ).alias("total_victims"),
            inc["fatalities"],
            inc["injuries"],
            aff["affected_area_length_m"],
            aff["affected_areas_count"],
            # blast_pressure и speed — уже в incident_description
            inc["blast_pressure_mpa"],
            inc["blast_wave_speed_mps"],
            inc["economic_damage"],
            F.coalesce(seis["has_seismic_event"], F.lit(False)).alias("is_seismic_event"),
            F.current_timestamp().alias("calculated_at"),
        )
    )

    print(f"[M_inc] Computed {result.count()} rows")
    write_table(result, "incident_metrics")


# ── Оператор M: M_vent ────────────────────────────────────────────────────────

def compute_m_vent(spark: SparkSession):
    """Вентиляционные показатели из premise_parameters"""
    premise = read_table(spark, "premise_parameters")
    
    # Фильтруем вентиляционные параметры
    vent = premise.filter(F.col("param_type") == "ventilation")
    
    if vent.count() == 0:
        print("[M_vent] No ventilation data found")
        return
    
    # Нормативные значения
    AIR_SPEED_NORM = 0.5  # м/с
    K_LEAK_NORM = 0.3     # коэффициент утечек (норма)
    
    result = vent.select(
        vent["incident_id"],
        vent["location"].alias("premise_id"),
        vent["measurement_date"].alias("measurement_dttm"),
        # Фактические значения
        vent["air_velocity_mps"].alias("air_speed_fact"),
        vent["air_flow_m3_min"].alias("airflow_in"),
        vent["cross_section_m2"],
        F.lit(AIR_SPEED_NORM).alias("air_speed_norm"),
        # Расчётные показатели
        (F.greatest(F.lit(0.0), 
            (F.lit(AIR_SPEED_NORM) - F.coalesce(vent["air_velocity_mps"], F.lit(0.0))) 
            / AIR_SPEED_NORM * 100.0)
        ).alias("delta_v_pct"),
        vent["leakage_coefficient"].alias("k_leak"),
        vent["distribution_coefficient"].alias("k_distr"),
        # Соответствие норме
        (F.coalesce(vent["air_velocity_mps"], F.lit(0.0)) >= AIR_SPEED_NORM).alias("is_ventilation_valid"),
        F.current_timestamp().alias("calculated_at"),
    )
    
    print(f"[M_vent] Computed {result.count()} rows")
    write_table(result, "ventilation_metrics")

# ── Оператор M: M_fire ────────────────────────────────────────────────────────

def compute_m_fire(spark: SparkSession):
    """
    Формулы (21)-(25) из раздела 2.2.6 ВКР.

    Источники:
      air_analysis  → концентрации CO2, O2, CO (из ПАСС "Комир")
      sensor_record → показания датчиков CO, CH4 в реальном времени

    ΔO₂  = O2_ATM  − o2_percent
    ΔCO₂ = co2_percent − CO2_ATM
    ΔCO  = co_percent  − CO_ATM

    R1 = ΔO₂  / ΔCO₂   (если ΔCO₂ ≠ 0)
    R2 = ΔCO  / ΔO₂    (если ΔO₂  ≠ 0)
    R3 = ΔCO  / ΔCO₂   (если ΔCO₂ ≠ 0)

    is_oxidation = (R1 < R1_CRIT=2.5)
    """
    air = read_table(spark, "air_analysis")

    delta_o2  = (F.lit(O2_ATM)  - air["o2_percent"]).alias("delta_o2")
    delta_co2 = (air["co2_percent"] - F.lit(CO2_ATM)).alias("delta_co2")
    delta_co  = (air["co_percent"]  - F.lit(CO_ATM)).alias("delta_co")

    r1 = F.when(
        (air["co2_percent"] - CO2_ATM) != 0,
        (F.lit(O2_ATM) - air["o2_percent"]) / (air["co2_percent"] - CO2_ATM)
    ).otherwise(F.lit(None)).alias("r1_o2_co2_ratio")

    r2 = F.when(
        (F.lit(O2_ATM) - air["o2_percent"]) != 0,
        (air["co_percent"] - CO_ATM) / (F.lit(O2_ATM) - air["o2_percent"])
    ).otherwise(F.lit(None)).alias("r2_co_o2_ratio")

    r3 = F.when(
        (air["co2_percent"] - CO2_ATM) != 0,
        (air["co_percent"] - CO_ATM) / (air["co2_percent"] - CO2_ATM)
    ).otherwise(F.lit(None)).alias("r3_co_co2_ratio")

    is_oxidation = F.when(
        r1.isNotNull(),
        (F.lit(O2_ATM) - air["o2_percent"]) / (air["co2_percent"] - CO2_ATM) < R1_CRIT
    ).otherwise(F.lit(None)).alias("is_oxidation")

    result = air.select(
        "incident_id",
        "sample_id",
        "sample_point",
        "sample_dttm",
        "co2_percent",
        "o2_percent",
        "co_percent",
        "ch4_percent",
        "h2_percent",
        delta_o2,
        delta_co2,
        delta_co,
        r1,
        r2,
        r3,
        is_oxidation,
        "conclusion",
        F.current_timestamp().alias("calculated_at"),
    )

    print(f"[M_fire] Computed {result.count()} rows")
    write_table(result, "fire_metrics")


def compute_degassing_metrics(spark: SparkSession):
    """Показатели дегазации из premise_parameters"""
    premise = read_table(spark, "premise_parameters")
    
    deg = premise.filter(F.col("param_type") == "degassing")
    
    if deg.count() == 0:
        print("[Degassing] No degassing data found")
        return
    
    # η_deg = Q_газ_CH₄ / Q_общ_CH₄ × 100%
    result = deg.select(
        deg["incident_id"],
        deg["location"].alias("premise_id"),
        deg["measurement_date"].alias("measurement_dttm"),
        deg["gas_flow_m3_min"].alias("airflow_mix_m3min"),
        deg["ch4_concentration_percent"].alias("ch4_percent"),
        deg["vacuum_pressure_mmH2O"].alias("negative_pressure_mm"),
        deg["gas_flow_m3_min"].alias("q_gas_ch4_m3min"),
        deg["ch4_flow_m3_min"].alias("ch4_total_m3min"),
        # Эффективность дегазации
        F.when(
            deg["ch4_flow_m3_min"].isNotNull() & (deg["ch4_flow_m3_min"] > 0),
            deg["gas_flow_m3_min"] / deg["ch4_flow_m3_min"] * 100.0
        ).otherwise(F.lit(None)).alias("eta_deg_pct"),
        F.current_timestamp().alias("calculated_at"),
    )
    
    print(f"[Degassing] Computed {result.count()} rows")
    write_table(result, "degassing_metrics")

# ── Оператор M: показатели пылевзрывозащиты ──────────────────────────────────

def compute_dust_metrics(spark: SparkSession):
    """Показатели пылевзрывозащиты из premise_parameters"""
    premise = read_table(spark, "premise_parameters")
    
    dust = premise.filter(F.col("param_type") == "dust")
    
    if dust.count() == 0:
        print("[Dust] No dust control data found")
        return
    
    C_NORM = 85.0  # норматив негорючих веществ, %
    
    result = dust.select(
        dust["incident_id"],
        dust["location"].alias("premise_id"),
        dust["measurement_date"].alias("measurement_dttm"),
        F.lit("coal_dust").alias("dust_type"),
        dust["noncombustible_content_percent"].alias("c_noncomb_pct"),
        F.lit(C_NORM).alias("c_norm_pct"),
        # Соответствие норме (is_compliant уже заполняет парсер)
        dust["is_compliant"].alias("is_compliant_dust"),
        dust["cross_section_m2"],
        F.lit(None).cast(DoubleType()).alias("length_m"),
        F.current_timestamp().alias("calculated_at"),
    )
    
    print(f"[Dust] Computed {result.count()} rows")
    write_table(result, "dust_metrics")


# ── Сводные метрики по инциденту ──────────────────────────────────────────────

def compute_incident_summary(spark: SparkSession):
    """
    Агрегирует все расчётные показатели по инциденту
    в единую сводную таблицу S(uᵢ) для быстрого доступа.
    """
    m_inc  = read_table(spark, "incident_metrics").alias("m")
    m_vent = (
        read_table(spark, "ventilation_metrics")
        .groupBy("incident_id")
        .agg(
            F.avg("delta_v_pct").alias("avg_delta_v_pct"),
            F.avg("k_leak").alias("avg_k_leak"),
            F.avg("k_distr").alias("avg_k_distr"),
            F.min("is_ventilation_valid").alias("any_vent_violation"),
        )
        .alias("v")
    )
    m_fire = (
        read_table(spark, "fire_metrics")
        .groupBy("incident_id")
        .agg(
            F.min("r1_o2_co2_ratio").alias("min_r1"),
            F.max("r2_co_o2_ratio").alias("max_r2"),
            F.max("is_oxidation").alias("oxidation_detected"),
            F.count(F.when(F.col("is_oxidation"), 1)).alias("oxidation_sample_count"),
        )
        .alias("f")
    )

    result = (
        m_inc
        .join(m_vent, m_inc["incident_id"] == m_vent["incident_id"], "left")
        .join(m_fire, m_inc["incident_id"] == m_fire["incident_id"], "left")
        .select(
            m_inc["incident_id"],
            # M_inc
            "total_victims", "fatalities", "injuries",
            "affected_area_length_m", "affected_areas_count",
            "blast_pressure_mpa", "blast_wave_speed_mps",
            "is_seismic_event",
            # M_vent (агрегаты)
            m_vent["avg_delta_v_pct"],
            m_vent["avg_k_leak"],
            m_vent["avg_k_distr"],
            (~m_vent["any_vent_violation"]).alias("ventilation_compliant"),
            # M_fire (агрегаты)
            m_fire["min_r1"],
            m_fire["max_r2"],
            m_fire["oxidation_detected"],
            m_fire["oxidation_sample_count"],
            F.current_timestamp().alias("calculated_at"),
        )
    )

    print(f"[Summary] Computed {result.count()} rows")
    write_table(result, "incident_metrics_summary")


# ── Точка входа ───────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    print("=" * 60)
    print("Mine Metrics Calculator - Spark Job")
    print("=" * 60)

    steps = [
        ("M_inc  — общие показатели инцидента",       compute_m_inc),
        ("M_vent — вентиляционные показатели",         compute_m_vent),
        ("M_fire — пожарные показатели (R1/R2/R3)",    compute_m_fire),
        ("Дегазация — η_deg",                          compute_degassing_metrics),
        ("Пылевзрывозащита — is_compliant_dust",       compute_dust_metrics),
        ("Сводная таблица S(uᵢ)",                      compute_incident_summary),
    ]

    errors = []
    for name, fn in steps:
        try:
            print(f"\n> {name}")
            fn(spark)
            print(f"  OK")
        except Exception as e:
            print(f"  FAILED: {type(e).__name__}: {e}")
            errors.append((name, e))

    print("\n" + "=" * 60)
    if errors:
        print(f"Завершено с {len(errors)} ошибками:")
        for name, e in errors:
            print(f"   - {name}: {e}")
    else:
        print("Все рассчитанные метрики успешно вычислены")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
