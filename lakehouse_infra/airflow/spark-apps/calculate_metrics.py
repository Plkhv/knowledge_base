# spark-apps/calculate_all_metrics.py
# -*- coding: utf-8 -*-

"""
Spark приложение для расчёта всех метрик на основе данных из Iceberg.
Расчёт выполняется для указанного инцидента.

Запуск:
    spark-submit --master spark://spark-master:7077 \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        spark-apps/calculate_all_metrics.py \
        --catalog iceberg \
        --namespace mine \
        --incident-id INC-2023-001
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, max as spark_max, min as spark_min,
    avg, count, coalesce, to_timestamp, datediff, expr, row_number, desc
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# ============================================
# 1. Инициализация Spark сессии
# ============================================

def create_spark_session(catalog_uri: str, warehouse: str) -> SparkSession:
    """Создаёт Spark сессию с Iceberg и Nessie"""
    return SparkSession.builder \
        .appName("CalculateAllMetrics") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", catalog_uri) \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", warehouse) \
        .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.nessie.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()


# ============================================
# 2. Константы
# ============================================

# Атмосферные значения для расчёта коэффициентов
CO2_ATM = 0.03      # атмосферный CO2, %
O2_ATM = 20.9       # атмосферный O2, %
CO_ATM = 0.0        # атмосферный CO, %

# Критическое значение R1 (признак эндогенного пожара)
CRITICAL_R1_THRESHOLD = 2.5

# Нормативные значения скорости воздуха в лаве (м/с)
VELOCITY_NORM_MIN = 0.5
VELOCITY_NORM_MAX = 4.0


# ============================================
# 3. Создание таблиц метрик (если не существуют)
# ============================================

def create_metrics_tables(spark: SparkSession, catalog: str, namespace: str):
    """Создаёт таблицы для метрик, если они не существуют"""
    
    # Таблица fire_metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.fire_metrics (
            metric_id STRING,
            incident_id STRING,
            r1_co2_o2_ratio DOUBLE,
            r2_co_o2_ratio DOUBLE,
            r3_co_co2_ratio DOUBLE,
            critical_r1_threshold DOUBLE,
            is_oxidation_detected INT,
            fire_duration_minutes INT,
            fire_spread_speed_mps DOUBLE,
            max_co_ppm DOUBLE,
            calculation_date TIMESTAMP,
            source_file STRING
        ) USING iceberg
    """)
    
    # Таблица incident_metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.incident_metrics (
            metric_id STRING,
            incident_id STRING,
            fatalities_count INT,
            injuries_count INT,
            blast_pressure_mpa DOUBLE,
            blast_wave_speed_mps DOUBLE,
            affected_area_length_m DOUBLE,
            destroyed_structures_count INT,
            economic_damage DOUBLE,
            is_seismic_event INT,
            calculation_date TIMESTAMP,
            source_file STRING
        ) USING iceberg
    """)
    
    # Таблица ventilation_metrics
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.ventilation_metrics (
            metric_id STRING,
            incident_id STRING,
            air_velocity_min_mps DOUBLE,
            air_velocity_norm_mps DOUBLE,
            velocity_deficit_percent DOUBLE,
            leakage_coefficient DOUBLE,
            distribution_coefficient DOUBLE,
            is_ventilation_valid INT,
            calculation_date TIMESTAMP,
            source_file STRING
        ) USING iceberg
    """)
    
    print("Tables created/verified: fire_metrics, incident_metrics, ventilation_metrics")


# ============================================
# 4. Расчёт fire_metrics (пожарные метрики)
# ============================================

def calculate_fire_metrics(spark: SparkSession, catalog: str, namespace: str, incident_id: str):
    """Рассчитывает пожарные метрики на основе air_analysis"""
    
    print("\nCalculating fire_metrics...")
    
    # Читаем данные анализов воздуха
    air_df = spark.table(f"{catalog}.{namespace}.air_analysis") \
        .filter(col("incident_id") == incident_id)
    
    if air_df.count() == 0:
        print("   No air_analysis data found")
        return
    
    # Рассчитываем R-коэффициенты для каждой пробы
    fire_metrics_df = air_df.select(
        lit(None).cast("string").alias("metric_id"),  # будет заполнен позже
        col("incident_id"),
        spark_round(
            (col("co2_percent") - CO2_ATM) / (O2_ATM - col("o2_percent")), 4
        ).alias("r1_co2_o2_ratio"),
        spark_round(
            (col("co_percent") - CO_ATM) / (O2_ATM - col("o2_percent")), 4
        ).alias("r2_co_o2_ratio"),
        spark_round(
            (col("co_percent") - CO_ATM) / (col("co2_percent") - CO2_ATM), 4
        ).alias("r3_co_co2_ratio"),
        lit(CRITICAL_R1_THRESHOLD).alias("critical_r1_threshold"),
        when(
            (col("co2_percent") - CO2_ATM) / (O2_ATM - col("o2_percent")) > 0.6, 1
        ).otherwise(0).alias("is_oxidation_detected"),
        lit(None).cast("int").alias("fire_duration_minutes"),
        lit(None).cast("double").alias("fire_spread_speed_mps"),
        lit(None).cast("double").alias("max_co_ppm"),
        lit(datetime.now()).cast("timestamp").alias("calculation_date"),
        lit("calculated").alias("source_file")
    )
    
    # Генерация уникальных ID
    window = Window.orderBy("r1_co2_o2_ratio")
    fire_metrics_df = fire_metrics_df.withColumn(
        "metric_id", 
        concat(lit("FIRE-"), row_number().over(window).cast("string"))
    )
    
    # Сохраняем результаты
    fire_metrics_df.writeTo(f"{catalog}.{namespace}.fire_metrics").append()
    
    count = fire_metrics_df.count()
    print(f"   Calculated {count} fire_metrics records")


# ============================================
# 5. Расчёт incident_metrics (общие метрики)
# ============================================

def calculate_incident_metrics(spark: SparkSession, catalog: str, namespace: str, incident_id: str):
    """Рассчитывает общие метрики инцидента"""
    
    print("\nCalculating incident_metrics...")
    
    # 5.1. Количество погибших и пострадавших из incident_description
    incident_desc = spark.table(f"{catalog}.{namespace}.incident_description") \
        .filter(col("incident_id") == incident_id) \
        .select("fatalities", "injuries").first()
    
    fatalities = incident_desc[0] if incident_desc else None
    injuries = incident_desc[1] if incident_desc else None
    
    # 5.2. Данные из affected_areas (зоны поражения)
    affected_df = spark.table(f"{catalog}.{namespace}.affected_areas") \
        .filter(col("incident_id") == incident_id)
    
    # Протяжённость зоны поражения (количество записей как proxy)
    affected_area_length = affected_df.count()
    
    # Количество разрушенных перемычек (поиск по ключевым словам)
    destroyed_structures = affected_df \
        .filter(col("premise_id").contains("перемычк") | col("damage_type").contains("разруш")) \
        .count()
    
    # 5.3. Сейсмические события
    seismic_count = spark.table(f"{catalog}.{namespace}.seismic_event") \
        .filter(col("event_dttm").contains("2023-10-28")) \
        .count()
    is_seismic = 1 if seismic_count > 0 else 0
    
    # 5.4. Данные из модели взрыва (из affected_areas)
    blast_pressure = affected_df \
        .filter(col("damage_description").contains("давление")) \
        .select("damage_description").first()
    
    blast_wave_speed = affected_df \
        .filter(col("damage_description").contains("скорость")) \
        .select("damage_description").first()
    
    # 5.5. Экономический ущерб (пока не заполняется)
    economic_damage = None
    
    # Формируем результат
    incident_metrics_df = spark.createDataFrame([(
        None,  # metric_id
        incident_id,
        fatalities,
        injuries,
        None,  # blast_pressure_mpa (может быть извлечён из текста)
        None,  # blast_wave_speed_mps
        float(affected_area_length),  # affected_area_length_m
        destroyed_structures,
        economic_damage,
        is_seismic,
        datetime.now(),
        "calculated"
    )], [
        "metric_id", "incident_id", "fatalities_count", "injuries_count",
        "blast_pressure_mpa", "blast_wave_speed_mps", "affected_area_length_m",
        "destroyed_structures_count", "economic_damage", "is_seismic_event",
        "calculation_date", "source_file"
    ])
    
    # Генерация ID
    incident_metrics_df = incident_metrics_df.withColumn(
        "metric_id", 
        lit("INC-METRIC-1")
    )
    
    incident_metrics_df.writeTo(f"{catalog}.{namespace}.incident_metrics").append()
    print(f"   Calculated 1 incident_metrics record")
    print(f"      fatalities: {fatalities}, injuries: {injuries}")
    print(f"      affected_area: {affected_area_length} records")
    print(f"      is_seismic_event: {is_seismic}")


# ============================================
# 6. Расчёт ventilation_metrics (вентиляционные метрики)
# ============================================

def calculate_ventilation_metrics(spark: SparkSession, catalog: str, namespace: str, incident_id: str):
    """Рассчитывает вентиляционные метрики на основе premise_parameters"""
    
    print("\nCalculating ventilation_metrics...")
    
    # Читаем параметры выработок
    params_df = spark.table(f"{catalog}.{namespace}.premise_parameters") \
        .filter(col("incident_id") == incident_id)
    
    if params_df.count() == 0:
        print("   No premise_parameters data found")
        return
    
    # 6.1. Минимальная скорость в лаве
    lava_velocity = params_df \
        .filter(col("location").like("%Лава%")) \
        .agg(spark_min("air_velocity_mps").alias("min_velocity")) \
        .first()
    
    min_velocity = lava_velocity[0] if lava_velocity else None
    
    # 6.2. Расходы воздуха для расчёта коэффициентов
    vsh_flow = params_df \
        .filter(col("location").like("%ВШ%")) \
        .agg(spark_max("air_flow_m3_min").alias("flow")) \
        .first()[0]
    
    ksh_flow = params_df \
        .filter(col("location").like("%КШ%")) \
        .agg(spark_max("air_flow_m3_min").alias("flow")) \
        .first()[0]
    
    lava_flow = params_df \
        .filter(col("location").like("%Лава%")) \
        .agg(spark_max("air_flow_m3_min").alias("flow")) \
        .first()[0]
    
    # 6.3. Расчёт дефицита скорости
    if min_velocity is not None:
        if min_velocity < VELOCITY_NORM_MIN:
            velocity_deficit = round((VELOCITY_NORM_MIN - min_velocity) / VELOCITY_NORM_MIN * 100, 2)
            is_valid = 0
        elif min_velocity > VELOCITY_NORM_MAX:
            velocity_deficit = round((min_velocity - VELOCITY_NORM_MAX) / VELOCITY_NORM_MAX * 100, 2)
            is_valid = 0
        else:
            velocity_deficit = 0.0
            is_valid = 1
    else:
        velocity_deficit = None
        is_valid = None
    
    # 6.4. Расчёт коэффициента утечек и распределения
    total_flow = (vsh_flow or 0) + (ksh_flow or 0)
    if total_flow > 0 and lava_flow:
        leakage_coefficient = round((total_flow - lava_flow) / total_flow, 4)
        distribution_coefficient = round(lava_flow / total_flow, 4)
    else:
        leakage_coefficient = None
        distribution_coefficient = None
    
    # Формируем результат
    ventilation_metrics_df = spark.createDataFrame([(
        None,  # metric_id
        incident_id,
        min_velocity,
        VELOCITY_NORM_MIN,
        velocity_deficit,
        leakage_coefficient,
        distribution_coefficient,
        is_valid,
        datetime.now(),
        "calculated"
    )], [
        "metric_id", "incident_id", "air_velocity_min_mps", "air_velocity_norm_mps",
        "velocity_deficit_percent", "leakage_coefficient", "distribution_coefficient",
        "is_ventilation_valid", "calculation_date", "source_file"
    ])
    
    # Генерация ID
    ventilation_metrics_df = ventilation_metrics_df.withColumn(
        "metric_id", 
        lit("VENT-METRIC-1")
    )
    
    ventilation_metrics_df.writeTo(f"{catalog}.{namespace}.ventilation_metrics").append()
    
    print(f"   Calculated 1 ventilation_metrics record")
    print(f"      min_velocity: {min_velocity} m/s")
    print(f"      velocity_deficit: {velocity_deficit}%")
    print(f"      leakage_coefficient: {leakage_coefficient}")
    print(f"      distribution_coefficient: {distribution_coefficient}")
    print(f"      is_valid: {is_valid}")


# ============================================
# 7. Основная функция
# ============================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="iceberg", help="Catalog name")
    parser.add_argument("--namespace", default="mine", help="Namespace name")
    parser.add_argument("--incident-id", required=True, help="Incident ID (e.g., INC-2023-001)")
    parser.add_argument("--catalog-uri", default="http://nessie:19120/api/v1", help="Nessie API URI")
    parser.add_argument("--warehouse", default="s3a://warehouse/", help="Warehouse path")
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("CALCULATING ALL METRICS")
    print(f"Catalog: {args.catalog}")
    print(f"Namespace: {args.namespace}")
    print(f"Incident ID: {args.incident_id}")
    print("=" * 70)
    
    # Создаём Spark сессию
    spark = create_spark_session(args.catalog_uri, args.warehouse)
    
    # Создаём таблицы метрик (если не существуют)
    create_metrics_tables(spark, args.catalog, args.namespace)
    
    # Расчёт метрик
    calculate_fire_metrics(spark, args.catalog, args.namespace, args.incident_id)
    calculate_incident_metrics(spark, args.catalog, args.namespace, args.incident_id)
    calculate_ventilation_metrics(spark, args.catalog, args.namespace, args.incident_id)
    
    print("\n" + "=" * 70)
    print("ALL METRICS CALCULATED SUCCESSFULLY")
    print("=" * 70)
    
    spark.stop()


if __name__ == "__main__":
    main()