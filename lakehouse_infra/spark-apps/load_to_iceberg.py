#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для загрузки данных в Iceberg
Точно такой же, как в вашем Jupyter ноутбуке
"""

import json
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def get_spark_session():
    """Создаёт SparkSession как в вашем ноутбуке"""
    
    # Путь к JAR файлам (как в Jupyter)
    jars_path = "/home/jovyan/jars/*.jar"
    
    spark = SparkSession.builder \
        .appName("Load to Iceberg") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.nessie.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "password") \
        .getOrCreate()
    
    return spark


def get_schema_from_json(file_path, spark):
    """Читает JSON и возвращает схему (как в вашем ноутбуке)"""
    
    # Читаем JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not data:
        raise ValueError("JSON файл пуст")
    
    # Берём первый объект для определения схемы
    first_record = data[0] if isinstance(data, list) else data
    
    # Определяем тип поля
    def infer_type(value):
        if isinstance(value, bool):
            return IntegerType()
        elif isinstance(value, int):
            return IntegerType()
        elif isinstance(value, float):
            return FloatType()
        else:
            return StringType()
    
    # Создаём схему с сохранением порядка полей
    fields = []
    for field_name, field_value in first_record.items():
        if field_name == '_corrupt_record':
            continue
        if field_name == '_source_file':
            field_name = 'source_file'
        fields.append(StructField(field_name, infer_type(field_value), True))
    
    return StructType(fields)


def load_table(spark, json_path, table_name):
    """Загружает таблицу в Iceberg (как в вашем ноутбуке)"""
    
    print(f"  📄 {table_name}.json")
    
    # Определяем схему из JSON
    schema = get_schema_from_json(json_path, spark)
    print(f"     Схема: {[f.name for f in schema.fields]}")
    
    # Читаем JSON с заданной схемой
    df = spark.read.schema(schema).option("multiLine", "true").json(json_path)
    
    count = df.count()
    if count > 0:
        # Создаём таблицу и загружаем данные
        df.writeTo(f"iceberg.mine.{table_name}").createOrReplace()
        print(f"     ✅ {count} записей загружено")
        return count
    else:
        print(f"     ⚠️ файл пуст")
        return 0


def main():
    """Основная функция - как в вашем ноутбуке"""
    
    # Путь к JSON файлам
    base_path = "/home/jovyan/mine_parser/output"  # или другой путь
    
    # Список таблиц (как в вашем ноутбуке)
    tables_to_load = [
        'affected_areas', 'air_analysis', 'chronology_incident', 'company_description',
        'employee', 'equipment', 'equipment_certificate', 'equipment_issue_log',
        'equipment_maintenance', 'expert_dictionary', 'gas_analysis', 'geological_structure',
        'hypotesis_prove_facts', 'incident_description', 'inspection_description',
        'premise', 'premise_parameters', 'regulatory_document', 'seismic_event',
        'sensor_record', 'sensor_reestr', 'witness_statement'
    ]
    
    print("\n📤 ЗАГРУЗКА ДАННЫХ В ICEBERG")
    print("=" * 70)
    
    # Создаём Spark сессию
    spark = get_spark_session()
    print("✅ SparkSession с Iceberg создана!")
    
    # Создаём namespace
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.mine")
        print("✅ Schema 'nessie.mine' created")
    except Exception as e:
        print(f"⚠️ Schema: {e}")
    
    loaded = 0
    for table_name in tables_to_load:
        try:
            json_path = f"{base_path}/{table_name}.json"
            if os.path.exists(json_path):
                count = load_table(spark, json_path, table_name)
                if count > 0:
                    loaded += 1
            else:
                print(f"  ⚠️ {table_name}.json не найден")
        except Exception as e:
            print(f"     ❌ Ошибка: {str(e)[:150]}")
    
    print(f"\n✅ Загружено {loaded} из {len(tables_to_load)} таблиц")
    
    spark.stop()


if __name__ == "__main__":
    main()