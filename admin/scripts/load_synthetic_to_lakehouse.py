#!/usr/bin/env python3
import os
import sys
"""
Скрипт для загрузки синтетических данных в Lakehouse (MinIO + Iceberg)
Данные будут доступны через Trino и сохранятся в MinIO
"""
if sys.platform == 'win32':
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from trino.dbapi import connect
import time
import requests

# ===================== КОНФИГУРАЦИЯ =====================
TRINO_HOST = "localhost"
TRINO_PORT = 8082
TRINO_CATALOG = "iceberg"  # Используем iceberg каталог, который связан с MinIO
TRINO_SCHEMA = "lakehouse"

# Параметры подключения к Trino (без пароля)
TRINO_CONN_PARAMS = {
    "host": TRINO_HOST,
    "port": TRINO_PORT,
    "user": "admin",
    "catalog": TRINO_CATALOG,
    "schema": TRINO_SCHEMA,
}
# =========================================================

# Типы инцидентов
INCIDENT_TYPES = [
    "Пожар", "Взрыв", "Разлив нефтепродуктов", "Отказ оборудования",
    "Травмирование персонала", "Утечка газа", "Затопление", "Обрушение конструкций"
]

# Типы оборудования
EQUIPMENT_TYPES = [
    "Насос", "Компрессор", "Трубопровод", "Резервуар", "Электродвигатель",
    "Трансформатор", "Газораспределительная станция", "Котельная установка"
]

# Причины аварий
CAUSE_CATEGORIES = {
    "Технические": ["Износ оборудования", "Дефект изготовления", "Нарушение режима эксплуатации", "Отказ системы автоматики"],
    "Человеческий фактор": ["Ошибка персонала", "Нарушение инструкций", "Недостаточная квалификация", "Несоблюдение техники безопасности"],
    "Организационные": ["Отсутствие контроля", "Несвоевременное обслуживание", "Нарушение регламента", "Недостаток обучения"],
    "Внешние": ["Неблагоприятные погодные условия", "Стихийное бедствие", "Действия третьих лиц", "Сбой электроснабжения"]
}

# Местоположения
LOCATIONS = ["Цех №1", "Цех №2", "Цех №3", "Склад ГСМ", "Насосная станция", "Компрессорная", "Резервуарный парк", "Эстакада налива"]

# Должности
POSITIONS = ["Оператор", "Машинист", "Инженер", "Начальник смены", "Технолог", "Механик", "Электрик", "Мастер"]

# Имена
FIRST_NAMES = ["Иван", "Петр", "Сергей", "Алексей", "Дмитрий", "Андрей", "Михаил", "Владимир"]
LAST_NAMES = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнов", "Васильев", "Попов", "Соколов"]
PATRONYMICS = ["Иванович", "Петрович", "Сергеевич", "Алексеевич", "Дмитриевич", "Андреевич", "Михайлович"]


def check_trino():
    """Проверка доступности Trino"""
    try:
        resp = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info", timeout=5)
        print(f"✅ Trino доступен (версия: {resp.json().get('nodeVersion', {}).get('version', 'unknown')})")
        return True
    except Exception as e:
        print(f"❌ Trino недоступен: {e}")
        return False


def get_trino_connection():
    """Получение соединения с Trino"""
    return connect(**TRINO_CONN_PARAMS)


def create_iceberg_tables():
    """Создание Iceberg таблиц в MinIO через Trino"""
    print("\n📁 Создание Iceberg таблиц...")
    
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        
        # Создаём схему
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
            print(f"  ✅ Схема {TRINO_CATALOG}.{TRINO_SCHEMA} создана")
        except Exception as e:
            print(f"  ⚠️ Ошибка при создании схемы: {e}")
        
        # Создаём таблицу incidents (Iceberg с хранением в MinIO)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.incidents (
                incident_id VARCHAR,
                incident_date VARCHAR,
                incident_time VARCHAR,
                incident_type VARCHAR,
                location VARCHAR,
                equipment_type VARCHAR,
                cause_category VARCHAR,
                cause_detail VARCHAR,
                damage_amount DOUBLE,
                injured_count INTEGER,
                fatalities_count INTEGER,
                description VARCHAR,
                investigation_status VARCHAR,
                created_at VARCHAR
            ) WITH (
                format = 'PARQUET',
                location = 's3://lakehouse/incidents/'
            )
        """)
        print("  ✅ Таблица incidents создана (Iceberg + MinIO)")
        
        # Создаём таблицу equipment
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.equipment (
                equipment_id VARCHAR,
                equipment_type VARCHAR,
                manufacturer VARCHAR,
                model VARCHAR,
                installation_date VARCHAR,
                last_maintenance VARCHAR,
                location VARCHAR,
                status VARCHAR,
                created_at VARCHAR
            ) WITH (
                format = 'PARQUET',
                location = 's3://lakehouse/equipment/'
            )
        """)
        print("  ✅ Таблица equipment создана (Iceberg + MinIO)")
        
        # Создаём таблицу personnel
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.personnel (
                personnel_id VARCHAR,
                full_name VARCHAR,
                position VARCHAR,
                department VARCHAR,
                experience_years INTEGER,
                certification_date VARCHAR,
                created_at VARCHAR
            ) WITH (
                format = 'PARQUET',
                location = 's3://lakehouse/personnel/'
            )
        """)
        print("  ✅ Таблица personnel создана (Iceberg + MinIO)")
        
        # Создаём таблицу investigations
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.investigations (
                investigation_id VARCHAR,
                incident_id VARCHAR,
                commission_head VARCHAR,
                commission_members VARCHAR,
                investigation_start VARCHAR,
                investigation_end VARCHAR,
                root_cause VARCHAR,
                preventive_measures VARCHAR,
                created_at VARCHAR
            ) WITH (
                format = 'PARQUET',
                location = 's3://lakehouse/investigations/'
            )
        """)
        print("  ✅ Таблица investigations создана (Iceberg + MinIO)")


def random_date(start_date, end_date):
    """Генерация случайной даты"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)


def generate_incident_data(n=100):
    """Генерация данных об инцидентах"""
    incidents = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    for i in range(n):
        incident_date = random_date(start_date, end_date)
        
        incidents.append({
            "incident_id": f"INC-{incident_date.year}-{i+1:04d}",
            "incident_date": incident_date.strftime("%Y-%m-%d"),
            "incident_time": f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
            "incident_type": random.choice(INCIDENT_TYPES),
            "location": random.choice(LOCATIONS),
            "equipment_type": random.choice(EQUIPMENT_TYPES),
            "cause_category": random.choice(list(CAUSE_CATEGORIES.keys())),
            "cause_detail": random.choice(CAUSE_CATEGORIES[random.choice(list(CAUSE_CATEGORIES.keys()))]),
            "damage_amount": random.randint(10000, 5000000),
            "injured_count": random.randint(0, 3),
            "fatalities_count": random.randint(0, 1) if random.random() < 0.2 else 0,
            "description": f"Произошел инцидент",
            "investigation_status": random.choice(["Завершено", "В работе", "Начато"]),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(incidents)


def generate_equipment_data(n=30):
    """Генерация данных об оборудовании"""
    equipment = []
    statuses = ["Активно", "На ремонте", "Резерв", "Списано"]
    status_probs = [0.8, 0.1, 0.07, 0.03]
    
    for i in range(n):
        equipment_type = random.choice(EQUIPMENT_TYPES)
        installation_date = random_date(datetime(2010, 1, 1), datetime(2023, 12, 31))
        
        equipment.append({
            "equipment_id": f"EQ-{equipment_type[:2].upper()}-{i+1:04d}",
            "equipment_type": equipment_type,
            "manufacturer": random.choice(["ООО Техмаш", "ЗАО Энергомаш", "ОАО Нефтемаш", "ООО Газпроммаш", "Завод им. Калинина"]),
            "model": f"{random.choice(['A', 'B', 'C', 'D', 'E'])}-{random.randint(100, 999)}",
            "installation_date": installation_date.strftime("%Y-%m-%d"),
            "last_maintenance": random_date(installation_date + timedelta(days=30), datetime(2024, 12, 31)).strftime("%Y-%m-%d"),
            "location": random.choice(LOCATIONS),
            "status": random.choices(statuses, weights=status_probs, k=1)[0],
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(equipment)


def generate_personnel_data(n=50):
    """Генерация данных о персонале"""
    personnel = []
    
    for i in range(n):
        personnel.append({
            "personnel_id": f"P-{i+1:04d}",
            "full_name": f"{random.choice(LAST_NAMES)} {random.choice(FIRST_NAMES)} {random.choice(PATRONYMICS)}",
            "position": random.choice(POSITIONS),
            "department": random.choice(["Производственный цех", "Ремонтная служба", "Отдел безопасности", "Технический отдел"]),
            "experience_years": random.randint(1, 35),
            "certification_date": random_date(datetime(2020, 1, 1), datetime(2025, 1, 1)).strftime("%Y-%m-%d"),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(personnel)


def generate_investigation_data(incidents_df):
    """Генерация данных о расследованиях"""
    investigations = []
    
    for _, incident in incidents_df.iterrows():
        if random.random() < 0.7:
            investigation_date = random_date(
                datetime.strptime(incident["incident_date"], "%Y-%m-%d"),
                datetime.strptime(incident["incident_date"], "%Y-%m-%d") + timedelta(days=30)
            )
            
            investigations.append({
                "investigation_id": f"INV-{incident['incident_id']}",
                "incident_id": incident["incident_id"],
                "commission_head": random.choice(["Петров А.И.", "Сидоров В.Н.", "Кузнецов Д.С.", "Смирнов О.В."]),
                "commission_members": str(random.sample(["Иванов И.И.", "Петров П.П.", "Сидоров С.С."], 2)),
                "investigation_start": investigation_date.strftime("%Y-%m-%d"),
                "investigation_end": (investigation_date + timedelta(days=random.randint(7, 45))).strftime("%Y-%m-%d"),
                "root_cause": random.choice(["Несоблюдение инструкций", "Износ оборудования", "Отсутствие контроля", "Недостаточное обучение"]),
                "preventive_measures": random.choice(["Проведение инструктажа", "Замена оборудования", "Усиление контроля", "Дополнительное обучение"]),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    
    return pd.DataFrame(investigations)


def insert_dataframe_to_trino(df, table_name):
    """Вставка DataFrame в таблицу Trino"""
    if df.empty:
        print(f"  ⚠️ Таблица {table_name} пуста, пропуск")
        return
    
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        
        # Формируем INSERT запрос
        columns = df.columns.tolist()
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Конвертируем в список кортежей
        values = [tuple(row) for row in df.to_numpy()]
        
        # Вставляем пакетами по 100 записей
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
        
        print(f"  ✅ В таблицу {table_name} загружено {len(df)} записей")


def verify_data():
    """Проверка загруженных данных"""
    print("\n🔍 Проверка загруженных данных...")
    
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        
        # Проверяем incidents
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.incidents")
        count = cursor.fetchone()[0]
        print(f"  📊 incidents: {count} записей")
        
        # Проверяем equipment
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.equipment")
        count = cursor.fetchone()[0]
        print(f"  📊 equipment: {count} записей")
        
        # Проверяем personnel
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.personnel")
        count = cursor.fetchone()[0]
        print(f"  📊 personnel: {count} записей")
        
        # Проверяем investigations
        cursor.execute(f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.investigations")
        count = cursor.fetchone()[0]
        print(f"  📊 investigations: {count} записей")
        
        # Пример данных
        cursor.execute(f"SELECT incident_id, incident_type, incident_date FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.incidents LIMIT 3")
        print("\n  📋 Пример инцидентов:")
        for row in cursor.fetchall():
            print(f"     - {row}")


def main():
    print("=" * 60)
    print("🚀 Загрузка синтетических данных в Lakehouse (MinIO + Iceberg)")
    print("=" * 60)
    
    # Проверка Trino
    if not check_trino():
        print("❌ Невозможно продолжить. Убедитесь, что Trino запущен.")
        return
    
    # Создание таблиц Iceberg
    create_iceberg_tables()
    
    # Генерация данных
    print("\n📊 Генерация синтетических данных...")
    
    incidents_df = generate_incident_data(100)
    print(f"  - Сгенерировано инцидентов: {len(incidents_df)}")
    
    equipment_df = generate_equipment_data(30)
    print(f"  - Сгенерировано оборудования: {len(equipment_df)}")
    
    personnel_df = generate_personnel_data(50)
    print(f"  - Сгенерировано сотрудников: {len(personnel_df)}")
    
    investigations_df = generate_investigation_data(incidents_df)
    print(f"  - Сгенерировано расследований: {len(investigations_df)}")
    
    # Загрузка данных
    print("\n💾 Загрузка данных в Lakehouse (Iceberg + MinIO)...")
    
    insert_dataframe_to_trino(incidents_df, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.incidents")
    insert_dataframe_to_trino(equipment_df, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.equipment")
    insert_dataframe_to_trino(personnel_df, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.personnel")
    insert_dataframe_to_trino(investigations_df, f"{TRINO_CATALOG}.{TRINO_SCHEMA}.investigations")
    
    # Проверка
    verify_data()
    
    print("\n" + "=" * 60)
    print("✅ Загрузка завершена!")
    print("=" * 60)
    print("\n📋 Данные сохранены в MinIO (бакет 'lakehouse')")
    print("🔍 Просмотр через админ-панель: http://localhost:8501")
    print("🔍 Просмотр через MinIO Console: http://localhost:9001 (admin/password)")
    print("🔍 SQL запросы через Trino: http://localhost:8082")


if __name__ == "__main__":
    main()