#!/usr/bin/env python3
"""
Скрипт для генерации синтетических данных об инцидентах
и загрузки их в Lakehouse через Trino.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from trino.dbapi import connect
import requests

# ===================== КОНФИГУРАЦИЯ =====================
TRINO_HOST = "localhost"
TRINO_PORT = 8082
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "lakehouse"
# =========================================================

# Типы инцидентов
INCIDENT_TYPES = [
    "Пожар",
    "Взрыв",
    "Разлив нефтепродуктов",
    "Отказ оборудования",
    "Травмирование персонала",
    "Утечка газа",
    "Затопление",
    "Обрушение конструкций"
]

# Типы оборудования
EQUIPMENT_TYPES = [
    "Насос",
    "Компрессор",
    "Трубопровод",
    "Резервуар",
    "Электродвигатель",
    "Трансформатор",
    "Газораспределительная станция",
    "Котельная установка"
]

# Причины аварий
CAUSE_CATEGORIES = {
    "Технические": [
        "Износ оборудования",
        "Дефект изготовления",
        "Нарушение режима эксплуатации",
        "Отказ системы автоматики"
    ],
    "Человеческий фактор": [
        "Ошибка персонала",
        "Нарушение инструкций",
        "Недостаточная квалификация",
        "Несоблюдение техники безопасности"
    ],
    "Организационные": [
        "Отсутствие контроля",
        "Несвоевременное обслуживание",
        "Нарушение регламента",
        "Недостаток обучения"
    ],
    "Внешние": [
        "Неблагоприятные погодные условия",
        "Стихийное бедствие",
        "Действия третьих лиц",
        "Сбой электроснабжения"
    ]
}

# Местоположения
LOCATIONS = [
    "Цех №1",
    "Цех №2",
    "Цех №3",
    "Склад ГСМ",
    "Насосная станция",
    "Компрессорная",
    "Резервуарный парк",
    "Эстакада налива"
]

# Должности
POSITIONS = [
    "Оператор",
    "Машинист",
    "Инженер",
    "Начальник смены",
    "Технолог",
    "Механик",
    "Электрик",
    "Мастер"
]

def check_trino_connection():
    """Проверка подключения к Trino"""
    try:
        response = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info", timeout=5)
        if response.status_code == 200:
            print(f"✅ Trino доступен: {response.json().get('nodeVersion', {}).get('version', 'unknown')}")
            return True
    except Exception as e:
        print(f"❌ Trino недоступен: {e}")
        return False

def check_catalogs():
    """Проверка доступных каталогов"""
    try:
        response = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/catalog", timeout=5)
        catalogs = response.json()
        print(f"📁 Доступные каталоги: {catalogs}")
        return catalogs
    except Exception as e:
        print(f"⚠️ Не удалось получить список каталогов: {e}")
        return []

def random_date(start_date, end_date):
    """Генерация случайной даты"""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, delta.seconds)
    return start_date + timedelta(days=random_days, seconds=random_seconds)

def generate_incident_data(n=100):
    """Генерация данных об инцидентах"""
    incidents = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    for i in range(n):
        incident_date = random_date(start_date, end_date)
        incident_type = random.choice(INCIDENT_TYPES)
        location = random.choice(LOCATIONS)
        
        cause_category = random.choice(list(CAUSE_CATEGORIES.keys()))
        cause_detail = random.choice(CAUSE_CATEGORIES[cause_category])
        
        damage_amount = random.randint(10000, 5000000)
        injured = random.randint(0, 3)
        fatalities = random.randint(0, 1) if random.random() < 0.2 else 0
        
        incidents.append({
            "incident_id": f"INC-{2020 + random.randint(0, 4)}-{i+1:04d}",
            "incident_date": incident_date.strftime("%Y-%m-%d"),
            "incident_time": incident_date.strftime("%H:%M:%S"),
            "incident_type": incident_type,
            "location": location,
            "equipment_type": random.choice(EQUIPMENT_TYPES),
            "cause_category": cause_category,
            "cause_detail": cause_detail,
            "damage_amount": damage_amount,
            "injured_count": injured,
            "fatalities_count": fatalities,
            "description": f"Произошел {incident_type.lower()} в {location}. Причина: {cause_detail.lower()}. Ущерб: {damage_amount:,} руб.",
            "investigation_status": random.choice(["Завершено", "В работе", "Начато"]),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return pd.DataFrame(incidents)

def generate_equipment_data(n=30):
    """Генерация данных об оборудовании"""
    equipment = []
    
    # Вероятности для статуса
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
    first_names = ["Иван", "Петр", "Сергей", "Алексей", "Дмитрий", "Андрей", "Михаил", "Владимир"]
    last_names = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнов", "Васильев", "Попов", "Соколов"]
    patronymics = ["Иванович", "Петрович", "Сергеевич", "Алексеевич", "Дмитриевич", "Андреевич", "Михайлович"]
    
    for i in range(n):
        personnel.append({
            "personnel_id": f"P-{i+1:04d}",
            "full_name": f"{random.choice(last_names)} {random.choice(first_names)} {random.choice(patronymics)}",
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
                "commission_members": str(random.sample(["Иванов И.И.", "Петров П.П.", "Сидоров С.С.", "Кузнецов К.К.", "Смирнов С.В."], random.randint(2, 4))),
                "investigation_start": investigation_date.strftime("%Y-%m-%d"),
                "investigation_end": (investigation_date + timedelta(days=random.randint(7, 45))).strftime("%Y-%m-%d"),
                "root_cause": random.choice(["Несоблюдение инструкций", "Износ оборудования", "Отсутствие контроля", "Недостаточное обучение", "Проектный недостаток"]),
                "preventive_measures": random.choice(["Проведение внепланового инструктажа", "Замена оборудования", "Усиление контроля", "Дополнительное обучение персонала", "Модернизация системы безопасности"]),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    
    return pd.DataFrame(investigations)

def create_schema_and_tables():
    """Создание схемы и таблиц в Trino"""
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin"
    )
    cursor = conn.cursor()
    
    # Сначала получаем список каталогов
    try:
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        print(f"📁 Доступные каталоги: {catalogs}")
        
        if TRINO_CATALOG in catalogs:
            actual_catalog = TRINO_CATALOG
        elif "memory" in catalogs:
            print(f"⚠️ Каталог '{TRINO_CATALOG}' не найден. Используем 'memory' для тестирования")
            actual_catalog = "memory"
        else:
            actual_catalog = catalogs[0] if catalogs else "system"
            print(f"⚠️ Используем каталог: {actual_catalog}")
    except Exception as e:
        print(f"⚠️ Ошибка при проверке каталогов: {e}")
        actual_catalog = "memory"
    
    # Создаём схему
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {actual_catalog}.{TRINO_SCHEMA}")
        print(f"✅ Схема {actual_catalog}.{TRINO_SCHEMA} создана/существует")
    except Exception as e:
        print(f"⚠️ Ошибка при создании схемы: {e}")
    
    # Создаём таблицу incidents
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {actual_catalog}.{TRINO_SCHEMA}.incidents (
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
            )
        """)
        print(f"✅ Таблица incidents создана в каталоге {actual_catalog}")
    except Exception as e:
        print(f"❌ Ошибка при создании incidents: {e}")
    
    # Создаём таблицу equipment
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {actual_catalog}.{TRINO_SCHEMA}.equipment (
                equipment_id VARCHAR,
                equipment_type VARCHAR,
                manufacturer VARCHAR,
                model VARCHAR,
                installation_date VARCHAR,
                last_maintenance VARCHAR,
                location VARCHAR,
                status VARCHAR,
                created_at VARCHAR
            )
        """)
        print(f"✅ Таблица equipment создана в каталоге {actual_catalog}")
    except Exception as e:
        print(f"❌ Ошибка при создании equipment: {e}")
    
    # Создаём таблицу personnel
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {actual_catalog}.{TRINO_SCHEMA}.personnel (
                personnel_id VARCHAR,
                full_name VARCHAR,
                position VARCHAR,
                department VARCHAR,
                experience_years INTEGER,
                certification_date VARCHAR,
                created_at VARCHAR
            )
        """)
        print(f"✅ Таблица personnel создана в каталоге {actual_catalog}")
    except Exception as e:
        print(f"❌ Ошибка при создании personnel: {e}")
    
    # Создаём таблицу investigations
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {actual_catalog}.{TRINO_SCHEMA}.investigations (
                investigation_id VARCHAR,
                incident_id VARCHAR,
                commission_head VARCHAR,
                commission_members VARCHAR,
                investigation_start VARCHAR,
                investigation_end VARCHAR,
                root_cause VARCHAR,
                preventive_measures VARCHAR,
                created_at VARCHAR
            )
        """)
        print(f"✅ Таблица investigations создана в каталоге {actual_catalog}")
    except Exception as e:
        print(f"❌ Ошибка при создании investigations: {e}")
    
    cursor.close()
    conn.close()
    return actual_catalog

def insert_data_to_trino(df, table_name, catalog):
    """Вставка данных в таблицу Trino"""
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog=catalog,
        schema=TRINO_SCHEMA
    )
    cursor = conn.cursor()
    
    columns = df.columns.tolist()
    placeholders = ", ".join(["?" for _ in columns])
    columns_str = ", ".join(columns)
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    values = [tuple(row) for row in df.to_numpy()]
    
    try:
        # Разбиваем на пакеты по 100 записей
        batch_size = 100
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
        print(f"✅ В таблицу {table_name} загружено {len(df)} записей")
    except Exception as e:
        print(f"❌ Ошибка при вставке в {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    print("=" * 60)
    print("🚀 Генерация синтетических данных для Lakehouse")
    print("=" * 60)
    
    # Проверка Trino
    print("\n🔌 Проверка подключения к Trino...")
    if not check_trino_connection():
        print("❌ Trino недоступен. Убедитесь, что контейнер запущен: docker ps")
        return
    
    # Создаём таблицы и получаем каталог
    print("\n📁 Создание таблиц в Trino...")
    catalog = create_schema_and_tables()
    
    # Генерируем данные
    print("\n📊 Генерация данных...")
    incidents_df = generate_incident_data(100)
    print(f"   - Сгенерировано {len(incidents_df)} инцидентов")
    
    equipment_df = generate_equipment_data(30)
    print(f"   - Сгенерировано {len(equipment_df)} единиц оборудования")
    
    personnel_df = generate_personnel_data(50)
    print(f"   - Сгенерировано {len(personnel_df)} сотрудников")
    
    investigations_df = generate_investigation_data(incidents_df)
    print(f"   - Сгенерировано {len(investigations_df)} расследований")
    
    # Загружаем данные
    print(f"\n💾 Загрузка данных в Lakehouse (каталог: {catalog})...")
    insert_data_to_trino(incidents_df, "incidents", catalog)
    insert_data_to_trino(equipment_df, "equipment", catalog)
    insert_data_to_trino(personnel_df, "personnel", catalog)
    insert_data_to_trino(investigations_df, "investigations", catalog)
    
    print("\n" + "=" * 60)
    print("✅ Загрузка данных завершена!")
    print("=" * 60)
    print("\n📋 Для просмотра данных в админ-панели:")
    print("   - Запустите админ-панель: python main.py")
    print("   - В левой панели выберите таблицу для просмотра")
    print(f"   - Используйте каталог: {catalog}, схема: {TRINO_SCHEMA}")
    print("\n🔍 Проверка через Trino (в браузере):")
    print(f"   http://{TRINO_HOST}:{TRINO_PORT}/ui/")
    print(f"\n   SQL запрос: SELECT * FROM {catalog}.{TRINO_SCHEMA}.incidents LIMIT 5")

if __name__ == "__main__":
    main()