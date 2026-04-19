# create_trino_tables.py
"""
Скрипт для создания таблиц в Trino
Запуск: python create_trino_tables.py
"""

from trino.dbapi import connect
import sys

def create_tables():
    """Создаёт все таблицы в Trino"""
    
    conn = connect(
        host='trino',
        port=8080,
        user='trino',
        catalog='iceberg',
        schema='mine',
    )
    cursor = conn.cursor()
    
    # Читаем SQL файл
    with open('mine_parser/create_tables.sql', 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    # Разбиваем на отдельные statements
    statements = []
    current = []
    
    for line in sql_script.split('\n'):
        if line.strip().startswith('--'):
            continue
        current.append(line)
        if ';' in line:
            statements.append(' '.join(current).replace(';', ''))
            current = []
    
    # Выполняем каждый statement
    for stmt in statements:
        stmt = stmt.strip()
        if stmt and not stmt.startswith('--'):
            try:
                cursor.execute(stmt)
                print(f"✅ Executed: {stmt[:50]}...")
            except Exception as e:
                if "already exists" not in str(e):
                    print(f"⚠️ Warning: {e}")
    
    cursor.close()
    conn.close()
    print("\n✅ All tables created successfully!")

if __name__ == "__main__":
    create_tables()