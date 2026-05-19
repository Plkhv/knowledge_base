# dags/trino_setup_dag.py

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import logging


default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
}


with DAG(
    dag_id='trino_tables_setup',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['trino', 'iceberg', 'setup'],
) as dag:

    @task
    def create_tables():
        """Создание таблиц в Trino из SQL файла"""
        from trino.dbapi import connect

        conn = connect(
            host='trino',
            port=8080,
            user='trino',
            catalog='iceberg',
            schema='mine',
        )

        cursor = conn.cursor()

        sql_file_path = '/opt/airflow/mine_parser/create_tables.sql'

        logging.info(f"Reading SQL from {sql_file_path}")

        with open(sql_file_path, 'r') as f:
            sql = f.read()

        # Разбиваем на отдельные команды
        statements = [s.strip() for s in sql.split(';') if s.strip()]

        logging.info(f"Found {len(statements)} SQL statements")

        executed = 0

        for stmt in statements:
            try:
                cursor.execute(stmt)
                executed += 1
                logging.info(f"✅ Executed: {stmt[:80]}...")
            except Exception as e:
                # Игнорируем "already exists"
                if "already exists" in str(e).lower():
                    logging.warning(f"⚠️ Skipped (exists): {stmt[:80]}...")
                else:
                    logging.error(f"❌ Failed: {stmt[:80]}...")
                    raise

        logging.info(f"🎉 Executed {executed}/{len(statements)} statements")

        cursor.close()
        conn.close()


    @task
    def show_tables():
        """Показывает созданные таблицы"""
        from trino.dbapi import connect

        conn = connect(
            host='trino',
            port=8080,
            user='trino',
            catalog='iceberg',
            schema='mine',
        )

        cursor = conn.cursor()

        cursor.execute("SHOW TABLES FROM iceberg.mine")
        tables = cursor.fetchall()

        logging.info("=== TABLES IN iceberg.mine ===")

        for table in tables:
            logging.info(f"📊 {table[0]}")

        cursor.close()
        conn.close()

        return tables


    # DAG flow
    tables_created = create_tables()
    tables_list = show_tables()

    tables_created >> tables_list