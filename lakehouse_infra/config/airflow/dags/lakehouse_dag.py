# config/airflow/dags/lakehouse_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def process_raw_message(**context):
    """Обработка сырого сообщения из Kafka"""
    message = context['message']
    # Базовая обработка, валидация
    print(f"Processing message: {message}")
    return message

def save_to_bronze(**context):
    """Сохранение в Bronze слой (сырые данные)"""
    # Здесь будет сохранение в Iceberg через Spark
    pass

with DAG(
    'lakehouse_ingestion_dag',
    default_args=default_args,
    description='Ingest data from Kafka to Iceberg Lakehouse',
    schedule_interval='*/5 * * * *',  # каждые 5 минут
    catchup=False,
    tags=['lakehouse', 'iceberg', 'kafka'],
) as dag:

    # Потребление из Kafka
    consume_kafka = ConsumeFromTopicOperator(
        task_id='consume_from_kafka',
        kafka_config_id='kafka_default',
        topics=['{{ var.value.KAFKA_TOPIC_RAW_EVENTS }}'],
        apply_function='process_raw_message',
        poll_timeout=10,
        max_messages=100,
    )

    # Запись в Bronze слой через Spark
    spark_bronze_write = SparkSubmitOperator(
        task_id='spark_bronze_write',
        application='/opt/airflow/dags/spark_jobs/bronze_write.py',
        name='bronze_write',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.sql.catalog.polaris': 'org.apache.polaris.spark.SparkCatalog',
            'spark.sql.catalog.polaris.uri': 'http://polaris:8181/api/catalog',
            'spark.sql.catalog.polaris.warehouse': 'lakehouse',
            'spark.sql.catalog.polaris.credential': Variable.get('POLARIS_CREDENTIAL'),
        }
    )

    consume_kafka >> spark_bronze_write