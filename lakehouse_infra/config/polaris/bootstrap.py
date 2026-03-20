#!/usr/bin/env python3
"""
Скрипт для инициализации Apache Polaris:
- Создание каталогов (catalogs) для Iceberg
- Создание principal (пользователя)
- Настройка прав доступа
"""

import os
import time
import requests
import json
from requests.auth import HTTPBasicAuth

# Конфигурация из переменных окружения
POLARIS_HOST = os.getenv("POLARIS_HOST", "polaris")
POLARIS_PORT = os.getenv("POLARIS_PORT", "8181")
POLARIS_ADMIN_USER = os.getenv("POLARIS_ADMIN_USER", "admin")
POLARIS_ADMIN_PASSWORD = os.getenv("POLARIS_ADMIN_PASSWORD", "admin123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

BASE_URL = f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/management/v1"

def wait_for_polaris():
    """Ожидание готовности Polaris API"""
    print("Ожидание готовности Polaris...")
    for i in range(30):
        try:
            response = requests.get(f"http://{POLARIS_HOST}:{POLARIS_PORT}/healthcheck")
            if response.status_code == 200:
                print("Polaris готов к работе")
                return True
        except:
            pass
        time.sleep(2)
    raise Exception("Polaris не запустился вовремя")

def get_token():
    """Получение токена администратора"""
    print("Получение токена администратора...")
    auth = HTTPBasicAuth(POLARIS_ADMIN_USER, POLARIS_ADMIN_PASSWORD)
    response = requests.post(
        f"{BASE_URL}/auth/tokens",
        auth=auth,
        json={"principalType": "USER"}
    )
    response.raise_for_status()
    token = response.json()["token"]
    print("Токен получен")
    return token

def create_catalog(token, name, bucket):
    """Создание каталога в Polaris"""
    print(f"Создание каталога '{name}' (bucket: {bucket})...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    catalog_config = {
        "name": name,
        "type": "INTERNAL",
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [f"s3://{bucket}/"],
            "s3Config": {
                "endpoint": f"http://{MINIO_ENDPOINT}",
                "accessKey": MINIO_ACCESS_KEY,
                "secretKey": MINIO_SECRET_KEY,
                "region": "us-east-1",
                "pathStyleAccess": True
            }
        }
    }
    
    response = requests.post(
        f"{BASE_URL}/catalogs",
        headers=headers,
        json=catalog_config
    )
    
    if response.status_code == 409:
        print(f"Каталог '{name}' уже существует")
        return
    
    response.raise_for_status()
    print(f"Каталог '{name}' создан")

def create_principal(token, name):
    """Создание principal (пользователя)"""
    print(f"Создание principal '{name}'...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    principal_config = {
        "name": name,
        "type": "USER"
    }
    
    response = requests.post(
        f"{BASE_URL}/principals",
        headers=headers,
        json=principal_config
    )
    
    if response.status_code == 409:
        print(f"Principal '{name}' уже существует")
        # Получаем существующего principal
        response = requests.get(
            f"{BASE_URL}/principals/{name}",
            headers=headers
        )
        response.raise_for_status()
        return response.json()
    
    response.raise_for_status()
    print(f"Principal '{name}' создан")
    return response.json()

def create_principal_role(token, name):
    """Создание роли для principal"""
    print(f"Создание роли '{name}'...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    role_config = {
        "name": name
    }
    
    response = requests.post(
        f"{BASE_URL}/principal-roles",
        headers=headers,
        json=role_config
    )
    
    if response.status_code == 409:
        print(f"Роль '{name}' уже существует")
        return
    
    response.raise_for_status()
    print(f"Роль '{name}' создана")

def assign_role_to_principal(token, principal_name, role_name):
    """Назначение роли principal"""
    print(f"Назначение роли '{role_name}' пользователю '{principal_name}'...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.put(
        f"{BASE_URL}/principals/{principal_name}/principal-roles/{role_name}",
        headers=headers
    )
    
    if response.status_code == 409:
        print(f"Роль уже назначена")
        return
    
    response.raise_for_status()
    print(f"Роль назначена")

def grant_catalog_access(token, role_name, catalog_name, privileges):
    """Предоставление доступа к каталогу для роли"""
    print(f"Настройка прав доступа к каталогу '{catalog_name}' для роли '{role_name}'...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    for privilege in privileges:
        response = requests.put(
            f"{BASE_URL}/principal-roles/{role_name}/catalogs/{catalog_name}/privileges/{privilege}",
            headers=headers
        )
        
        if response.status_code == 409:
            print(f"Привилегия {privilege} уже назначена")
            continue
        
        response.raise_for_status()
        print(f"Привилегия {privilege} назначена")

def create_client_credentials(token, principal_name):
    """Создание clientId/clientSecret для principal"""
    print(f"Генерация clientId/clientSecret для '{principal_name}'...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        f"{BASE_URL}/principals/{principal_name}/credentials",
        headers=headers
    )
    response.raise_for_status()
    
    creds = response.json()
    print(f"Client credentials созданы")
    return creds

def main():
    """Основная функция инициализации"""
    print("\n" + "="*50)
    print("Инициализация Apache Polaris")
    print("="*50 + "\n")
    
    # Ожидание готовности
    wait_for_polaris()
    
    # Получение токена
    token = get_token()
    
    # Создание каталогов
    create_catalog(token, "lakehouse", "lakehouse")
    create_catalog(token, "warehouse", "warehouse")
    
    # Создание principal
    principal = create_principal(token, "user1")
    
    # Создание роли
    create_principal_role(token, "catalog_admin")
    
    # Назначение роли principal
    assign_role_to_principal(token, "user1", "catalog_admin")
    
    # Предоставление прав доступа к каталогам
    grant_catalog_access(token, "catalog_admin", "lakehouse", 
                        ["CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA", "CATALOG_READ_PROPERTIES"])
    grant_catalog_access(token, "catalog_admin", "warehouse",
                        ["CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA", "CATALOG_READ_PROPERTIES"])
    
    # Создание client credentials
    creds = create_client_credentials(token, "user1")
    
    print("\n" + "="*50)
    print("Инициализация Polaris завершена!")
    print("="*50)
    print("\nДанные для подключения Spark:")
    print(f"   Client ID: {creds.get('clientId', 'N/A')}")
    print(f"   Client Secret: {creds.get('clientSecret', 'N/A')}")
    print("\nEndpoints:")
    print(f"   Polaris REST API: http://localhost:8181/api/catalog")
    print(f"   MinIO Console: http://localhost:9001")
    print(f"   Spark Master UI: http://localhost:8081")
    print(f"   Trino UI: http://localhost:8082")
    print(f"   Kafka UI: http://localhost:8080")
    print(f"   Airflow: http://localhost:8085")
    print(f"   Jupyter: http://localhost:8888")
    print("\n" + "="*50)

if __name__ == "__main__":
    main()