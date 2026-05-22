#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
import requests
from trino.dbapi import connect

if sys.platform == "win32":
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")


# ===================== КОНФИГУРАЦИЯ =====================
TRINO_HOST = "localhost"
TRINO_PORT = 8082
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "lakehouse"
TRINO_USER = "admin"

TRINO_CONN_PARAMS = {
    "host": TRINO_HOST,
    "port": TRINO_PORT,
    "user": TRINO_USER,
    "catalog": TRINO_CATALOG,
    "schema": TRINO_SCHEMA,
}
# ========================================================


ModelType = Literal["string", "int", "float", "date", "timestamp", "time", "boolean"]


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    model_type: ModelType

    @property
    def trino_type(self) -> str:
        return {
            "string": "VARCHAR",
            "int": "INTEGER",
            "float": "DOUBLE",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            "time": "TIME",
            "boolean": "BOOLEAN",
        }.get(self.model_type, "VARCHAR")

    @property
    def insert_expr(self) -> str:
        # Всегда приводим типы на стороне Trino.
        # Для нестроковых типов используем TRY_CAST, чтобы "грязные" значения
        # (например, текст в числовом поле) превращались в NULL, а не валили загрузку.
        if self.trino_type == "VARCHAR":
            return "CAST(? AS VARCHAR)"
        return f"TRY_CAST(? AS {self.trino_type})"


def _repo_root() -> Path:
    # .../admin/scripts/<this_file> -> parents[2] == repo root
    return Path(__file__).resolve().parents[2]


def check_trino() -> bool:
    try:
        resp = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info", timeout=5)
        version = resp.json().get("nodeVersion", {}).get("version", "unknown")
        print(f"✅ Trino доступен (версия: {version})")
        return True
    except Exception as e:
        print(f"❌ Trino недоступен: {e}")
        return False


def get_trino_connection():
    return connect(**TRINO_CONN_PARAMS)


_identifier_re = re.compile(r"[^a-z0-9_]+")


def normalize_identifier(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"\s+", "_", name)
    name = _identifier_re.sub("_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def make_unique(names: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        base = n
        if base not in seen:
            seen[base] = 1
            out.append(base)
            continue
        seen[base] += 1
        out.append(f"{base}_{seen[base]}")
    return out


def read_model_xlsx(model_path: Path) -> dict[str, list[ColumnSpec]]:
    df = pd.read_excel(model_path)

    required = {"Таблица", "Атрибут", "Тип данных"}
    missing = required - set(df.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(
            f"В файле модели нет обязательных колонок: {missing_str}. "
            f"Найдено: {list(df.columns)}"
        )

    df = df[["Таблица", "Атрибут", "Тип данных"]].copy()
    df["Таблица"] = df["Таблица"].astype(str)
    df["Атрибут"] = df["Атрибут"].astype(str)
    df["Тип данных"] = df["Тип данных"].astype(str).str.strip().str.lower()

    schema: dict[str, list[ColumnSpec]] = {}

    for table_name, g in df.groupby("Таблица", sort=False):
        t = normalize_identifier(table_name)
        if not t:
            continue

        raw_cols = [normalize_identifier(x) for x in g["Атрибут"].tolist()]
        raw_types = g["Тип данных"].tolist()

        cols = make_unique([c if c else "col" for c in raw_cols])
        specs: list[ColumnSpec] = []
        for col, typ in zip(cols, raw_types, strict=True):
            model_type: ModelType
            if typ in {"string", "int", "float", "date", "timestamp", "time", "boolean"}:
                model_type = typ  # type: ignore[assignment]
            else:
                model_type = "string"
            specs.append(ColumnSpec(name=col, model_type=model_type))

        schema[t] = specs

    return schema


def create_iceberg_tables(schema: dict[str, list[ColumnSpec]], *, run_id: str, base_location: str):
    print("\n📁 Создание Iceberg таблиц...")

    created: set[str] = set()

    with get_trino_connection() as conn:
        cursor = conn.cursor()

        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}")
            print(f"  ✅ Схема {TRINO_CATALOG}.{TRINO_SCHEMA} создана")
        except Exception as e:
            print(f"  ⚠️ Ошибка при создании схемы: {e}")

        for table_name, cols in schema.items():
            full_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"

            # Важно: DROP TABLE не очищает S3. Поэтому используем уникальный location на каждую таблицу.
            location = f"{base_location.rstrip('/')}/{table_name}/{run_id}"

            try:
                cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
            except Exception as e:
                print(f"  ⚠️ Не удалось удалить таблицу {full_name}: {e}")

            cols_sql = ",\n    ".join([f'"{c.name}" {c.trino_type}' for c in cols])
            create_sql = f"""
CREATE TABLE {full_name} (
    {cols_sql}
) WITH (
    format = 'PARQUET',
    location = '{location}'
)
"""
            try:
                cursor.execute(create_sql)
                created.add(table_name)
                print(f"  ✅ Таблица {table_name} создана ({len(cols)} колонок)")
            except Exception as e:
                print(f"  ❌ Ошибка создания таблицы {table_name}: {e}")

    return created


def _read_json_records(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data is None:
        return []
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    raise ValueError(f"Неожиданный формат JSON: {type(data)}")


def find_table_data_file(table_name: str, output_dirs: list[Path]) -> Path | None:
    for d in output_dirs:
        p = d / f"{table_name}.json"
        if p.exists():
            return p
    return None


def dataframe_from_records(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame.from_records(records)

    # Совместимость с пайплайном из lakehouse_infra: _source_file -> source_file
    if "_source_file" in df.columns and "source_file" not in df.columns:
        df = df.rename(columns={"_source_file": "source_file"})

    # Не грузим системное поле ошибок
    if "_corrupt_record" in df.columns:
        df = df.drop(columns=["_corrupt_record"])

    return df


def _coerce_param_value(value):
    if value is None:
        return None

    # pandas Timestamp/NaT
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()

    # datetime/date/time -> ISO string
    try:
        from datetime import date, datetime, time

        if isinstance(value, (datetime, date, time)):
            return value.isoformat()
    except Exception:
        pass

    # numpy scalar -> python scalar
    try:
        import numpy as _np

        if isinstance(value, _np.generic):
            value = value.item()
    except Exception:
        pass

    # NaN / NaT
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # dict/list -> JSON string
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False)

    return value


def insert_dataframe_to_trino(df: pd.DataFrame, *, table_name: str, cols: list[ColumnSpec], batch_size: int = 200):
    if df.empty:
        print(f"  ⚠️ {table_name}: данных нет, пропуск")
        return 0

    full_name = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}"

    # Подгоняем колонки под схему
    schema_cols = [c.name for c in cols]
    df = df.copy()
    for c in schema_cols:
        if c not in df.columns:
            df[c] = None

    df = df[schema_cols]

    # Приводим NaN/NaT -> None (важно: делаем dtype=object, иначе pandas может
    # обратно превратить None в NaN при формировании numpy-массива).
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    quoted_cols = ", ".join([f'"{c.name}"' for c in cols])
    row_expr = ", ".join([c.insert_expr for c in cols])

    values = list(df.itertuples(index=False, name=None))
    total = len(values)

    print(f"  ⏳ {table_name}: вставка {total} строк...")

    inserted = 0
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        for i in range(0, total, batch_size):
            batch_rows = values[i : i + batch_size]
            values_sql = ", ".join([f"({row_expr})" for _ in batch_rows])
            insert_sql = f"INSERT INTO {full_name} ({quoted_cols}) VALUES {values_sql}"

            flat_params = []
            for row in batch_rows:
                for v in row:
                    flat_params.append(_coerce_param_value(v))

            cursor.execute(insert_sql, tuple(flat_params))
            try:
                conn.commit()
            except Exception:
                pass
            inserted += len(batch_rows)

    print(f"  ✅ {table_name}: загружено {inserted} записей")
    return inserted


def main(argv: list[str] | None = None) -> int:
    root = _repo_root()

    parser = argparse.ArgumentParser(
        description="Загрузка таблиц в Lakehouse (MinIO + Iceberg) по модели из Excel",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=str(root / "admin" / "scripts" / "Модель.xlsx"),
        help="Путь к Модель.xlsx",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(root / "output"),
        help="Папка с JSON (результат run_parsers.py)",
    )
    parser.add_argument(
        "--output2",
        type=str,
        default=str(root / "mine_parser" / "output"),
        help="Доп. папка с JSON (если используете mine_parser/output)",
    )
    parser.add_argument(
        "--create-only",
        action="store_true",
        help="Только создать таблицы, не загружать данные",
    )
    parser.add_argument(
        "--base-location",
        type=str,
        default="s3://lakehouse",
        help="S3 префикс для Iceberg (bucket/prefix)",
    )

    args = parser.parse_args(argv)

    print("=" * 76)
    print("🚀 Загрузка данных в Lakehouse (MinIO + Iceberg) по Модель.xlsx")
    print("=" * 76)

    if not check_trino():
        return 2

    model_path = Path(args.model)
    if not model_path.exists():
        print(f"❌ Модель не найдена: {model_path}")
        return 2

    output_dirs = [Path(args.output), Path(args.output2)]

    schema = read_model_xlsx(model_path)
    print(f"\n📊 Найдено таблиц в модели: {len(schema)}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
    created = create_iceberg_tables(schema, run_id=run_id, base_location=args.base_location)

    if args.create_only:
        print("\n✅ Таблицы созданы (create-only)")
        return 0

    print("\n💾 Загрузка данных из JSON (если файлы есть)...")

    total_inserted = 0
    tables_with_data = 0

    for table_name, cols in schema.items():
        if table_name not in created:
            print(f"  ⚠️ {table_name}: таблица не создана, пропуск загрузки")
            continue

        data_file = find_table_data_file(table_name, output_dirs)
        if not data_file:
            print(f"  ⚠️ {table_name}: JSON не найден (ожидалось в {', '.join(map(str, output_dirs))})")
            continue

        records = _read_json_records(data_file)
        df = dataframe_from_records(records)
        if df.empty:
            print(f"  ⚠️ {table_name}: {data_file.name} пустой")
            continue

        try:
            inserted = insert_dataframe_to_trino(df, table_name=table_name, cols=cols)
            if inserted:
                tables_with_data += 1
                total_inserted += inserted
        except Exception as e:
            print(f"  ❌ {table_name}: ошибка загрузки данных: {e}")
            continue

    print("\n" + "=" * 76)
    print(f"✅ Готово: создано таблиц {len(created)}/{len(schema)}, загружено таблиц {tables_with_data}, строк {total_inserted}")
    print("=" * 76)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
