from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pandas as pd
import json

# Служебные схемы/таблицы, которые копировать не нужно
EXCLUDE_SCHEMAS = {"pg_catalog", "information_schema", "tiger", "topology"}
EXCLUDE_TABLES = {("public", "spatial_ref_sys")}

@dag(
    dag_id="etl_full_copy_all_databases_ru",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "postgres", "replication", "full_copy"],
)
def etl_full_copy_all_databases_ru():

    def get_all_tables(conn_id: str):
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema','tiger','topology');
        """
        return hook.get_records(sql)

    def get_table_columns_with_types(conn_id: str, schema: str, table: str):
        """
        Возвращает описание колонок с info о типах (включая массивы и user-defined).
        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = """
            SELECT
                a.attname AS column_name,
                format_type(a.atttypid, a.atttypmod) AS display_type,
                tn.nspname AS type_schema,
                t.typtype AS typtype,               -- b/e/d/c/p/r
                (t.typelem <> 0) AS is_array,
                etn.nspname AS elem_type_schema,
                et.typtype AS elem_typtype
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_type t ON a.atttypid = t.oid
            JOIN pg_namespace tn ON t.typnamespace = tn.oid
            LEFT JOIN pg_type et ON t.typelem = et.oid
            LEFT JOIN pg_namespace etn ON et.typnamespace = etn.oid
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """
        rows = hook.get_records(sql, parameters=(schema, table))
        cols = []
        for (col, display_type, type_schema, typtype, is_array, elem_type_schema, elem_typtype) in rows:
            cols.append({
                "name": col,
                "display_type": display_type,
                "type_schema": type_schema,
                "typtype": typtype,
                "is_array": bool(is_array),
                "elem_type_schema": elem_type_schema,
                "elem_typtype": elem_typtype,
            })
        return cols

    def normalize_pg_type(col_info: dict) -> str:
        """
        Приводим тип к безопасному для DWH:
        - базовые типы оставляем;
        - user-defined/enum/domain/composite/range → text;
        - массивы с user-defined элементом → text[];
        - hstore оставляем как hstore (включим EXTENSION).
        """
        t = col_info["display_type"]
        typtype = col_info["typtype"]
        type_schema = col_info["type_schema"]
        is_array = col_info["is_array"]
        elem_typtype = col_info["elem_typtype"]
        elem_type_schema = col_info["elem_type_schema"]

        if t == "hstore":
            return "hstore"

        if is_array:
            if (elem_typtype in ("e", "d", "c", "p", "r")) or (elem_type_schema not in ("pg_catalog", "public")):
                return "text[]"
            return t  # например integer[], text[], uuid[] ...

        if (typtype in ("e", "d", "c", "p", "r")) or (type_schema not in ("pg_catalog", "public")):
            return "text"

        return t

    def convert_row_values(row: list, columns: list) -> list:
        """
        Готовим значения для вставки:
        - None / NaN → None
        - json/jsonb → json.dumps для dict/list
        - hstore → оставляем dict (после register_hstore адаптируется)
        - массивы → оставляем Python list (psycopg2 адаптирует к ARRAY)
        """
        out = []
        for val, col in zip(row, columns):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                out.append(None)
                continue

            t = col["target_type"]

            if t in ("json", "jsonb"):
                if isinstance(val, (dict, list)):
                    out.append(json.dumps(val, ensure_ascii=False))
                else:
                    out.append(val)
                continue

            if t == "hstore":
                # ожидается dict; если строка — не трогаем
                out.append(val)
                continue

            if t.endswith("[]"):
                # если строка с JSON-массивом — постараемся распарсить
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        out.append(parsed if isinstance(parsed, list) else [parsed])
                    except Exception:
                        # оставим как есть; psycopg2 может не понять строку
                        out.append([val])
                else:
                    out.append(val)
                continue

            out.append(val)
        return out

    @task
    def copy_all_tables(source_conn: str, target_conn: str):
        src = PostgresHook(postgres_conn_id=source_conn)
        dwh = PostgresHook(postgres_conn_id=target_conn)

        # Включаем расширения в DWH (важно для hstore)
        dwh.run("CREATE EXTENSION IF NOT EXISTS hstore;")
        # На всякий случай: если используешь образ postgis — не помешает
        dwh.run("CREATE EXTENSION IF NOT EXISTS postgis;")

        # Зарегистрируем адаптер hstore, чтобы dict → hstore
        try:
            from psycopg2.extras import register_hstore
            conn = dwh.get_conn()
            register_hstore(conn, globally=True, unicode=True)
            conn.close()
        except Exception as e:
            print(f"warn: register_hstore failed: {e}")

        tables = get_all_tables(source_conn)

        for schema, table in tables:
            if schema in EXCLUDE_SCHEMAS or (schema, table) in EXCLUDE_TABLES:
                print(f"⚠️ Пропускаем служебную таблицу {schema}.{table}")
                continue

            full_name = f'{schema}.{table}'
            print(f"=== Копируем {full_name} ===")

            # Данные из источника
            df = src.get_pandas_df(f'SELECT * FROM "{schema}"."{table}"')

            # Создаём схему в DWH
            dwh.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

            # Описание колонок и нормализация типов для DWH
            cols_info = get_table_columns_with_types(source_conn, schema, table)
            for c in cols_info:
                c["target_type"] = normalize_pg_type(c)

            # Создаём таблицу в DWH при необходимости
            cols_def = ", ".join([f'"{c["name"]}" {c["target_type"]}' for c in cols_info])
            dwh.run(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({cols_def});')

            # Если пустая — очистим (на всякий) и дальше
            if df.empty:
                print(f"Пропускаем {full_name}, пустая таблица.")
                dwh.run(f'TRUNCATE TABLE "{schema}"."{table}";')
                continue

            # NaN → None
            df = df.astype(object).where(pd.notnull(df), None)

            # Преобразуем строки под типы
            rows_raw = df.values.tolist()
            rows = [convert_row_values(r, cols_info) for r in rows_raw]

            # Перезаписываем целиком
            dwh.run(f'TRUNCATE TABLE "{schema}"."{table}";')
            dwh.insert_rows(
                table=f'{schema}.{table}',
                rows=rows,
                target_fields=[c["name"] for c in cols_info],
                commit_every=5000,
            )

    # Копируем все таблицы из первой и второй базы
    copy_all_tables("pg_source3", "pg_dwh-ru")
    copy_all_tables("pg_source4", "pg_dwh-ru")


dag = etl_full_copy_all_databases_ru()
