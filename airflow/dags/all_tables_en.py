from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import os
import tempfile

# Таблицы, которые нужно исключить (системные PostGIS)
EXCLUDE_SCHEMAS = {"tiger", "topology"}
EXCLUDE_TABLES = {
    ("public", "spatial_ref_sys"),  # тоже системная
}

@dag(
    dag_id="etl_copy_everything_safe",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "replication", "full_copy"],
)
def etl_copy_everything_safe():

    def get_all_tables(conn_id: str):
        """Берём все таблицы"""
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = """
            SELECT n.nspname as schema, c.relname as table
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog','information_schema');
        """
        return hook.get_records(sql)

    def get_table_columns(conn_id: str, schema: str, table: str):
        hook = PostgresHook(postgres_conn_id=conn_id)
        sql = """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """
        return hook.get_records(sql, parameters=(schema, table))

    def safe_type(pg_type: str) -> str:
        """Базовые типы оставляем, остальные превращаем в TEXT"""
        basic = ["integer","bigint","smallint","serial","bigserial",
                 "numeric","real","double precision",
                 "text","varchar","character varying",
                 "date","timestamp","timestamp without time zone",
                 "timestamp with time zone","boolean","uuid","json","jsonb"]
        if pg_type in basic:
            return pg_type
        return "text"

    @task
    def copy_table(source_conn: str, target_conn: str, schema: str, table: str):
        if schema in EXCLUDE_SCHEMAS or (schema, table) in EXCLUDE_TABLES:
            print(f"⏭ Пропускаем {schema}.{table} (системная PostGIS таблица)")
            return

        src = PostgresHook(postgres_conn_id=source_conn)
        dwh = PostgresHook(postgres_conn_id=target_conn)

        os.makedirs("/tmp/airflow_copy", exist_ok=True)
        tmpfile = tempfile.NamedTemporaryFile(delete=False, dir="/tmp/airflow_copy")
        tmp_path = tmpfile.name
        tmpfile.close()

        try:
            # Выгрузка в CSV
            with src.get_conn() as conn_src, open(tmp_path, "w", encoding="utf-8") as f:
                with conn_src.cursor() as cur_src:
                    cur_src.copy_expert(f'COPY "{schema}"."{table}" TO STDOUT WITH CSV', f)

            # Создаём схему
            dwh.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

            # Создаём таблицу
            cols = get_table_columns(source_conn, schema, table)
            cols_def = ", ".join([f'"{c}" {safe_type(t)}' for c, t in cols])
            dwh.run(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;')
            dwh.run(f'CREATE TABLE "{schema}"."{table}" ({cols_def});')

            # Загружаем CSV
            with dwh.get_conn() as conn_dwh, open(tmp_path, "r", encoding="utf-8") as f:
                with conn_dwh.cursor() as cur_dwh:
                    cur_dwh.copy_expert(f'COPY "{schema}"."{table}" FROM STDIN WITH CSV', f)
                conn_dwh.commit()

            print(f"✅ {schema}.{table} скопирована")

        except Exception as e:
            raise RuntimeError(f"Ошибка при копировании {schema}.{table}: {e}")

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ТАСКИ СОЗДАЮТСЯ ПРИ ПОСТРОЕНИИ DAG
    for source_conn in ["pg_source1", "pg_source2"]:
        tables = get_all_tables(source_conn)
        for schema, table in tables:
            copy_table.override(task_id=f"copy_{source_conn}_{schema}_{table}")(
                source_conn, "pg_dwh-en", schema, table
            )

dag = etl_copy_everything_safe()
