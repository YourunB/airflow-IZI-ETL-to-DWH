from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import os
import tempfile
from contextlib import closing

EXCLUDE_SCHEMAS = {"tiger", "topology"}
EXCLUDE_TABLES = {
    ("public", "spatial_ref_sys"),
    ("public", "geometry_columns"),
    ("public", "geography_columns"),
    ("public", "raster_columns"),
    ("public", "raster_overviews"),
    ("public", "pg_stat_statements"),
    ("public", "pg_stat_statements_info"),
}


def get_all_objects(conn_id: str):
    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        SELECT n.nspname as schema, c.relname as name, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','v','m')
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


def get_view_definition(conn_id: str, schema: str, view: str):
    hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """
        SELECT pg_get_viewdef(c.oid, true)
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s
          AND c.relname = %s
          AND c.relkind = 'v';
    """
    result = hook.get_first(sql, parameters=(schema, view))
    return result[0] if result else None


def safe_type(pg_type: str) -> str:
    basic = [
        "integer","bigint","smallint","serial","bigserial",
        "numeric","real","double precision",
        "text","varchar","character varying",
        "date","timestamp","timestamp without time zone",
        "timestamp with time zone","boolean","uuid","json","jsonb"
    ]
    return pg_type if pg_type in basic else "text"


def copy_table(source_conn: str, target_conn: str, schema: str, name: str):
    src = PostgresHook(postgres_conn_id=source_conn)
    dwh = PostgresHook(postgres_conn_id=target_conn)

    dwh.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    os.makedirs("/tmp/airflow_copy", exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=False, dir="/tmp/airflow_copy") as tmpfile:
        tmp_path = tmpfile.name

    try:
        # выгрузка из источника
        with closing(src.get_conn()) as conn_src, open(tmp_path, "w", encoding="utf-8") as f:
            with closing(conn_src.cursor()) as cur_src:
                cur_src.execute("SET statement_timeout = '30min';")  # защита от зависания
                cur_src.copy_expert(f'COPY "{schema}"."{name}" TO STDOUT WITH CSV', f)
            conn_src.commit()

        # создаем таблицу в целевой БД
        cols = get_table_columns(source_conn, schema, name)
        cols_def = ", ".join([f'"{c}" {safe_type(t)}' for c, t in cols])
        dwh.run(f'DROP TABLE IF EXISTS "{schema}"."{name}" CASCADE;')
        dwh.run(f'CREATE TABLE "{schema}"."{name}" ({cols_def});')

        # загружаем данные в целевую БД
        with closing(dwh.get_conn()) as conn_dwh, open(tmp_path, "r", encoding="utf-8") as f:
            with closing(conn_dwh.cursor()) as cur_dwh:
                cur_dwh.execute("SET statement_timeout = '30min';")
                cur_dwh.copy_expert(f'COPY "{schema}"."{name}" FROM STDIN WITH CSV', f)
            conn_dwh.commit()

        print(f"✅ {schema}.{name} скопирована")

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def ensure_placeholder_function(dwh: PostgresHook):
    dwh.run("""
        CREATE OR REPLACE FUNCTION public._(text)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$
            SELECT $1;
        $$;
    """)


def copy_view(source_conn: str, target_conn: str, schema: str, name: str):
    dwh = PostgresHook(postgres_conn_id=target_conn)
    ensure_placeholder_function(dwh)

    view_def = get_view_definition(source_conn, schema, name)
    if not view_def:
        print(f"⚠ Не удалось получить SQL для {schema}.{name}")
        return
    dwh.run(f'DROP VIEW IF EXISTS "{schema}"."{name}" CASCADE;')
    dwh.run(f'CREATE VIEW "{schema}"."{name}" AS {view_def};')
    print(f"✅ VIEW {schema}.{name} создана")


def load_sql(filename: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "sql", filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# ----------------------------------------------------------------------------------------------------------
VIEW_SQL_DVIZHENIE_DENEZHNYKH_SREDSTV_P_AND_L_RU_RU = load_sql("dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru.sql")
#-----------------------------------------------------------------------------------------------------------

@dag(
    dag_id="etl_copy_everything_safe_ru",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "replication", "full_copy"],
)
def etl_copy_everything_safe_ru():

    @task(retries=3, max_active_tis_per_dag=1)
    def copy_all_from_source(source_conn: str, target_conn: str):
        objects = get_all_objects(source_conn)

        # сначала таблицы
        for schema, name, kind in objects:
            if kind in ("r", "m") and schema not in EXCLUDE_SCHEMAS and (schema, name) not in EXCLUDE_TABLES:
                copy_table(source_conn, target_conn, schema, name)

        # потом VIEW
        for schema, name, kind in objects:
            if kind == "v" and schema not in EXCLUDE_SCHEMAS and (schema, name) not in EXCLUDE_TABLES:
                copy_view(source_conn, target_conn, schema, name)
    
    @task
    def create_view_dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru(target_conn: str):
        hook = PostgresHook(postgres_conn_id=target_conn)
        hook.run('CREATE SCHEMA IF NOT EXISTS models;')
        hook.run(VIEW_SQL_DVIZHENIE_DENEZHNYKH_SREDSTV_P_AND_L_RU_RU)
        print("✅ View dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru создана")
    
    @task
    def create_index_dvizhenie_denezhnykh_sredstv(target_conn: str):
        hook = PostgresHook(postgres_conn_id=target_conn)
        hook.run("""
        CREATE INDEX IF NOT EXISTS idx_dvizhenie_denezhnykh_sredstv_club_date
            ON models.dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru (id_club, payment_time_local);
        """)
        print("✅ Индекс создан")


    # Копируем данные все данные из БД Main + Shop
    copy1 = copy_all_from_source("pg_source3", "pg_dwh-ru")
    copy2 = copy_all_from_source("pg_source4", "pg_dwh-ru")

# ----------------------------------------------------------------------------------------------------------
    [copy1, copy2] >> create_view_dvizhenie_denezhnykh_sredstv_p_and_l_ru_ru("pg_dwh-ru") >> create_index_dvizhenie_denezhnykh_sredstv("pg_dwh-ru")
# ----------------------------------------------------------------------------------------------------------

dag = etl_copy_everything_safe_ru()
