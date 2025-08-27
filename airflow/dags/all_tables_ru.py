from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
import os
import tempfile

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
FORBIDDEN_FUNCS = ["pg_stat_statements", "pg_stat_statements_info", "_("]


def is_safe_view(view_def: str) -> bool:
    return not any(fn in view_def for fn in FORBIDDEN_FUNCS)


@dag(
    dag_id="etl_copy_everything_safe_ru",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "replication", "full_copy"],
)
def etl_copy_everything_safe_ru():

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

    @task(retries=3)
    def copy_object(source_conn: str, target_conn: str, schema: str, name: str, kind: str):
        if schema in EXCLUDE_SCHEMAS or (schema, name) in EXCLUDE_TABLES:
            print(f"⏭ Пропускаем {schema}.{name}")
            return

        src = PostgresHook(postgres_conn_id=source_conn)
        dwh = PostgresHook(postgres_conn_id=target_conn)
        dwh.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        if kind in ("r", "m"):
            os.makedirs("/tmp/airflow_copy", exist_ok=True)
            tmpfile = tempfile.NamedTemporaryFile(delete=False, dir="/tmp/airflow_copy")
            tmp_path = tmpfile.name
            tmpfile.close()
            try:
                with src.get_conn() as conn_src, open(tmp_path, "w", encoding="utf-8") as f:
                    with conn_src.cursor() as cur_src:
                        cur_src.copy_expert(f'COPY "{schema}"."{name}" TO STDOUT WITH CSV', f)

                cols = get_table_columns(source_conn, schema, name)
                cols_def = ", ".join([f'"{c}" {safe_type(t)}' for c, t in cols])
                dwh.run(f'DROP TABLE IF EXISTS "{schema}"."{name}" CASCADE;')
                dwh.run(f'CREATE TABLE "{schema}"."{name}" ({cols_def});')

                with dwh.get_conn() as conn_dwh, open(tmp_path, "r", encoding="utf-8") as f:
                    with conn_dwh.cursor() as cur_dwh:
                        cur_dwh.copy_expert(f'COPY "{schema}"."{name}" FROM STDIN WITH CSV', f)
                    conn_dwh.commit()

                print(f"✅ {schema}.{name} скопирована")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        elif kind == "v":
            view_def = get_view_definition(source_conn, schema, name)
            if not view_def:
                print(f"⚠ Не удалось получить SQL для {schema}.{name}")
                return
            if not is_safe_view(view_def):
                print(f"⏭ Пропускаем {schema}.{name} (неподдерживаемые функции)")
                return
            dwh.run(f'DROP VIEW IF EXISTS "{schema}"."{name}" CASCADE;')
            dwh.run(f'CREATE VIEW "{schema}"."{name}" AS {view_def};')
            print(f"✅ VIEW {schema}.{name} создана")

    # Гарантируем: сначала таблицы, потом VIEW
    for source_conn in ["pg_source3", "pg_source4"]:
        objects = get_all_objects(source_conn)

        with TaskGroup(group_id=f"{source_conn}_tables") as tables_group:
            for schema, name, kind in objects:
                if kind in ("r", "m"):
                    copy_object.override(task_id=f"copy_{schema}_{name}")(
                        source_conn, "pg_dwh-ru", schema, name, kind
                    )

        with TaskGroup(group_id=f"{source_conn}_views") as views_group:
            for schema, name, kind in objects:
                if kind == "v":
                    copy_object.override(task_id=f"copy_{schema}_{name}")(
                        source_conn, "pg_dwh-ru", schema, name, kind
                    )

        tables_group >> views_group


dag = etl_copy_everything_safe_ru()
