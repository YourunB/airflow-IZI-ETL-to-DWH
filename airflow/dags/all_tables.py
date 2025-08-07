from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

@dag(
    dag_id="debug_list_tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def debug_list_tables():
    @task
    def list_tables():
        hook = PostgresHook(postgres_conn_id='pg_source1')
        tables = hook.get_records("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'billing'
        """)
        for schema, name in tables:
            print(f"{schema}.{name}")

    list_tables()

dag = debug_list_tables()
