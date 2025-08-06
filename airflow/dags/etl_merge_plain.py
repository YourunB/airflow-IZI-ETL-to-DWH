from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

@dag(
    dag_id="etl_customers_with_payments",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "dwh"]
)
def etl_customers_with_payments():
    @task
    def etl_merge():
        pg1 = PostgresHook(postgres_conn_id='pg_source1')
        dwh = PostgresHook(postgres_conn_id='pg_dwh')

        payments = pg1.get_records("""
            SELECT id_user, amount
            FROM billing.v_payment
            WHERE date >= NOW() - INTERVAL '1 day'
        """)

        customers = pg1.get_records("""
            SELECT customer_id, phone
            FROM customers
        """)

        customer_map = {cid: phone for cid, phone in customers}

        rows = []
        for cid, amount in payments:
            phone = customer_map.get(cid)
            if phone is not None:
                rows.append((cid, phone, amount))

        dwh.run("TRUNCATE TABLE customers_with_payments")
        dwh.insert_rows(table='customers_with_payments', rows=rows)

    etl_merge()

dag = etl_customers_with_payments()
