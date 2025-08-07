from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

@dag(
    dag_id="etl_payment_sales_merge",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "dwh"]
)
def etl_payment_sales_merge():
    @task
    def etl_merge():
        pg1 = PostgresHook(postgres_conn_id='pg_source1')
        pg2 = PostgresHook(postgres_conn_id='pg_source2')
        dwh = PostgresHook(postgres_conn_id='pg_dwh-en')

        payments = pg1.get_records("""
            SELECT
              id,
              id_gate,
              amount,
              id_reservation_terminal,
              id_subscription_user,
              id_reservation_shop,
              id_balance_user,
              id_user_init,
              id_parent,
              id_payment_type,
              date,
              comment
            FROM billing.payment
        """)

        sales = pg2.get_records("""
            SELECT
              id,
              id_user,
              id_user_master,
              id_user_slave,
              date_create,
              comment,
              id_club,
              transmitted,
              id_state,
              id_payment
            FROM public.sales
        """)

        sales_map = {s[-1]: s for s in sales}

        merged_rows = []
        for p in payments:
            pid = p[0]
            sale = sales_map.get(pid)
            if sale:
                merged_rows.append(p + sale)

        dwh.run("TRUNCATE TABLE customers_with_payments")
        dwh.insert_rows(table='customers_with_payments', rows=merged_rows)

    etl_merge()

dag = etl_payment_sales_merge()
