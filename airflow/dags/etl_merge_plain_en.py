from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import duckdb
import pandas as pd

@dag(
    dag_id="etl_vpayment_sales_merge_ru",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "duckdb", "merge"]
)
def etl_vpayment_sales_merge_ru():
    @task
    def merge_and_insert():
        pg1 = PostgresHook(postgres_conn_id='pg_source1')
        pg2 = PostgresHook(postgres_conn_id='pg_source2')
        dwh = PostgresHook(postgres_conn_id='pg_dwh-en')

        # Получаем данные
        v_payment_df = pd.DataFrame(pg1.get_records("""
            SELECT
              id,
              id_payment_type,
              payment_type_name,
              grp_payment_type,
              id_club,
              id_user,
              id_type_balance,
              name_balance,
              amount,
              id_reservation_terminal,
              id_subscription_user,
              id_reservation_shop,
              id_user_init,
              id_parent,
              date,
              comment
            FROM billing.v_payment
        """))

        sales_df = pd.DataFrame(pg2.get_records("""
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
        """))

        # Объединяем через DuckDB
        con = duckdb.connect()
        con.register('v_payment', v_payment_df)
        con.register('sales', sales_df)

        merged_df = con.execute("""
            SELECT
                vp.id AS payment_id,
                vp.id_payment_type,
                vp.payment_type_name,
                vp.grp_payment_type,
                vp.id_club,
                vp.id_user,
                vp.id_type_balance,
                vp.name_balance,
                vp.amount,
                vp.id_reservation_terminal,
                vp.id_subscription_user,
                vp.id_reservation_shop,
                vp.id_user_init,
                vp.id_parent,
                vp.date AS payment_date,
                vp.comment AS payment_comment,

                s.id AS sale_id,
                s.id_user AS sale_user_id,
                s.id_user_master AS sale_user_master_id,
                s.id_user_slave AS sale_user_slave_id,
                s.date_create AS sale_date,
                s.comment AS sale_comment,
                s.id_club AS sale_club_id,
                s.transmitted AS sale_transmitted,
                s.id_state AS sale_state_id,
                s.id_payment AS sale_payment_id
            FROM v_payment vp
            JOIN sales s ON vp.id = s.id_payment
        """).fetchdf()

        # Вставка
        dwh.run("TRUNCATE TABLE customers_with_payments")
        dwh.insert_rows(
            table='customers_with_payments',
            rows=merged_df.values.tolist(),
            target_fields=list(merged_df.columns),
            commit_every=1000
        )

    merge_and_insert()

dag = etl_vpayment_sales_merge_ru()
