FROM apache/airflow:latest

USER airflow

RUN pip install --no-cache-dir duckdb \
    apache-airflow-providers-postgres \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-standard
