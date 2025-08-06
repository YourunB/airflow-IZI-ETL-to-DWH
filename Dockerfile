FROM apache/airflow:latest

USER root

RUN pip install --no-cache-dir apache-airflow-providers-postgres

RUN pip install --no-cache-dir apache-airflow-providers-common-sql \
                               apache-airflow-providers-standard

USER airflow
