#!/bin/bash

echo "Waiting for Airflow to be ready..."

# Ждём, пока Webserver станет доступен
until airflow connections list >/dev/null 2>&1; do
  echo "Waiting for Airflow CLI to be available..."
  sleep 2
done

echo "Creating Airflow connections..."

airflow connections delete 'pg_source1' || true
airflow connections delete 'pg_source2' || true
airflow connections delete 'pg_source3' || true
airflow connections delete 'pg_source4' || true
airflow connections delete 'pg_dwh-en' || true

airflow connections add 'pg_source1' \
    --conn-type 'postgres' \
    --conn-host 'postgres_source1' \
    --conn-port '5432' \
    --conn-schema 'source1' \
    --conn-login 'user1' \
    --conn-password 'pass1'

airflow connections add 'pg_source2' \
    --conn-type 'postgres' \
    --conn-host 'postgres_source2' \
    --conn-port '5432' \
    --conn-schema 'source2' \
    --conn-login 'user2' \
    --conn-password 'pass2'

airflow connections add 'pg_source3' \
    --conn-type 'postgres' \
    --conn-host 'postgres_source3' \
    --conn-port '5432' \
    --conn-schema 'source3' \
    --conn-login 'user3' \
    --conn-password 'pass3'

airflow connections add 'pg_source4' \
    --conn-type 'postgres' \
    --conn-host 'postgres_source4' \
    --conn-port '5432' \
    --conn-schema 'source4' \
    --conn-login 'user4' \
    --conn-password 'pass4'

airflow connections add 'pg_dwh-en' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh-en' \
    --conn-port '5432' \
    --conn-schema 'dwh-en' \
    --conn-login 'dwhuser-en' \
    --conn-password 'dwhpass-en'

airflow connections add 'pg_dwh-ru' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh-ru' \
    --conn-port '5432' \
    --conn-schema 'dwh-ru' \
    --conn-login 'dwhuser-ru' \
    --conn-password 'dwhpass-ru'

echo "Done creating Airflow connections."
