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
airflow connections delete 'pg_dwh_en' || true
airflow connections delete 'pg_dwh_ru' || true

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

airflow connections add 'pg_dwh_en' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh-en' \
    --conn-port '5432' \
    --conn-schema 'dwh_en' \
    --conn-login 'dwhuser_en' \
    --conn-password 'dwhpass_en'

airflow connections add 'pg_dwh_ru' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh-ru' \
    --conn-port '5432' \
    --conn-schema 'dwh_ru' \
    --conn-login 'dwhuser_ru' \
    --conn-password 'dwhpass_ru'

echo "Done creating Airflow connections."
