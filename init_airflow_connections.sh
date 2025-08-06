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
airflow connections delete 'pg_dwh' || true

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

airflow connections add 'pg_dwh' \
    --conn-type 'postgres' \
    --conn-host 'postgres_dwh' \
    --conn-port '5432' \
    --conn-schema 'dwh' \
    --conn-login 'dwhuser' \
    --conn-password 'dwhpass'

echo "Done creating Airflow connections."
