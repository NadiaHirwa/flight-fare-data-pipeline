#!/bin/bash
# Runs automatically on first container init (docker-entrypoint-initdb.d convention).
# Creates two dedicated, least-privilege roles — never uses the postgres
# superuser for application connections. See ADR-002 and the Security
# section in docs/MASTER_PLAN.md.
set -euo pipefail

# Airflow's own metadata role — the airflow-webserver/scheduler containers
# connect as this role, not as the postgres superuser.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${AIRFLOW_DB_USER} WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${AIRFLOW_DB_USER};
    GRANT ALL ON SCHEMA public TO ${AIRFLOW_DB_USER};
EOSQL

# Analytics database and its own least-privilege role.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${ANALYTICS_DB_NAME};
    CREATE USER ${ANALYTICS_DB_USER} WITH PASSWORD '${ANALYTICS_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${ANALYTICS_DB_NAME} TO ${ANALYTICS_DB_USER};
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${ANALYTICS_DB_NAME}" <<-EOSQL
    GRANT ALL ON SCHEMA public TO ${ANALYTICS_DB_USER};
EOSQL