#!/bin/bash
# Runs automatically on first container init (docker-entrypoint-initdb.d convention).
# Creates the analytics database and a least-privilege role for it, separate
# from the Airflow metadata database/role — see ADR-002 in docs/engineering_decisions.md.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE ${ANALYTICS_DB_NAME};
    CREATE USER ${ANALYTICS_DB_USER} WITH PASSWORD '${ANALYTICS_DB_PASSWORD}';
    GRANT ALL PRIVILEGES ON DATABASE ${ANALYTICS_DB_NAME} TO ${ANALYTICS_DB_USER};
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${ANALYTICS_DB_NAME}" <<-EOSQL
    GRANT ALL ON SCHEMA public TO ${ANALYTICS_DB_USER};
EOSQL
