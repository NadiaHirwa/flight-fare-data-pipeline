#!/bin/bash
# Runs automatically on first container init (docker-entrypoint-initdb.d convention).
# Creates two dedicated, least-privilege roles — never uses the postgres
# superuser for application connections. See ADR-002 and the Security
# section in docs/MASTER_PLAN.md.
#
# Note this runs exactly once, against an empty data directory, and BEFORE any
# of include/sql/analytics/ has been applied. So it cannot grant on tables that
# do not exist yet — which is precisely the gap that used to break the
# pipeline. The fix is in three parts, all below:
#
#   1. ALTER DEFAULT PRIVILEGES  — anything the SUPERUSER creates in this schema
#      later is automatically readable/writable by analytics_writer. Plain
#      "GRANT ALL ON SCHEMA" does not do this; it applies to the schema, not to
#      tables created in it afterwards.
#   2. ALTER SCHEMA ... OWNER    — so tables analytics_writer creates itself are
#      unambiguously its own.
#   3. an idempotent ownership sweep — grants are not sufficient for every
#      operation. CREATE INDEX requires *ownership* of the table, not privileges
#      on it, so kpi_top_routes.sql fails with "must be owner of table" against
#      a superuser-owned table no matter how many grants exist. The sweep is a
#      no-op on a fresh volume and repairs an existing one if re-run.
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

    -- (2) analytics_writer owns the schema, so anything it creates there is
    -- its own and needs no follow-up fix.
    ALTER SCHEMA public OWNER TO ${ANALYTICS_DB_USER};

    -- (1) Applies to objects created by the role running this statement — the
    -- superuser. Without it, DDL applied as postgres produces tables that
    -- analytics_writer cannot even SELECT from.
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON TABLES TO ${ANALYTICS_DB_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT ALL ON SEQUENCES TO ${ANALYTICS_DB_USER};

    -- (3) Ownership sweep. Empty on a fresh volume; the loop exists so that
    -- re-running this script against a database whose tables were created by
    -- the superuser repairs them rather than requiring a manual ALTER TABLE
    -- per table. Covers flight_fare_quotes and the four kpi_* tables.
    DO \$sweep\$
    DECLARE
        target record;
    BEGIN
        FOR target IN
            SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        LOOP
            EXECUTE format(
                'ALTER TABLE public.%I OWNER TO %I',
                target.tablename,
                '${ANALYTICS_DB_USER}'
            );
        END LOOP;
    END
    \$sweep\$;
EOSQL
