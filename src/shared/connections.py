"""Database engines for the two layers, built without hardcoded credentials.

Shared because both databases are reached from more than one stage:

  MySQL staging      src/ingestion/ writes it, src/validation/ reads it,
                     src/transformation/ reads valid rows and updates the
                     run's audit row.
  PostgreSQL analytics  src/transformation/ writes the fact table,
                     src/kpi/ and src/quality/ read it.

Keeping these here rather than in whichever stage happened to need one first
is what stops later stages importing an earlier stage purely to borrow its
connection helper — src/validation/ previously reached into
src/ingestion/staging_loader.py for exactly that reason.

Per the Security section of docs/MASTER_PLAN.md, credentials never appear in
source. Both engines resolve an Airflow Connection first, so secrets live in
Airflow's encrypted connection store, and fall back to the environment
variables already defined in .env.example when Airflow is not present (tests,
a local script, a `make` target). Neither path puts a password in git.
"""

from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

logger = logging.getLogger(__name__)

# Overridable so a deployment can name its Airflow Connections differently
# without a code change.
DEFAULT_STAGING_CONN_ID = "mysql_staging"
DEFAULT_ANALYTICS_CONN_ID = "postgres_analytics"

MYSQL_DRIVER = "mysql+pymysql"
POSTGRES_DRIVER = "postgresql+psycopg2"

DEFAULT_MYSQL_PORT = 3306
DEFAULT_POSTGRES_PORT = 5432


class ConnectionConfigError(Exception):
    """No usable credentials for a database.

    Defined here rather than imported from a stage's exception module: this
    package is a leaf and may not import from ingestion, validation,
    transformation, loading, kpi, or quality.
    """


def get_staging_engine() -> Engine:
    """Engine for the MySQL staging database (raw_flights, quarantine,
    pipeline_runs)."""
    return _build_engine(
        driver=MYSQL_DRIVER,
        conn_id=os.environ.get("MYSQL_STAGING_CONN_ID", DEFAULT_STAGING_CONN_ID),
        env_user="MYSQL_USER",
        env_password="MYSQL_PASSWORD",
        env_database="MYSQL_DATABASE",
        env_host="MYSQL_HOST",
        env_port="MYSQL_PORT",
        default_host="mysql",
        default_port=DEFAULT_MYSQL_PORT,
        label="MySQL staging",
    )


def get_analytics_engine() -> Engine:
    """Engine for the PostgreSQL analytics database (flight_fare_quotes, kpi_*).

    Uses the least-privilege ANALYTICS_DB_USER from .env.example, never the
    postgres superuser — see postgres-init/01-init-analytics-db.sh and the
    Security section of docs/MASTER_PLAN.md. Note the analytics database is a
    separate database on the same instance as Airflow's own metadata database
    (ADR-002), so the database name matters here, not just the host.
    """
    return _build_engine(
        driver=POSTGRES_DRIVER,
        conn_id=os.environ.get("POSTGRES_ANALYTICS_CONN_ID", DEFAULT_ANALYTICS_CONN_ID),
        env_user="ANALYTICS_DB_USER",
        env_password="ANALYTICS_DB_PASSWORD",
        env_database="ANALYTICS_DB_NAME",
        env_host="POSTGRES_HOST",
        env_port="POSTGRES_PORT",
        default_host="postgres",
        default_port=DEFAULT_POSTGRES_PORT,
        label="PostgreSQL analytics",
    )


def _build_engine(
    driver: str,
    conn_id: str,
    env_user: str,
    env_password: str,
    env_database: str,
    env_host: str,
    env_port: str,
    default_host: str,
    default_port: int,
    label: str,
) -> Engine:
    url = _url_from_airflow_connection(driver, conn_id, default_port, label) or (
        _url_from_environment(
            driver=driver,
            env_user=env_user,
            env_password=env_password,
            env_database=env_database,
            env_host=env_host,
            env_port=env_port,
            default_host=default_host,
            default_port=default_port,
            label=label,
        )
    )
    # pool_pre_ping: the DAG can sit between tasks long enough for a database
    # to drop an idle connection; without this the next task fails on a stale
    # socket and burns a retry on a non-problem.
    return create_engine(url, pool_pre_ping=True)


def _url_from_airflow_connection(
    driver: str, conn_id: str, default_port: int, label: str
) -> URL | None:
    """Return a URL from the named Airflow Connection, or None if unavailable."""
    # Airflow 3 moved BaseHook to the task SDK; airflow.hooks.base still works
    # but emits a DeprecationWarning on every call. Try the current location
    # first and fall back, so this works on 3.x without warning noise and still
    # works if pointed at an older Airflow.
    try:
        from airflow.sdk.bases.hook import BaseHook
    except ImportError:
        try:
            from airflow.hooks.base import BaseHook
        except ImportError:
            logger.info(
                "Airflow not importable; using environment variables for %s.", label
            )
            return None

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:  # noqa: BLE001 - Airflow raises different types by version
        logger.info(
            "Airflow connection %r not found; using environment variables for %s.",
            conn_id,
            label,
        )
        return None

    logger.info("Using Airflow connection %r for %s.", conn_id, label)
    return URL.create(
        driver,
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port or default_port,
        database=conn.schema,
    )


def _url_from_environment(
    driver: str,
    env_user: str,
    env_password: str,
    env_database: str,
    env_host: str,
    env_port: str,
    default_host: str,
    default_port: int,
    label: str,
) -> URL:
    """Return a URL built from the .env variables.

    Names match .env.example exactly, so there is one vocabulary for these
    credentials across Compose, the Makefile, and this module.
    """
    missing = [
        name
        for name in (env_user, env_password, env_database)
        if not os.environ.get(name)
    ]
    if missing:
        raise ConnectionConfigError(
            f"No credentials available for {label}: Airflow connection unusable "
            f"and environment variables {missing} are unset. See .env.example."
        )

    # URL.create rather than an f-string: it escapes the password, so a
    # password containing '@', '/' or ':' cannot silently corrupt the URL.
    return URL.create(
        driver,
        username=os.environ[env_user],
        password=os.environ[env_password],
        host=os.environ.get(env_host, default_host),
        port=int(os.environ.get(env_port, str(default_port))),
        database=os.environ[env_database],
    )
