"""CSV -> MySQL staging.raw_flights, plus the run's audit row.

Implements the load_to_mysql_staging task body (ADR-007: the DAG orchestrates,
this module holds the logic).

Credentials are never hardcoded here, per the Security section of
docs/MASTER_PLAN.md. Connection details come from an Airflow Connection when
running inside Airflow, falling back to the environment variables already
defined in .env.example when running outside it (tests, a local script, a
`make` target). Neither path puts a password in source control.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine

from .exceptions import StagingLoadError
from .schema import EXPECTED_COLUMNS

logger = logging.getLogger(__name__)

STAGING_TABLE = "raw_flights"
PIPELINE_RUNS_TABLE = "pipeline_runs"

# Overridable so a deployment can name its Airflow Connection differently
# without a code change.
DEFAULT_STAGING_CONN_ID = "mysql_staging"

# Rows per executemany. Bounded so memory stays flat regardless of file size;
# 57,000 rows (docs/data_profile.md) is 12 batches.
INSERT_BATCH_SIZE = 5_000

# Read size for checksumming. The file is 13.49 MB today, but streaming it
# costs nothing extra and removes any assumption about that staying true.
CHECKSUM_CHUNK_BYTES = 1024 * 1024

# CSV header -> raw_flights column. The staging table snake_cases identifiers
# but mirrors the source column set exactly; this is the single place that
# mapping is written down (see the header comment in
# include/sql/staging/create_staging_table.sql).
COLUMN_MAP: dict[str, str] = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "fare_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_staging_engine() -> Engine:
    """Build a SQLAlchemy engine for the MySQL staging database.

    Prefers the Airflow Connection named by MYSQL_STAGING_CONN_ID (default
    "mysql_staging"), which is the mechanism docs/MASTER_PLAN.md specifies, so
    credentials live in Airflow's encrypted connection store rather than in
    the process environment. Falls back to the .env variables when Airflow is
    not importable or the connection is not defined — that is what lets this
    module be exercised outside a running Airflow.
    """
    conn_id = os.environ.get("MYSQL_STAGING_CONN_ID", DEFAULT_STAGING_CONN_ID)
    url = _url_from_airflow_connection(conn_id) or _url_from_environment()
    # pool_pre_ping: the DAG can sit between tasks long enough for MySQL to
    # drop an idle connection; without this the next task fails on a stale
    # socket and burns a retry on a non-problem.
    return create_engine(url, pool_pre_ping=True)


def _url_from_airflow_connection(conn_id: str) -> URL | None:
    """Return a URL from the named Airflow Connection, or None if unavailable."""
    try:
        from airflow.hooks.base import BaseHook
    except ImportError:
        logger.info("Airflow not importable; using environment variables for MySQL.")
        return None

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:  # noqa: BLE001 - Airflow raises different types by version
        logger.info(
            "Airflow connection %r not found; using environment variables for MySQL.",
            conn_id,
        )
        return None

    logger.info("Using Airflow connection %r for MySQL staging.", conn_id)
    return URL.create(
        "mysql+pymysql",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port or 3306,
        database=conn.schema,
    )


def _url_from_environment() -> URL:
    """Return a URL built from the .env variables.

    Names match .env.example exactly, so there is one vocabulary for these
    credentials across Compose, the Makefile, and this module.
    """
    missing = [
        name
        for name in ("MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE")
        if not os.environ.get(name)
    ]
    if missing:
        raise StagingLoadError(
            f"No MySQL credentials available: Airflow connection unusable and "
            f"environment variables {missing} are unset. See .env.example."
        )

    # URL.create rather than an f-string: it escapes the password, so a
    # password containing '@', '/' or ':' cannot silently corrupt the URL.
    return URL.create(
        "mysql+pymysql",
        username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        host=os.environ.get("MYSQL_HOST", "mysql"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        database=os.environ["MYSQL_DATABASE"],
    )


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def compute_file_checksum(path: Path) -> str:
    """SHA-256 of the file's bytes, streamed.

    Recorded on the run row so "the exact same file was submitted twice" is
    detectable without building a separate subsystem for it
    (docs/MASTER_PLAN.md, "File-level protection").
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHECKSUM_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_to_mysql_staging(source_path: str, pipeline_run_id: str) -> None:
    """Truncate staging.raw_flights and bulk-load the source CSV into it.

    Also opens the run's audit row in staging.pipeline_runs (status 'running')
    and records source_row_count / staged_row_count once the load completes.

    The run row is left at 'running' on success: status describes the whole
    pipeline run, not this task, and it is the end of the DAG that has the
    standing to call a run successful. Only failure is recorded here, so a
    crashed run does not sit at 'running' forever and quietly make the audit
    table lie.

    Idempotent by construction (docs/MASTER_PLAN.md, "Retries must be safe"):
    the audit row is upserted and the counts reset, the table is truncated
    before insert, and source_row_number is derived from the file's own order
    rather than from an auto-increment. Re-running produces the same end state.

    Raises:
        StagingLoadError: credentials unavailable, the CSV is structurally
            broken, or rows read does not equal rows landed.
    """
    path = Path(source_path)
    engine = get_staging_engine()
    checksum = compute_file_checksum(path)
    logger.info("Source %s checksum sha256=%s", path.name, checksum)

    _open_pipeline_run(engine, pipeline_run_id, path, checksum)

    try:
        source_row_count = _truncate_and_load(engine, path, pipeline_run_id)
        staged_row_count = _count_staged_rows(engine, pipeline_run_id)
        _record_counts(engine, pipeline_run_id, source_row_count, staged_row_count)

        # Counted independently on each side on purpose: source_row_count comes
        # from reading the file, staged_row_count from SELECT COUNT(*). Setting
        # both from the same Python variable would make the reconciliation
        # equation in docs/MASTER_PLAN.md true by construction and therefore
        # incapable of ever detecting the data loss it exists to detect.
        if staged_row_count != source_row_count:
            raise StagingLoadError(
                f"Row count mismatch for run {pipeline_run_id}: read "
                f"{source_row_count} rows from {path.name} but "
                f"{staged_row_count} landed in {STAGING_TABLE}."
            )
    except Exception:
        _mark_run_failed(engine, pipeline_run_id)
        raise

    logger.info(
        "Staged %d rows into %s for run %s.",
        staged_row_count,
        STAGING_TABLE,
        pipeline_run_id,
    )


def _open_pipeline_run(
    engine: Engine, pipeline_run_id: str, path: Path, checksum: str
) -> None:
    """Upsert the run's audit row and reset it to a clean 'running' state.

    ON DUPLICATE KEY UPDATE rather than a plain INSERT because Airflow retries
    this task with the same run_id: a plain INSERT would fail on the primary
    key, and the retry would die before doing any work. Resetting the counts to
    NULL matters too — a retry that inherited a previous attempt's counts would
    report figures for a load that no longer exists.
    """
    statement = text(
        f"""
        INSERT INTO {PIPELINE_RUNS_TABLE} (
            pipeline_run_id, source_file, source_file_checksum, started_at, status
        )
        VALUES (:run_id, :source_file, :checksum, NOW(6), 'running') AS new
        ON DUPLICATE KEY UPDATE
            source_file          = new.source_file,
            source_file_checksum = new.source_file_checksum,
            started_at           = NOW(6),
            completed_at         = NULL,
            source_row_count     = NULL,
            staged_row_count     = NULL,
            valid_row_count      = NULL,
            rejected_row_count   = NULL,
            loaded_row_count     = NULL,
            status               = 'running'
        """
    )
    with engine.begin() as conn:
        conn.execute(
            statement,
            {"run_id": pipeline_run_id, "source_file": str(path), "checksum": checksum},
        )
    logger.info("Opened pipeline run %s (status=running).", pipeline_run_id)


def _truncate_and_load(engine: Engine, path: Path, pipeline_run_id: str) -> int:
    """Truncate raw_flights, stream the CSV in, return rows read."""
    columns = ["source_row_number", "pipeline_run_id"] + [
        COLUMN_MAP[name] for name in EXPECTED_COLUMNS
    ]
    # The column names interpolated here come from module-level constants, not
    # from input — every *value* below is a bound parameter, per the
    # parameterized-queries rule in docs/MASTER_PLAN.md's Security section.
    insert_statement = text(
        f"INSERT INTO {STAGING_TABLE} ({', '.join(columns)}) "
        f"VALUES ({', '.join(':' + column for column in columns)})"
    )

    with engine.begin() as conn:
        # TRUNCATE is DDL in MySQL and implicitly commits, so this cannot be
        # rolled back with the inserts below. That is acceptable precisely
        # because of ADR-001: the recovery for a half-finished load is to run
        # the task again, which truncates and reloads from scratch.
        conn.execute(text(f"TRUNCATE TABLE {STAGING_TABLE}"))

    rows_read = 0
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        try:
            header = [column.strip() for column in next(reader)]
        except StopIteration:
            raise StagingLoadError(f"Source file is empty: {path}") from None

        index_of = _build_column_index(header, path)

        with engine.begin() as conn:
            batch: list[dict[str, object]] = []
            for row in reader:
                if not row:
                    continue
                rows_read += 1

                if len(row) != len(header):
                    # Structural, so Level 1: fail fast. Loading it anyway
                    # would shift every field after the break, writing values
                    # into the wrong columns instead of reporting a broken file.
                    raise StagingLoadError(
                        f"Row at line {reader.line_num} has {len(row)} fields, "
                        f"expected {len(header)}: {path}"
                    )

                params: dict[str, object] = {
                    "source_row_number": rows_read,
                    "pipeline_run_id": pipeline_run_id,
                }
                for csv_column, db_column in COLUMN_MAP.items():
                    params[db_column] = row[index_of[csv_column]]
                batch.append(params)

                if len(batch) >= INSERT_BATCH_SIZE:
                    conn.execute(insert_statement, batch)
                    logger.info("Staged %d rows so far...", rows_read)
                    batch = []

            if batch:
                conn.execute(insert_statement, batch)

    return rows_read


def _build_column_index(header: list[str], path: Path) -> dict[str, int]:
    """Map each expected CSV column to its position in this file's header.

    Built from the actual header rather than assumed, which is what makes the
    load tolerant of column order in the same way validate_source_schema is.
    Re-checked here rather than trusted because Airflow can run this task on
    its own (a cleared task, a partial re-run) without the validation task
    having run first in that attempt.
    """
    index_of = {name: position for position, name in enumerate(header)}
    missing = [name for name in COLUMN_MAP if name not in index_of]
    if missing:
        raise StagingLoadError(
            f"Source file is missing expected columns {missing}: {path}. "
            "Run validate_source_schema first."
        )
    return index_of


def _count_staged_rows(engine: Engine, pipeline_run_id: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {STAGING_TABLE} WHERE pipeline_run_id = :run_id"
            ),
            {"run_id": pipeline_run_id},
        )
        return int(result.scalar_one())


def _record_counts(
    engine: Engine, pipeline_run_id: str, source_row_count: int, staged_row_count: int
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {PIPELINE_RUNS_TABLE}
                   SET source_row_count = :source_row_count,
                       staged_row_count = :staged_row_count
                 WHERE pipeline_run_id = :run_id
                """
            ),
            {
                "source_row_count": source_row_count,
                "staged_row_count": staged_row_count,
                "run_id": pipeline_run_id,
            },
        )


def _mark_run_failed(engine: Engine, pipeline_run_id: str) -> None:
    """Close the audit row as failed, without masking the original exception.

    Wrapped in its own try/except: if the database is what failed, this update
    will fail too, and letting that secondary error propagate would replace the
    real diagnosis with a confusing one.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    UPDATE {PIPELINE_RUNS_TABLE}
                       SET status = 'failed', completed_at = NOW(6)
                     WHERE pipeline_run_id = :run_id
                    """
                ),
                {"run_id": pipeline_run_id},
            )
    except Exception:
        logger.exception("Could not mark run %s as failed.", pipeline_run_id)
