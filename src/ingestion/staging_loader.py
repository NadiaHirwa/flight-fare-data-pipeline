"""CSV -> MySQL staging.raw_flights, plus the run's audit row.

Implements the load_to_mysql_staging task body (ADR-007: the DAG orchestrates,
this module holds the logic).

Three things this module used to own now live in src/shared/, because
validation and transformation need them too and should not import an ingestion
module to get them:

  connections.py    the engine (credentials never hardcoded, per the Security
                    section of docs/MASTER_PLAN.md)
  tables.py         STAGING_TABLE, PIPELINE_RUNS_TABLE, COLUMN_MAP
  pipeline_runs.py  opening the audit row, recording counts, marking failure

What remains here is what only ingestion does: read the CSV and land it.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..shared.connections import get_staging_engine
from ..shared.pipeline_runs import mark_run_failed, open_run, record_counts
from ..shared.tables import COLUMN_MAP, STAGING_TABLE
from .exceptions import StagingLoadError
from .schema import EXPECTED_COLUMNS

logger = logging.getLogger(__name__)

# Rows per executemany. Bounded so memory stays flat regardless of file size;
# 57,000 rows (docs/data_profile.md) is 12 batches.
INSERT_BATCH_SIZE = 5_000

# Read size for checksumming. The file is 13.49 MB today, but streaming it
# costs nothing extra and removes any assumption about that staying true.
CHECKSUM_CHUNK_BYTES = 1024 * 1024


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
        StagingLoadError: the CSV is structurally broken, or rows read does not
            equal rows landed.
        ConnectionConfigError: no usable MySQL credentials.
    """
    path = Path(source_path)
    engine = get_staging_engine()
    checksum = compute_file_checksum(path)
    logger.info("Source %s checksum sha256=%s", path.name, checksum)

    open_run(engine, pipeline_run_id, path, checksum)

    try:
        source_row_count = _truncate_and_load(engine, path, pipeline_run_id)
        staged_row_count = _count_staged_rows(engine, pipeline_run_id)
        record_counts(
            engine,
            pipeline_run_id,
            source_row_count=source_row_count,
            staged_row_count=staged_row_count,
        )

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
        mark_run_failed(engine, pipeline_run_id)
        raise

    logger.info(
        "Staged %d rows into %s for run %s.",
        staged_row_count,
        STAGING_TABLE,
        pipeline_run_id,
    )


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
