"""Writers for the staging.pipeline_runs audit row.

Every stage of the pipeline updates part of this one row:

  ingestion       opens it, then records source_row_count / staged_row_count
  validation      records valid_row_count / rejected_row_count
  transformation  records loaded_row_count
  any of them     marks it failed when they raise

That is what makes reconciliation a single query rather than a guess
(docs/MASTER_PLAN.md, "Pipeline Audit / Run Metadata"):

    source_row_count = valid_row_count + rejected_row_count
    valid_row_count  = loaded_row_count

Because all three stages write the same row, these helpers belong in the
shared leaf package. Keeping them in src/ingestion/ meant a later stage had to
import the ingestion module to mark a run failed, and — before this module
existed — transformation simply did not mark it, so a failed fact load left
the run sitting at 'running' and the audit table quietly lying.

The schema is in include/sql/staging/create_pipeline_runs_table.sql. Note this
table is never truncated: it accumulates across runs by design.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .tables import PIPELINE_RUNS_TABLE

logger = logging.getLogger(__name__)

# The five count columns, as an allowlist. record_counts interpolates column
# names into its SET clause — which is safe only because every name is checked
# against this set first. Values are always bound parameters.
COUNT_COLUMNS: frozenset[str] = frozenset(
    {
        "source_row_count",
        "staged_row_count",
        "valid_row_count",
        "rejected_row_count",
        "loaded_row_count",
    }
)


def open_run(
    engine: Engine, pipeline_run_id: str, source_file: Path | str, checksum: str
) -> None:
    """Upsert the run's audit row and reset it to a clean 'running' state.

    ON DUPLICATE KEY UPDATE rather than a plain INSERT because Airflow retries
    with the same run_id: a plain INSERT would fail on the primary key and the
    retry would die before doing any work. Resetting the counts to NULL matters
    too — a retry that inherited a previous attempt's counts would report
    figures for a load that no longer exists.

    Uses MySQL 8.0.19+ row-alias syntax rather than the deprecated VALUES().
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
            {
                "run_id": pipeline_run_id,
                "source_file": str(source_file),
                "checksum": checksum,
            },
        )
    logger.info("Opened pipeline run %s (status=running).", pipeline_run_id)


def record_counts(engine: Engine, pipeline_run_id: str, **counts: int) -> None:
    """Set one or more count columns on the run's audit row.

    One function rather than one per stage: the three callers differ only in
    which columns they set, and three near-identical UPDATE statements would
    drift. Called as, for example:

        record_counts(engine, run_id, valid_row_count=54478,
                      rejected_row_count=2522)

    Raises:
        ValueError: a column name outside COUNT_COLUMNS was passed. Checked
            rather than trusted because these names are interpolated into the
            SET clause; the allowlist is what keeps that safe.
    """
    if not counts:
        raise ValueError("record_counts requires at least one count to set.")

    unknown = sorted(set(counts) - COUNT_COLUMNS)
    if unknown:
        raise ValueError(
            f"Unknown pipeline_runs count column(s) {unknown}. "
            f"Valid columns: {sorted(COUNT_COLUMNS)}."
        )

    assignments = ", ".join(f"{column} = :{column}" for column in sorted(counts))
    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE {PIPELINE_RUNS_TABLE} SET {assignments} "
                "WHERE pipeline_run_id = :run_id"
            ),
            {**counts, "run_id": pipeline_run_id},
        )


def mark_run_succeeded(engine: Engine, pipeline_run_id: str) -> None:
    """Close the audit row as successful.

    Called by the DAG's last task, reconciliation_check, and only after it has
    proved the run's counts reconcile — no earlier stage has the standing to
    call a run successful, because none of them can see whether the stages
    after it will hold up.

    Without this the row stays at 'running' forever on a successful run, so
    every completed run reads as still in flight and the 'success' value in the
    status CHECK constraint is never used.

    Not wrapped in a try/except, unlike mark_run_failed: there is no original
    exception to protect here, and a failure to record success is a real
    failure of the audit trail that should surface.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {PIPELINE_RUNS_TABLE}
                   SET status = 'success', completed_at = NOW(6)
                 WHERE pipeline_run_id = :run_id
                """
            ),
            {"run_id": pipeline_run_id},
        )
    logger.info("Closed pipeline run %s (status=success).", pipeline_run_id)


def mark_run_failed(engine: Engine, pipeline_run_id: str) -> None:
    """Close the audit row as failed, without masking the original exception.

    Wrapped in its own try/except: if the database is what failed, this update
    will fail too, and letting that secondary error propagate would replace the
    real diagnosis with a confusing one.

    Note 'failed' is deliberately distinct from 'quality_gate_failed' — the
    latter means the data was abnormal, not that the pipeline malfunctioned.
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
