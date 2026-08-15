"""Row-level validation pass: read staging, quarantine failures, report counts.

Implements the validate_and_quarantine task body (ADR-007).

Rejected rows are never dropped (ADR-003): each violation becomes one row in
staging.quarantine, carrying the original record, its exact source position,
and the rule that rejected it.

Valid rows are NOT copied anywhere. docs/MASTER_PLAN.md is explicit that MySQL
"does not hold a second 'validated' copy — valid rows pass directly into the
transformation step rather than being persisted twice". So "valid" is defined
here as a query, not a table: a staged row for this run with no quarantine row
against it. See SELECT_VALID_ROWS_SQL / iter_valid_rows, which is what
transform_and_load_fact consumes.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..ingestion.staging_loader import (
    COLUMN_MAP,
    PIPELINE_RUNS_TABLE,
    STAGING_TABLE,
    get_staging_engine,
)
from ..shared.normalization import compute_record_hash
from .rules import Violation, validate_batch

logger = logging.getLogger(__name__)

QUARANTINE_TABLE = "quarantine"

# Rows pulled from raw_flights per round trip, and quarantine rows written per
# executemany. Bounded so memory stays flat: the 57,000-row file is read in
# pages rather than materialized at once.
READ_CHUNK_SIZE = 5_000
WRITE_BATCH_SIZE = 1_000

# The 17 source columns, in file order. These and only these go into
# quarantine.original_record — pipeline metadata (source_row_number,
# pipeline_run_id, ingested_at) is recorded in its own columns, and repeating
# it inside the JSON would make "what the source said" ambiguous.
SOURCE_COLUMNS: tuple[str, ...] = tuple(COLUMN_MAP.values())

# A staged row for this run with nothing quarantined against it. This is the
# canonical definition of "valid" — transform_and_load_fact must use this
# rather than reimplementing the anti-join, so the two can never disagree.
#
# Joined on source_row_number, not source_record_hash: a row failing two rules
# has two quarantine rows, and NOT EXISTS over the row's position is exact
# regardless of how many violations it accumulated.
SELECT_VALID_ROWS_SQL = f"""
    SELECT r.*
      FROM {STAGING_TABLE} r
     WHERE r.pipeline_run_id = :run_id
       AND NOT EXISTS (
           SELECT 1
             FROM {QUARANTINE_TABLE} q
            WHERE q.pipeline_run_id = r.pipeline_run_id
              AND q.source_row_number = r.source_row_number
       )
"""


def validate_and_quarantine(
    pipeline_run_id: str, engine: Engine | None = None
) -> dict[str, Any]:
    """Apply Levels 2 and 3 to every staged row for this run.

    Writes one quarantine row per violation and records the run's valid and
    rejected counts on staging.pipeline_runs.

    Idempotent (docs/MASTER_PLAN.md, "Retries must be safe by construction"):
    this run's quarantine rows are cleared before the pass begins, so an
    Airflow retry replaces its previous output instead of doubling it.

    Args:
        pipeline_run_id: the run whose staged rows to validate.
        engine: injectable for tests; defaults to the configured staging engine.

    Returns:
        A counts-only summary, safe for XCom per MASTER_PLAN's XCom discipline
        (no row data ever travels through it):

            pipeline_run_id, staged_row_count, valid_row_count,
            rejected_row_count, violation_count, rejection_rate

        rejected_row_count counts *rows*, violation_count counts *violations* —
        they differ whenever a row breaks more than one rule, and it is the row
        count that the ADR-005 gate is defined against.
    """
    engine = engine or get_staging_engine()

    _clear_previous_quarantine(engine, pipeline_run_id)

    rejected_rows = 0
    violation_count = 0
    staged_rows = 0
    batch: list[dict[str, Any]] = []

    with engine.begin() as conn:
        # validate_batch drives the batch-level duplicate check across the
        # stream; _iter_staged_rows supplies rows in ascending
        # source_row_number order, which is what makes "keep the first
        # occurrence" mean "keep the lowest row number".
        for record, violations in validate_batch(
            _iter_staged_rows(engine, pipeline_run_id)
        ):
            staged_rows += 1
            if not violations:
                continue

            rejected_rows += 1
            violation_count += len(violations)
            batch.extend(_build_quarantine_rows(record, violations, pipeline_run_id))

            if len(batch) >= WRITE_BATCH_SIZE:
                _insert_quarantine_rows(conn, batch)
                batch = []

        if batch:
            _insert_quarantine_rows(conn, batch)

    valid_rows = staged_rows - rejected_rows
    # Guard the zero case explicitly: an empty staging table is a real failure
    # mode, and 0/0 should not become a ZeroDivisionError inside a summary.
    rejection_rate = round(rejected_rows / staged_rows, 6) if staged_rows else 0.0

    _record_counts(engine, pipeline_run_id, valid_rows, rejected_rows)

    logger.info(
        "Validation for run %s: %d staged, %d valid, %d rejected "
        "(%d violations), rejection rate %.4f%%",
        pipeline_run_id,
        staged_rows,
        valid_rows,
        rejected_rows,
        violation_count,
        rejection_rate * 100,
    )

    return {
        "pipeline_run_id": pipeline_run_id,
        "staged_row_count": staged_rows,
        "valid_row_count": valid_rows,
        "rejected_row_count": rejected_rows,
        "violation_count": violation_count,
        "rejection_rate": rejection_rate,
    }


def _build_quarantine_rows(
    record: Mapping[str, Any], violations: list[Violation], pipeline_run_id: str
) -> list[dict[str, Any]]:
    """Turn one rejected row's violations into quarantine row parameters.

    original_record holds the row exactly as staged — un-normalized, so the
    stored evidence is what the source said rather than what validation made
    of it. source_record_hash is the same digest the fact table will carry, so
    a rejected row can be joined against analytics.flight_fare_quotes to prove
    it never landed.
    """
    original_record = json.dumps(
        {column: record.get(column) for column in SOURCE_COLUMNS},
        ensure_ascii=False,
        sort_keys=True,
    )
    record_hash = compute_record_hash(record)

    return [
        {
            "original_record": original_record,
            "source_row_number": record["source_row_number"],
            "source_record_hash": record_hash,
            "rejection_reason": violation.reason,
            "validation_level": violation.level,
            "rule_violated": violation.rule,
            "pipeline_run_id": pipeline_run_id,
        }
        for violation in violations
    ]


def _insert_quarantine_rows(conn, rows: list[dict[str, Any]]) -> None:
    """Insert a batch. ingested_at is left to its column DEFAULT."""
    conn.execute(
        text(
            f"""
            INSERT INTO {QUARANTINE_TABLE} (
                original_record, source_row_number, source_record_hash,
                rejection_reason, validation_level, rule_violated, pipeline_run_id
            ) VALUES (
                :original_record, :source_row_number, :source_record_hash,
                :rejection_reason, :validation_level, :rule_violated, :pipeline_run_id
            )
            """
        ),
        rows,
    )


def _clear_previous_quarantine(engine: Engine, pipeline_run_id: str) -> None:
    """Remove this run's quarantine rows so a retry replaces rather than adds.

    Scoped to the run: quarantine accumulates across runs by design, and
    truncating it would destroy the record of every previous run's rejections.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(
                f"DELETE FROM {QUARANTINE_TABLE} WHERE pipeline_run_id = :run_id"
            ),
            {"run_id": pipeline_run_id},
        )
    if result.rowcount:
        logger.info(
            "Cleared %d quarantine rows from a previous attempt of run %s.",
            result.rowcount,
            pipeline_run_id,
        )


def _iter_staged_rows(
    engine: Engine, pipeline_run_id: str
) -> Iterator[dict[str, Any]]:
    """Stream staged rows in source order, paged by source_row_number.

    Keyset pagination rather than LIMIT/OFFSET: OFFSET rescans and discards
    every earlier row on each page, so paging a large table that way degrades
    quadratically. Ordering by source_row_number also means quarantine rows are
    written in source order, which makes the table readable next to the CSV.
    """
    last_row_number = 0
    while True:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT *
                      FROM {STAGING_TABLE}
                     WHERE pipeline_run_id = :run_id
                       AND source_row_number > :last_row_number
                     ORDER BY source_row_number
                     LIMIT :chunk_size
                    """
                ),
                {
                    "run_id": pipeline_run_id,
                    "last_row_number": last_row_number,
                    "chunk_size": READ_CHUNK_SIZE,
                },
            ).mappings().all()

        if not rows:
            return

        for row in rows:
            yield dict(row)
        last_row_number = rows[-1]["source_row_number"]


def _record_counts(
    engine: Engine, pipeline_run_id: str, valid_rows: int, rejected_rows: int
) -> None:
    """Record the counts the reconciliation equations are checked against.

    source_row_count = valid_row_count + rejected_row_count is verified later
    by reconciliation_check (docs/MASTER_PLAN.md); this task supplies the two
    right-hand terms.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                UPDATE {PIPELINE_RUNS_TABLE}
                   SET valid_row_count = :valid_rows,
                       rejected_row_count = :rejected_rows
                 WHERE pipeline_run_id = :run_id
                """
            ),
            {
                "valid_rows": valid_rows,
                "rejected_rows": rejected_rows,
                "run_id": pipeline_run_id,
            },
        )


# ---------------------------------------------------------------------------
# Valid-row access for the next task
# ---------------------------------------------------------------------------

def count_valid_rows(engine: Engine, pipeline_run_id: str) -> int:
    """Count rows that passed every rule, computed from the same predicate."""
    with engine.begin() as conn:
        return int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM ({SELECT_VALID_ROWS_SQL}) AS valid_rows"),
                {"run_id": pipeline_run_id},
            ).scalar_one()
        )


def iter_valid_rows(
    engine: Engine, pipeline_run_id: str, chunk_size: int = READ_CHUNK_SIZE
) -> Iterator[dict[str, Any]]:
    """Yield the run's valid staged rows, for transform_and_load_fact.

    The valid set is derived, not stored — no second staging table, per
    docs/MASTER_PLAN.md. Paged the same way as the validation read so memory
    stays flat here too.
    """
    last_row_number = 0
    paged_sql = text(
        f"""
        SELECT * FROM ({SELECT_VALID_ROWS_SQL}) AS valid_rows
         WHERE valid_rows.source_row_number > :last_row_number
         ORDER BY valid_rows.source_row_number
         LIMIT :chunk_size
        """
    )
    while True:
        with engine.begin() as conn:
            rows = conn.execute(
                paged_sql,
                {
                    "run_id": pipeline_run_id,
                    "last_row_number": last_row_number,
                    "chunk_size": chunk_size,
                },
            ).mappings().all()

        if not rows:
            return

        for row in rows:
            yield dict(row)
        last_row_number = rows[-1]["source_row_number"]
