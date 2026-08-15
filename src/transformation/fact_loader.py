"""Valid staging rows -> PostgreSQL analytics.flight_fare_quotes.

Implements the transform_and_load_fact task body (ADR-007).

Reads across the two-database boundary described in docs/MASTER_PLAN.md's
Pipeline Architecture: valid rows come from MySQL staging and land in the
PostgreSQL serving layer. Nothing is persisted in between — MySQL holds no
second "validated" copy, so the valid set is the anti-join query already
defined in src/validation/quarantine.py rather than a table.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..shared.connections import get_analytics_engine, get_staging_engine
from ..shared.pipeline_runs import mark_run_failed, record_counts
from ..validation.quarantine import iter_valid_rows
from .converters import FACT_COLUMNS, convert_record
from .exceptions import FactLoadError

logger = logging.getLogger(__name__)

FACT_TABLE = "flight_fare_quotes"

# Rows per executemany into PostgreSQL. Bounded so memory stays flat: the
# 54,478 valid rows of the current file are read and written in pages.
INSERT_BATCH_SIZE = 5_000


def transform_and_load_fact(
    pipeline_run_id: str,
    staging_engine: Engine | None = None,
    analytics_engine: Engine | None = None,
) -> dict[str, Any]:
    """Convert this run's valid rows and load them into the fact table.

    Truncate-and-reload per ADR-001, then records loaded_row_count on
    staging.pipeline_runs so reconciliation_check can verify
    `valid_row_count = loaded_row_count`.

    The truncate and the inserts share one transaction. PostgreSQL's TRUNCATE
    is transactional, so a failure mid-load rolls back to the previous
    contents rather than leaving an empty fact table — the analytics layer is
    never observably empty partway through a run. (The MySQL staging load
    cannot do this: TRUNCATE implicitly commits there.) That also makes an
    Airflow retry safe: the retry truncates and reloads, and cannot
    double-insert.

    Args:
        pipeline_run_id: the run whose valid rows to load.
        staging_engine / analytics_engine: injectable for tests.

    Returns:
        A counts-only summary, safe for XCom:
        pipeline_run_id, fact_table, loaded_row_count.

    Raises:
        RecordConversionError: a contract-required value would not convert,
            meaning a row reached here that validation should have stopped.
        FactLoadError: rows converted does not equal rows landed.
    """
    staging_engine = staging_engine or get_staging_engine()
    analytics_engine = analytics_engine or get_analytics_engine()

    insert_statement = text(
        f"INSERT INTO {FACT_TABLE} ({', '.join(FACT_COLUMNS)}) "
        f"VALUES ({', '.join(':' + column for column in FACT_COLUMNS)})"
    )

    converted = 0
    batch: list[dict[str, Any]] = []

    try:
        with analytics_engine.begin() as conn:
            # ADR-001: full truncate-and-reload of the analytics layer on every
            # run. Not scoped to this run's rows because flight_fare_quotes has
            # no pipeline_run_id column to scope by — by design, since the table
            # only ever holds one run's output. See the note raised with this
            # module.
            conn.execute(text(f"TRUNCATE TABLE {FACT_TABLE}"))
            logger.info("Truncated %s ahead of run %s.", FACT_TABLE, pipeline_run_id)

            for record in iter_valid_rows(staging_engine, pipeline_run_id):
                batch.append(convert_record(record))
                converted += 1

                if len(batch) >= INSERT_BATCH_SIZE:
                    conn.execute(insert_statement, batch)
                    logger.info("Loaded %d rows so far...", converted)
                    batch = []

            if batch:
                conn.execute(insert_statement, batch)

        loaded = _count_fact_rows(analytics_engine)
        record_counts(staging_engine, pipeline_run_id, loaded_row_count=loaded)

        # Counted independently on each side on purpose: `converted` comes from
        # iterating staging, `loaded` from SELECT COUNT(*) on the fact table.
        # Deriving both from one number would make the reconciliation equation
        # true by construction and unable to detect the loss it exists to
        # detect. Recorded before the check so a mismatch is visible in the
        # audit table rather than only in the traceback.
        if loaded != converted:
            raise FactLoadError(
                f"Row count mismatch for run {pipeline_run_id}: converted "
                f"{converted} valid rows but {loaded} landed in {FACT_TABLE}."
            )
    except Exception:
        # Matches load_to_mysql_staging: a stage that raises closes the audit
        # row, so a crashed run cannot sit at 'running' forever and quietly
        # make the audit table lie. Marked on the staging engine because
        # pipeline_runs lives with staging, not with the analytics layer.
        mark_run_failed(staging_engine, pipeline_run_id)
        raise

    logger.info(
        "Loaded %d rows into %s for run %s.", loaded, FACT_TABLE, pipeline_run_id
    )
    return {
        "pipeline_run_id": pipeline_run_id,
        "fact_table": FACT_TABLE,
        "loaded_row_count": loaded,
    }


def _count_fact_rows(analytics_engine: Engine) -> int:
    with analytics_engine.begin() as conn:
        return int(
            conn.execute(text(f"SELECT COUNT(*) FROM {FACT_TABLE}")).scalar_one()
        )
