"""Source-to-target reconciliation against the run's audit row.

Implements the reconciliation_check task body (ADR-007). Reads both databases:
the audit row lives in MySQL staging, and the fact table it claims to describe
lives in PostgreSQL.

docs/MASTER_PLAN.md ("Pipeline Audit / Run Metadata") states the equations:

    source_row_count = valid_row_count + rejected_row_count   (must hold)
    valid_row_count  = loaded_row_count                        (must hold,
                                                    given truncate-and-reload)

and why they matter: "If these equations don't hold on any run, that's a
detectable data-loss signal."

A third check is added here for a reason the equations alone cannot cover.
Both of them are computed from numbers the pipeline wrote about itself, so a
stage that miscounted would satisfy them while still having lost rows. Reading
COUNT(*) from the fact table and comparing it to the recorded
loaded_row_count is what makes the audit row accountable to reality rather
than only to itself.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..shared.connections import get_analytics_engine, get_staging_engine
from ..shared.tables import PIPELINE_RUNS_TABLE

logger = logging.getLogger(__name__)

# Stated here rather than imported from src/transformation/, so a downstream
# checker does not depend on the stage it audits. A test pins it to the
# authoritative definition.
FACT_TABLE = "flight_fare_quotes"


class ReconciliationError(Exception):
    """A reconciliation equation did not hold, or the audit row is unusable.

    Carries the actual numbers, not just the fact of failure: "source 57000 !=
    54478 + 2000" tells you where the rows went; "reconciliation failed" does
    not.
    """


@dataclass(frozen=True)
class RunCounts:
    """The audit row's counts. Any may be None if its stage never ran."""

    pipeline_run_id: str
    source_row_count: int | None
    staged_row_count: int | None
    valid_row_count: int | None
    rejected_row_count: int | None
    loaded_row_count: int | None


def reconciliation_check(
    pipeline_run_id: str,
    staging_engine: Engine | None = None,
    analytics_engine: Engine | None = None,
) -> dict[str, Any]:
    """Verify the run's counts reconcile, end to end.

    Args:
        pipeline_run_id: the run to check.
        staging_engine / analytics_engine: injectable for tests.

    Returns:
        A counts-only summary, safe for XCom.

    Raises:
        ReconciliationError: the audit row is missing, a required count was
            never recorded, or an equation did not hold. The message names the
            actual numbers and the size of the discrepancy.
    """
    staging_engine = staging_engine or get_staging_engine()
    analytics_engine = analytics_engine or get_analytics_engine()

    counts = fetch_run_counts(staging_engine, pipeline_run_id)
    fact_row_count = fetch_fact_row_count(analytics_engine)
    failures = evaluate_reconciliation(counts, fact_row_count)

    if failures:
        message = (
            f"Reconciliation failed for run {pipeline_run_id} "
            f"({len(failures)} problem(s)): " + " | ".join(failures)
        )
        logger.error(message)
        raise ReconciliationError(message)

    logger.info(
        "Reconciliation passed for run %s: source %d = valid %d + rejected %d, "
        "valid = loaded = %d fact rows.",
        pipeline_run_id,
        counts.source_row_count,
        counts.valid_row_count,
        counts.rejected_row_count,
        fact_row_count,
    )
    return {
        "pipeline_run_id": pipeline_run_id,
        "source_row_count": counts.source_row_count,
        "valid_row_count": counts.valid_row_count,
        "rejected_row_count": counts.rejected_row_count,
        "loaded_row_count": counts.loaded_row_count,
        "fact_row_count": fact_row_count,
    }


def evaluate_reconciliation(counts: RunCounts, fact_row_count: int) -> list[str]:
    """Check both equations plus the fact-table cross-check.

    Pure: no database, no I/O. Returns an empty list when everything holds.
    """
    missing = [
        name
        for name in (
            "source_row_count",
            "valid_row_count",
            "rejected_row_count",
            "loaded_row_count",
        )
        if getattr(counts, name) is None
    ]
    if missing:
        # Stop here rather than comparing against None. A NULL count means the
        # stage that sets it never completed, which is a more fundamental
        # finding than any equation failing — and arithmetic on None would
        # raise a TypeError that says nothing useful.
        return [
            (
                f"audit row has no {', '.join(missing)} — the stage(s) that "
                "record these counts did not complete, so the run cannot be "
                "reconciled"
            )
        ]

    failures: list[str] = []

    accounted = counts.valid_row_count + counts.rejected_row_count
    if counts.source_row_count != accounted:
        difference = counts.source_row_count - accounted
        failures.append(
            f"source_row_count {counts.source_row_count} != valid_row_count "
            f"{counts.valid_row_count} + rejected_row_count "
            f"{counts.rejected_row_count} ({accounted}); "
            f"{abs(difference)} row(s) "
            f"{'unaccounted for' if difference > 0 else 'counted twice'}"
        )

    if counts.valid_row_count != counts.loaded_row_count:
        difference = counts.valid_row_count - counts.loaded_row_count
        failures.append(
            f"valid_row_count {counts.valid_row_count} != loaded_row_count "
            f"{counts.loaded_row_count}; "
            f"{abs(difference)} valid row(s) "
            f"{'never loaded' if difference > 0 else 'loaded in excess'}"
        )

    if counts.loaded_row_count != fact_row_count:
        difference = counts.loaded_row_count - fact_row_count
        failures.append(
            f"loaded_row_count {counts.loaded_row_count} does not match the "
            f"{fact_row_count} row(s) actually in {FACT_TABLE} "
            f"(off by {abs(difference)}) — the audit row and the served data "
            "disagree"
        )

    return failures


def fetch_run_counts(staging_engine: Engine, pipeline_run_id: str) -> RunCounts:
    """Read the run's audit row.

    Raises:
        ReconciliationError: no such run. Distinguished from a failing equation
            because it means the run never started, not that it lost rows.
    """
    with staging_engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT source_row_count, staged_row_count, valid_row_count,
                       rejected_row_count, loaded_row_count
                  FROM {PIPELINE_RUNS_TABLE}
                 WHERE pipeline_run_id = :run_id
                """
            ),
            {"run_id": pipeline_run_id},
        ).one_or_none()

    if row is None:
        raise ReconciliationError(
            f"No audit row in {PIPELINE_RUNS_TABLE} for run "
            f"{pipeline_run_id!r}. The run never opened, so there is nothing "
            "to reconcile."
        )

    return RunCounts(
        pipeline_run_id=pipeline_run_id,
        source_row_count=row[0],
        staged_row_count=row[1],
        valid_row_count=row[2],
        rejected_row_count=row[3],
        loaded_row_count=row[4],
    )


def fetch_fact_row_count(analytics_engine: Engine) -> int:
    with analytics_engine.begin() as conn:
        return int(
            conn.execute(text(f"SELECT COUNT(*) FROM {FACT_TABLE}")).scalar_one()
        )
