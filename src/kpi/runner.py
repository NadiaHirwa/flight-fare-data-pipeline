"""Execute the four KPI SQL scripts against the analytics database.

Implements the four compute_kpi_* task bodies (ADR-007: the DAG orchestrates,
this module holds the logic).

There is deliberately no SQL here. The KPI logic is already written, reviewed
and proven in include/sql/analytics/, exactly as docs/kpi_definitions.md says
("Exact SQL lives in include/sql/analytics/"). Re-expressing any of it in
Python would create a second definition of each KPI that could drift from the
first. This module's whole job is to run the right file and report how many
rows it wrote.

This is the ELT-shaped half of the architecture (ADR-009): the fact table is
loaded first, then aggregated in SQL against already-landed data. Plain SQL
scripts rather than dbt models — ADR-006.

Each script is idempotent on its own (CREATE IF NOT EXISTS + TRUNCATE +
INSERT), so an Airflow retry re-runs it to the same end state.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..shared.connections import get_analytics_engine
from .exceptions import KpiError

logger = logging.getLogger(__name__)

# <repo>/src/kpi/runner.py -> <repo>/include/sql/analytics
# Holds in the container too: src/ mounts at /opt/airflow/src and include/ at
# /opt/airflow/include, so the same relative walk resolves correctly.
SQL_DIR = Path(__file__).resolve().parents[2] / "include" / "sql" / "analytics"

# Each KPI's table. The script filename is the table name plus ".sql" — the
# convention docs/kpi_definitions.md and include/sql/analytics/ already follow.
# A rename that breaks the convention fails loudly on a missing file rather
# than silently computing the wrong KPI.
KPI_TABLES: tuple[str, ...] = (
    "kpi_avg_fare_by_airline",
    "kpi_flight_offer_count_by_airline",
    "kpi_top_routes",
    "kpi_seasonal_fare_variation",
)


def compute_avg_fare_by_airline(engine: Engine | None = None) -> dict[str, Any]:
    """KPI 1 — AVG(total_fare) GROUP BY airline."""
    return _run_kpi_script("kpi_avg_fare_by_airline", engine)


def compute_flight_offer_count_by_airline(
    engine: Engine | None = None,
) -> dict[str, Any]:
    """KPI 2 — COUNT(*) GROUP BY airline.

    Named "flight offer count", not "booking count": the source has no booking
    entity (ADR-011).
    """
    return _run_kpi_script("kpi_flight_offer_count_by_airline", engine)


def compute_top_routes(engine: Engine | None = None) -> dict[str, Any]:
    """KPI 3 — COUNT(*) GROUP BY route, ordered descending.

    Writes every route, not a top-N slice: the reconciliation check requires
    SUM(counts) to equal the fact row count. See the script's own comment.
    """
    return _run_kpi_script("kpi_top_routes", engine)


def compute_seasonal_fare_variation(engine: Engine | None = None) -> dict[str, Any]:
    """KPI 4 — AVG(total_fare) GROUP BY seasonality."""
    return _run_kpi_script("kpi_seasonal_fare_variation", engine)


def _run_kpi_script(table: str, engine: Engine | None = None) -> dict[str, Any]:
    """Execute one KPI script and report the rows it wrote.

    Args:
        table: the KPI table, which is also the script's filename stem.
        engine: injectable for tests; defaults to the analytics engine.

    Returns:
        A counts-only summary, safe for XCom per MASTER_PLAN's XCom discipline:
        {"kpi": <table>, "row_count": <rows now in the table>}.

    Raises:
        KpiError: the script file is not where it is expected to be.
    """
    script_path = SQL_DIR / f"{table}.sql"
    if not script_path.is_file():
        raise KpiError(
            f"KPI script not found for {table!r}: expected {script_path}. "
            "include/sql/analytics/ must be deployed alongside src/."
        )

    sql = script_path.read_text(encoding="utf-8")
    engine = engine or get_analytics_engine()

    with engine.begin() as conn:
        # The whole file goes to the driver in one call. Do NOT split it on
        # ";" — these scripts contain semicolons inside comments (for example
        # "the KPI's grain; declaring it as the key means...") and splitting
        # there cuts a statement in half. psycopg2 executes multi-statement
        # scripts natively, exactly as psql and Airflow's Postgres operator do.
        conn.exec_driver_sql(sql)

        # Counted inside the same transaction as the write, so the number
        # reported is the number this run produced. The table name is a
        # module-level constant, never input.
        row_count = int(
            conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one()
        )

    logger.info("Computed %s: %d rows.", table, row_count)
    return {"kpi": table, "row_count": row_count}
