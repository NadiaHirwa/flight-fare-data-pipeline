"""One module per KPI's logic — or rather, one runner for all four.

The KPI SQL is already written in include/sql/analytics/ and is the single
definition of each metric (docs/kpi_definitions.md). This package executes
those files; it does not restate their logic in Python, which would create a
second definition free to drift from the first.

  runner.py  one function per KPI, each running its own script and returning
             the row count it wrote.
"""

from .exceptions import KpiError
from .runner import (
    KPI_TABLES,
    SQL_DIR,
    compute_avg_fare_by_airline,
    compute_flight_offer_count_by_airline,
    compute_seasonal_fare_variation,
    compute_top_routes,
)

__all__ = [
    "KPI_TABLES",
    "SQL_DIR",
    "KpiError",
    "compute_avg_fare_by_airline",
    "compute_flight_offer_count_by_airline",
    "compute_seasonal_fare_variation",
    "compute_top_routes",
]
