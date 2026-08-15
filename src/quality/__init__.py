"""Quality gates, post-load checks, and source-to-target reconciliation.

  gate.py            the ADR-005 rejection-rate threshold, applied between
                     validation and the fact load. Standard library only — a
                     pure decision over a counts-only summary.
  post_load.py       the KPI-level sanity checks from docs/kpi_definitions.md,
                     run against the analytics database after the KPIs build.
  reconciliation.py  the source-to-target equations from docs/MASTER_PLAN.md,
                     read from the run's audit row and cross-checked against
                     the fact table itself.

Each module keeps its comparison logic as a pure function over a snapshot, so
every failure mode is testable with a hand-built fixture rather than a
deliberately broken database.
"""

from .gate import REJECTION_RATE_THRESHOLD, QualityGateError, quality_gate_check
from .post_load import (
    AirlineFareBounds,
    PostLoadFacts,
    QualityCheckError,
    evaluate_post_load,
    post_load_quality_check,
)
from .reconciliation import (
    ReconciliationError,
    RunCounts,
    evaluate_reconciliation,
    reconciliation_check,
)

__all__ = [
    "REJECTION_RATE_THRESHOLD",
    "AirlineFareBounds",
    "PostLoadFacts",
    "QualityCheckError",
    "QualityGateError",
    "ReconciliationError",
    "RunCounts",
    "evaluate_post_load",
    "evaluate_reconciliation",
    "post_load_quality_check",
    "quality_gate_check",
    "reconciliation_check",
]
