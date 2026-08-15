"""Quality gates, post-load checks, and source-to-target reconciliation.

  gate.py  the ADR-005 rejection-rate threshold, applied between validation
           and the fact load. Standard library only — a pure decision over a
           counts-only summary.

Still to come: the post-load checks and the KPI-level sanity checks described
in docs/kpi_definitions.md.
"""

from .gate import REJECTION_RATE_THRESHOLD, QualityGateError, quality_gate_check

__all__ = [
    "REJECTION_RATE_THRESHOLD",
    "QualityGateError",
    "quality_gate_check",
]
