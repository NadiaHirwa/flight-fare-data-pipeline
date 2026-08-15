"""KPI computation failure types."""


class KpiError(Exception):
    """A KPI script could not be located or run.

    Distinct from a SQL error surfacing from SQLAlchemy: this means the
    pipeline could not find the script it was asked to execute, which is a
    packaging or deployment problem rather than a data one.
    """
