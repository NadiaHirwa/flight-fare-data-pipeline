"""Type conversion and the load into the PostgreSQL fact table.

  converters.py   staging strings -> the declared types in
                  docs/data_contract.md. Pure, so every conversion is
                  testable without a database.
  fact_loader.py  reads this run's valid rows, converts them, and
                  truncate-and-reloads analytics.flight_fare_quotes.

This stage does NOT re-validate. Levels 2 and 3 (src/validation/) own the
contract; by the time a row arrives here it has already passed. In particular
the Total Fare reconciliation is not repeated, and total_fare is converted as
the source stated it rather than recomputed from base + tax.

This is the step that makes the pipeline ETL rather than ELT (ADR-009):
transformation happens in Python, before the data reaches the destination
that matters.
"""

from .converters import FACT_COLUMNS, convert_record, convert_records
from .exceptions import (
    FactLoadError,
    RecordConversionError,
    TransformationError,
)
from .fact_loader import FACT_TABLE, transform_and_load_fact

__all__ = [
    "FACT_COLUMNS",
    "FACT_TABLE",
    "FactLoadError",
    "RecordConversionError",
    "TransformationError",
    "convert_record",
    "convert_records",
    "transform_and_load_fact",
]
