"""Levels 2-3 row-level validation and the quarantine writer (ADR-003).

Implements the rules defined in docs/data_contract.md — do not hardcode rules
here that aren't first written in the contract.

  reference_data.py  the contract's value sets, transcribed and cited. Nothing
                     is derived from the file being validated (ADR-010).
  rules.py           one function per rule, plus the batch-level duplicate
                     tracker. Standard-library only, so every rule is testable
                     without a database.
  quarantine.py      reads staging, writes one quarantine row per violation,
                     returns a counts-only summary for XCom.

Level 1 (file-level schema) is not here — it runs before the database is
touched at all, in src/ingestion/schema.py.

Normalization and the record hash are NOT re-exported here. They live in
src/shared/normalization.py because src/transformation/ needs the identical
implementation, and it should import them from there rather than from this
package — a shared utility reached through validation would make
transformation depend on validation for no reason.
"""

from .quarantine import (
    SELECT_VALID_ROWS_SQL,
    count_valid_rows,
    iter_valid_rows,
    validate_and_quarantine,
)
from .reference_data import (
    DOMESTIC_AIRPORT_CODES,
    DUPLICATE_GRAIN_KEY_FIELDS,
    INTERNATIONAL_AIRPORT_CODES,
    REQUIRED_FIELDS,
    TOTAL_FARE_TOLERANCE,
    VALID_DESTINATION_CODES,
    VALID_FARE_CLASSES,
    VALID_SEASONALITY_VALUES,
    VALID_SOURCE_CODES,
)
from .rules import (
    ALL_RULES,
    DuplicateGrainKeyTracker,
    Violation,
    validate_batch,
    validate_record,
)

__all__ = [
    "ALL_RULES",
    "DOMESTIC_AIRPORT_CODES",
    "DUPLICATE_GRAIN_KEY_FIELDS",
    "INTERNATIONAL_AIRPORT_CODES",
    "REQUIRED_FIELDS",
    "SELECT_VALID_ROWS_SQL",
    "TOTAL_FARE_TOLERANCE",
    "VALID_DESTINATION_CODES",
    "VALID_FARE_CLASSES",
    "VALID_SEASONALITY_VALUES",
    "VALID_SOURCE_CODES",
    "DuplicateGrainKeyTracker",
    "Violation",
    "count_valid_rows",
    "iter_valid_rows",
    "validate_and_quarantine",
    "validate_batch",
    "validate_record",
]
