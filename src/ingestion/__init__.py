"""CSV -> MySQL staging.

Two responsibilities, kept in separate modules on purpose:

  schema.py          Level 1 file-level validation. Standard library only, so
                     it is testable with no database and no Airflow.
  staging_loader.py  the truncate-and-reload into staging.raw_flights.

Column set and file shape were confirmed against the real CSV by Phase 0
profiling — see docs/data_profile.md.

The engine, the staging table names, COLUMN_MAP, and the staging.pipeline_runs
audit writers are NOT re-exported here — they live in src/shared/ because
validation and transformation use them too, and should import them from there
rather than through this package.
"""

from .exceptions import (
    IngestionError,
    SourceFileError,
    SourceSchemaError,
    StagingLoadError,
)
from .schema import EXPECTED_COLUMNS, validate_source_schema
from .staging_loader import compute_file_checksum, load_to_mysql_staging

__all__ = [
    "EXPECTED_COLUMNS",
    "IngestionError",
    "SourceFileError",
    "SourceSchemaError",
    "StagingLoadError",
    "compute_file_checksum",
    "load_to_mysql_staging",
    "validate_source_schema",
]
