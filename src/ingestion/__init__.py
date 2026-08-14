"""CSV -> MySQL staging.

Two responsibilities, kept in separate modules on purpose:

  schema.py          Level 1 file-level validation. Standard library only, so
                     it is testable with no database and no Airflow.
  staging_loader.py  the truncate-and-reload into staging.raw_flights, plus
                     the run's audit row in staging.pipeline_runs.

Column set and file shape were confirmed against the real CSV by Phase 0
profiling — see docs/data_profile.md.
"""

from .exceptions import (
    IngestionError,
    SourceFileError,
    SourceSchemaError,
    StagingLoadError,
)
from .schema import EXPECTED_COLUMNS, validate_source_schema
from .staging_loader import (
    COLUMN_MAP,
    compute_file_checksum,
    get_staging_engine,
    load_to_mysql_staging,
)

__all__ = [
    "COLUMN_MAP",
    "EXPECTED_COLUMNS",
    "IngestionError",
    "SourceFileError",
    "SourceSchemaError",
    "StagingLoadError",
    "compute_file_checksum",
    "get_staging_engine",
    "load_to_mysql_staging",
    "validate_source_schema",
]
