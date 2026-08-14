"""Ingestion failure types.

These all represent Level 1 failures in docs/MASTER_PLAN.md's failure-strategy
table: the source file itself is structurally wrong, or the staging load could
not complete. Every one of them means "fail fast, no retry" — none of them is
ever a quarantine-and-continue event, which is reserved for row-level Level 2/3
problems handled in src/validation/.

They are distinct types rather than bare ValueError so the DAG can tell a bad
file apart from a bad database, and so a `except IngestionError` in a caller
cannot accidentally swallow an unrelated ValueError from library code.
"""


class IngestionError(Exception):
    """Base for every ingestion failure. Catch this to catch all of them."""


class SourceFileError(IngestionError):
    """The file is missing, is not a file, or cannot be opened for reading.

    Distinct from SourceSchemaError because the remedy differs: this is a
    delivery/permissions problem, not a problem with the file's contents.
    """


class SourceSchemaError(IngestionError):
    """The file is readable but structurally wrong.

    Wrong column set, duplicate headers, unparseable as CSV, or no data rows.
    Raised by validate_source_schema, which is the fail-fast gate that runs
    before the database is touched at all.
    """


class StagingLoadError(IngestionError):
    """The load into staging.raw_flights could not be completed correctly.

    Includes the row-count reconciliation failing (rows read from the CSV not
    matching rows landed in the table), which would otherwise silently break
    the `source_row_count = valid + rejected` equation later in the run.
    """
