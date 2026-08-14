"""Level 1 — file-level schema validation (docs/MASTER_PLAN.md).

Runs before any database insertion is attempted. A failure here means the
source file is structurally wrong, which is a pipeline-stopping error, not a
quarantine event: there is no sensible way to quarantine "the file has the
wrong columns".

This module deliberately imports nothing but the standard library — no
SQLAlchemy, no Airflow, no pandas. That is what makes the schema check
unit-testable without infrastructure structurally true rather than a happy
accident, and it keeps the fail-fast gate cheap enough to run before the
13.49 MB file is handed to the database.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from .exceptions import SourceFileError, SourceSchemaError

logger = logging.getLogger(__name__)

# The 17 columns confirmed against the real file by Phase 0 profiling
# (docs/data_profile.md, "Full column inventory"). Listed in source order for
# documentation; the check itself compares as a set — see validate_source_schema.
EXPECTED_COLUMNS: tuple[str, ...] = (
    "Airline",
    "Source",
    "Source Name",
    "Destination",
    "Destination Name",
    "Departure Date & Time",
    "Arrival Date & Time",
    "Duration (hrs)",
    "Stopovers",
    "Aircraft Type",
    "Class",
    "Booking Source",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
    "Seasonality",
    "Days Before Departure",
)

# Data rows sampled for field-count consistency. This is a structural smoke
# test — "is this actually a CSV with this shape" — not validation. Levels 2/3
# in src/validation/ read every row later; doing it twice would make this task
# as expensive as the thing it exists to run cheaply in front of.
PARSE_SAMPLE_ROWS = 50


def validate_source_schema(source_path: str) -> bool:
    """Confirm the source CSV is readable, correctly shaped, and parseable.

    Checks, in order:
      1. the path exists and can be opened
      2. it has a header row
      3. the header has no duplicate column names
      4. the header is exactly EXPECTED_COLUMNS as a set — order-tolerant,
         but neither missing nor extra columns are accepted
      5. the first PARSE_SAMPLE_ROWS data rows parse with the right field count
      6. there is at least one data row

    Returns True on success. The meaningful failure signal is the exception,
    not the return value — this never returns False, because every failure
    mode has a specific, actionable message attached to it that a bare False
    would throw away.

    Raises:
        SourceFileError: the file is missing or cannot be opened.
        SourceSchemaError: the file is readable but structurally wrong.
    """
    path = Path(source_path)

    if not path.exists():
        raise SourceFileError(f"Source file does not exist: {path}")
    if not path.is_file():
        raise SourceFileError(f"Source path exists but is not a file: {path}")

    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise SourceFileError(f"Source file cannot be opened for reading: {path} ({exc})") from exc

    # encoding="utf-8-sig" strips a UTF-8 BOM if present. A spreadsheet-exported
    # CSV carries one, which would otherwise turn the first header into
    # "﻿Airline" and fail the column check for a reason that has nothing
    # to do with the schema being wrong.
    with handle:
        reader = csv.reader(handle)
        try:
            raw_header = next(reader)
        except StopIteration:
            raise SourceSchemaError(f"Source file is empty — no header row: {path}") from None
        except (csv.Error, UnicodeDecodeError) as exc:
            raise SourceSchemaError(
                f"Source file is not parseable as CSV: {path} ({exc})"
            ) from exc

        # Tolerate stray whitespace around header names: that is a formatting
        # artifact, not a different schema. Missing/extra columns are NOT
        # tolerated, which is the distinction this check exists to draw.
        header = [column.strip() for column in raw_header]
        _check_no_duplicate_columns(header, path)
        _check_column_set(header, path)
        _check_rows_parse(reader, len(header), path)

    logger.info(
        "Level 1 schema validation passed: %s (%d columns, first %d rows sampled)",
        path,
        len(EXPECTED_COLUMNS),
        PARSE_SAMPLE_ROWS,
    )
    return True


def _check_no_duplicate_columns(header: list[str], path: Path) -> None:
    """Reject repeated column names.

    Checked explicitly because the set comparison in _check_column_set cannot
    see this: a file with "Airline" twice and "Seasonality" missing has the
    right number of columns and, once deduplicated, the wrong ones — but a
    naive set difference against a 17-name expectation could still look
    plausible. It also means row[index_of[name]] in the loader is unambiguous.
    """
    seen: set[str] = set()
    duplicates: set[str] = set()
    for column in header:
        if column in seen:
            duplicates.add(column)
        seen.add(column)

    if duplicates:
        raise SourceSchemaError(
            f"Source file has duplicate column headers {sorted(duplicates)}: {path}"
        )


def _check_column_set(header: list[str], path: Path) -> None:
    """Compare the header to EXPECTED_COLUMNS as a set.

    Order-tolerant by construction (sets have no order); the loader maps each
    column by name, so a reordered file loads correctly. Missing or extra
    columns are rejected: a missing one breaks a documented contract column,
    and an extra one means the source changed shape in a way nobody has
    reviewed against docs/data_contract.md.
    """
    actual = set(header)
    expected = set(EXPECTED_COLUMNS)

    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    if not missing and not unexpected:
        return

    problems = []
    if missing:
        problems.append(f"missing {missing}")
    if unexpected:
        problems.append(f"unexpected {unexpected}")

    raise SourceSchemaError(
        f"Source file column set does not match the expected {len(EXPECTED_COLUMNS)} "
        f"columns from docs/data_profile.md: {'; '.join(problems)}. "
        f"File: {path}"
    )


def _check_rows_parse(reader: Any, field_count: int, path: Path) -> None:
    """Sample the first PARSE_SAMPLE_ROWS data rows for structural sanity.

    Catches a file that has a plausible first line but is not really a CSV of
    this shape. Wholly-blank lines are skipped rather than failed — a trailing
    newline is not a schema problem.
    """
    rows_checked = 0
    try:
        for row in reader:
            if rows_checked >= PARSE_SAMPLE_ROWS:
                break
            if not row:
                continue
            if len(row) != field_count:
                raise SourceSchemaError(
                    f"Source file row at line {reader.line_num} has {len(row)} fields, "
                    f"expected {field_count}: {path}"
                )
            rows_checked += 1
    except (csv.Error, UnicodeDecodeError) as exc:
        raise SourceSchemaError(f"Source file is not parseable as CSV: {path} ({exc})") from exc

    if rows_checked == 0:
        raise SourceSchemaError(
            f"Source file has a valid header but no data rows: {path}"
        )
