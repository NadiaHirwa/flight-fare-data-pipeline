"""Unit tests for Level 1 file-level schema validation.

No database, no Airflow: validate_source_schema is standard-library-only by
design (see src/ingestion/schema.py), so every case here runs against a small
fixture CSV written to tmp_path. Database behaviour belongs to the integration
tests described in docs/testing_strategy.md, not here.
"""

import csv
from pathlib import Path

import pytest

from src.ingestion.exceptions import IngestionError, SourceFileError, SourceSchemaError
from src.ingestion.schema import (
    EXPECTED_COLUMNS,
    PARSE_SAMPLE_ROWS,
    validate_source_schema,
)

# One valid row, positionally matching EXPECTED_COLUMNS.
SAMPLE_ROW = [
    "Biman Bangladesh Airlines",  # Airline
    "DAC",                        # Source
    "Hazrat Shahjalal International Airport",  # Source Name
    "CGP",                        # Destination
    "Shah Amanat International Airport, Chittagong",  # Destination Name
    "2025-11-17 06:25:00",        # Departure Date & Time
    "2025-11-17 07:38:10",        # Arrival Date & Time
    "1.2195264539377717",         # Duration (hrs)
    "Direct",                     # Stopovers
    "Airbus A320",                # Aircraft Type
    "Economy",                    # Class
    "Online Website",             # Booking Source
    "21131.22502141266",          # Base Fare (BDT)
    "5169.683753211899",          # Tax & Surcharge (BDT)
    "26300.90877462456",          # Total Fare (BDT)
    "Regular",                    # Seasonality
    "10",                         # Days Before Departure
]

# The real dataset's header row, captured verbatim and checked in. The dataset
# itself is gitignored (include/data/*.csv — size and licensing), so a test that
# reads it would skip in CI, and a guard test that skips exactly where drift
# would go unnoticed is not guarding anything. The header alone is a few hundred
# bytes and carries no licensed data.
SOURCE_HEADER_FIXTURE = Path(__file__).parent / "fixtures" / "sample_header.csv"

# Only present on a machine where the Kaggle download has been placed. Tests
# using this are a local-only bonus and skip in CI by design — the checked-in
# fixture above is what actually holds the line there.
REAL_DATASET = (
    Path(__file__).parent.parent / "include" / "data" /
    "Flight_Price_Dataset_of_Bangladesh.csv"
)


def write_csv(path: Path, header, rows) -> Path:
    """Write a CSV fixture. Uses the csv module so quoting matches real files."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


@pytest.fixture
def valid_csv(tmp_path: Path) -> Path:
    return write_csv(tmp_path / "valid.csv", list(EXPECTED_COLUMNS), [SAMPLE_ROW])


# ---------------------------------------------------------------------------
# The expectation itself
# ---------------------------------------------------------------------------

def test_expected_columns_has_seventeen_entries():
    """docs/data_profile.md: 'Seventeen columns total'."""
    assert len(EXPECTED_COLUMNS) == 17
    assert len(set(EXPECTED_COLUMNS)) == 17, "EXPECTED_COLUMNS contains a duplicate"


def test_expected_columns_matches_the_checked_in_source_header():
    """Guards EXPECTED_COLUMNS against drifting from the real file's header.

    Every other test in this module builds its fixtures out of EXPECTED_COLUMNS,
    so they would all still pass if the constant itself were wrong. This is the
    one test that compares it against something independent: the real header
    row, captured verbatim from the dataset Phase 0 profiled
    (docs/data_profile.md, "Full column inventory") and committed under
    tests/fixtures/ so it runs everywhere, CI included.
    """
    with SOURCE_HEADER_FIXTURE.open(newline="", encoding="utf-8-sig") as handle:
        header = next(csv.reader(handle))
    assert [column.strip() for column in header] == list(EXPECTED_COLUMNS)


def test_real_dataset_header_matches_the_fixture():
    """Local-only: catches the fixture going stale if the dataset is replaced.

    Skips in CI, where the dataset is absent — that is the whole reason the
    fixture exists. Here it verifies the two agree wherever both are present.
    """
    if not REAL_DATASET.exists():
        pytest.skip(f"Source dataset not present at {REAL_DATASET}")
    with REAL_DATASET.open(newline="", encoding="utf-8-sig") as handle:
        real_header = next(csv.reader(handle))
    with SOURCE_HEADER_FIXTURE.open(newline="", encoding="utf-8-sig") as handle:
        fixture_header = next(csv.reader(handle))
    assert real_header == fixture_header


def test_real_dataset_passes_validation():
    """Local-only: end-to-end pass over the actual 57,000-row file."""
    if not REAL_DATASET.exists():
        pytest.skip(f"Source dataset not present at {REAL_DATASET}")
    assert validate_source_schema(str(REAL_DATASET)) is True


# ---------------------------------------------------------------------------
# Correct files
# ---------------------------------------------------------------------------

def test_valid_csv_passes(valid_csv: Path):
    assert validate_source_schema(str(valid_csv)) is True


def test_column_order_does_not_matter(tmp_path: Path):
    """Order-tolerant: the loader maps columns by name, not position."""
    reordered = list(reversed(EXPECTED_COLUMNS))
    path = write_csv(tmp_path / "reordered.csv", reordered, [list(reversed(SAMPLE_ROW))])
    assert validate_source_schema(str(path)) is True


def test_surrounding_whitespace_in_headers_is_tolerated(tmp_path: Path):
    """A formatting artifact, not a different schema."""
    padded = [f"  {column} " for column in EXPECTED_COLUMNS]
    path = write_csv(tmp_path / "padded.csv", padded, [SAMPLE_ROW])
    assert validate_source_schema(str(path)) is True


def test_utf8_bom_is_tolerated(tmp_path: Path):
    """Spreadsheet exports carry a BOM; it must not read as a wrong column."""
    path = tmp_path / "bom.csv"
    write_csv(path, list(EXPECTED_COLUMNS), [SAMPLE_ROW])
    path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())
    assert validate_source_schema(str(path)) is True


def test_trailing_blank_line_is_tolerated(tmp_path: Path):
    path = write_csv(tmp_path / "trailing.csv", list(EXPECTED_COLUMNS), [SAMPLE_ROW])
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    assert validate_source_schema(str(path)) is True


# ---------------------------------------------------------------------------
# Wrong column set
# ---------------------------------------------------------------------------

def test_missing_column_is_rejected(tmp_path: Path):
    header = [c for c in EXPECTED_COLUMNS if c != "Seasonality"]
    row = [v for c, v in zip(EXPECTED_COLUMNS, SAMPLE_ROW) if c != "Seasonality"]
    path = write_csv(tmp_path / "missing.csv", header, [row])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    message = str(excinfo.value)
    assert "Seasonality" in message
    assert "missing" in message


def test_extra_column_is_rejected(tmp_path: Path):
    header = list(EXPECTED_COLUMNS) + ["Loyalty Points"]
    path = write_csv(tmp_path / "extra.csv", header, [SAMPLE_ROW + ["500"]])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    message = str(excinfo.value)
    assert "Loyalty Points" in message
    assert "unexpected" in message


def test_missing_and_extra_are_reported_together(tmp_path: Path):
    """One run should surface the whole problem, not the first half of it."""
    header = [c for c in EXPECTED_COLUMNS if c != "Stopovers"] + ["Layovers"]
    row = [v for c, v in zip(EXPECTED_COLUMNS, SAMPLE_ROW) if c != "Stopovers"] + ["1"]
    path = write_csv(tmp_path / "both.csv", header, [row])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    message = str(excinfo.value)
    assert "Stopovers" in message
    assert "Layovers" in message


def test_renamed_column_reports_both_sides(tmp_path: Path):
    """A rename is a missing column and an extra one — the message should say so."""
    header = ["Carrier" if c == "Airline" else c for c in EXPECTED_COLUMNS]
    path = write_csv(tmp_path / "renamed.csv", header, [SAMPLE_ROW])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    message = str(excinfo.value)
    assert "Airline" in message
    assert "Carrier" in message


def test_duplicate_column_is_rejected(tmp_path: Path):
    """A set comparison alone cannot see this, so it is checked explicitly."""
    header = list(EXPECTED_COLUMNS)
    header[header.index("Seasonality")] = "Airline"
    path = write_csv(tmp_path / "duplicate.csv", header, [SAMPLE_ROW])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    assert "duplicate" in str(excinfo.value).lower()


def test_completely_different_header_is_rejected(tmp_path: Path):
    path = write_csv(tmp_path / "other.csv", ["a", "b", "c"], [["1", "2", "3"]])
    with pytest.raises(SourceSchemaError):
        validate_source_schema(str(path))


# ---------------------------------------------------------------------------
# Not a usable CSV
# ---------------------------------------------------------------------------

def test_empty_file_is_rejected(tmp_path: Path):
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    assert "empty" in str(excinfo.value).lower()


def test_header_only_file_is_rejected(tmp_path: Path):
    """Structurally valid but carries no data — a Level 1 failure."""
    path = write_csv(tmp_path / "header_only.csv", list(EXPECTED_COLUMNS), [])

    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    assert "no data rows" in str(excinfo.value).lower()


def test_ragged_row_within_the_sample_is_rejected(tmp_path: Path):
    path = write_csv(
        tmp_path / "ragged.csv",
        list(EXPECTED_COLUMNS),
        [SAMPLE_ROW, SAMPLE_ROW[:10]],
    )
    with pytest.raises(SourceSchemaError) as excinfo:
        validate_source_schema(str(path))
    message = str(excinfo.value)
    assert "fields" in message
    assert "expected 17" in message


def test_non_csv_content_is_rejected(tmp_path: Path):
    """A JSON file handed to the pipeline by mistake."""
    path = tmp_path / "not.csv"
    path.write_text('{"airline": "Biman", "fare": 26300.9}', encoding="utf-8")
    with pytest.raises(SourceSchemaError):
        validate_source_schema(str(path))


def test_missing_file_raises_source_file_error(tmp_path: Path):
    with pytest.raises(SourceFileError) as excinfo:
        validate_source_schema(str(tmp_path / "nope.csv"))
    assert "does not exist" in str(excinfo.value)


def test_directory_path_raises_source_file_error(tmp_path: Path):
    with pytest.raises(SourceFileError) as excinfo:
        validate_source_schema(str(tmp_path))
    assert "not a file" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Scope: cheap and fast, not full validation
# ---------------------------------------------------------------------------

def test_ragged_row_beyond_the_sample_is_not_inspected(tmp_path: Path):
    """Documents the deliberate boundary of this check.

    A row broken far past PARSE_SAMPLE_ROWS passes Level 1 — this task is a
    structural smoke test that must stay cheap. Row-level problems are caught
    by Levels 2/3 in src/validation/ and quarantined, per ADR-003.
    """
    rows = [SAMPLE_ROW] * (PARSE_SAMPLE_ROWS + 10) + [SAMPLE_ROW[:5]]
    path = write_csv(tmp_path / "late_ragged.csv", list(EXPECTED_COLUMNS), rows)
    assert validate_source_schema(str(path)) is True


def test_contract_violating_values_still_pass_level_one(tmp_path: Path):
    """Level 1 is about shape, not values.

    A negative fare and an unknown airport code are Level 2/3 concerns; if this
    test ever fails, validation logic has leaked into the wrong layer.
    """
    row = list(SAMPLE_ROW)
    row[EXPECTED_COLUMNS.index("Base Fare (BDT)")] = "-999.00"
    row[EXPECTED_COLUMNS.index("Source")] = "ZZZ"
    path = write_csv(tmp_path / "bad_values.csv", list(EXPECTED_COLUMNS), [row])
    assert validate_source_schema(str(path)) is True


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("error_type", [SourceFileError, SourceSchemaError])
def test_errors_share_a_catchable_base(error_type):
    """A caller can catch IngestionError to catch all Level 1 failures."""
    assert issubclass(error_type, IngestionError)
