"""Unit tests for staging-string -> fact-table type conversion.

No database — src/transformation/converters.py is pure by design, so every
conversion is exercised against a fixture row here. The load itself
(truncate-and-reload, batching, count reconciliation) is database behaviour and
belongs to the integration tests in docs/testing_strategy.md.

The fixture uses the real file's full float precision, because that is the
actual input: staging is all-VARCHAR and the source CSV carries values like
21131.22502141266. Rounding to NUMERIC(12,2) happens here, not before.
"""

from datetime import datetime
from decimal import Decimal

import pytest

from src.shared.normalization import compute_record_hash
from src.transformation.converters import (
    FACT_COLUMNS,
    convert_record,
    convert_records,
)
from src.transformation.exceptions import RecordConversionError

# A valid staging row, as iter_valid_rows yields it: every value a string.
STAGED_ROW = {
    "source_row_number": 1,
    "pipeline_run_id": "manual__test",
    "airline": "Malaysian Airlines",
    "source": "CXB",
    "source_name": "Cox's Bazar Airport",
    "destination": "CCU",
    "destination_name": "Netaji Subhas Chandra Bose International Airport, Kolkata",
    "departure_datetime": "2025-11-17 06:25:00",
    "arrival_datetime": "2025-11-17 07:38:10",
    "duration_hrs": "1.2195264539377717",
    "stopovers": "Direct",
    "aircraft_type": "Airbus A320",
    "fare_class": "Economy",
    "booking_source": "Online Website",
    "base_fare_bdt": "21131.22502141266",
    "tax_surcharge_bdt": "5169.683753211899",
    "total_fare_bdt": "26300.90877462456",
    "seasonality": "Regular",
    "days_before_departure": "10",
}

REQUIRED_FACT_FIELDS = (
    "airline", "source", "destination", "departure_datetime", "fare_class",
    "seasonality", "base_fare", "tax_surcharge", "total_fare",
)

CARRIED_THROUGH_FIELDS = (
    "source_name", "destination_name", "arrival_datetime", "duration_hrs",
    "stopovers", "aircraft_type", "booking_source", "days_before_departure",
)


def staged(**overrides):
    return {**STAGED_ROW, **overrides}


def naive(*args) -> datetime:
    """Build a naive datetime for an expected value.

    DTZ001 asks for a tzinfo, on the assumption that an aware datetime is
    always wanted. That is not true here: flight_fare_quotes.departure_datetime
    is TIMESTAMP WITHOUT TIME ZONE because a scheduled departure is wall-clock
    time at the origin airport, and the source carries no offset. Naive is the
    correct type, so the rule is suppressed once here with a reason rather than
    scattered across the assertions — and rather than building expectations
    with fromisoformat, which would compare the code under test against itself.
    """
    return datetime(*args)  # noqa: DTZ001


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------

def test_converted_row_has_exactly_the_fact_columns():
    """Guards against the dict and the INSERT column list drifting apart."""
    assert set(convert_record(STAGED_ROW)) == set(FACT_COLUMNS)


def test_fact_columns_count_matches_the_ddl():
    """17 contract columns + source_record_hash."""
    assert len(FACT_COLUMNS) == 18


def test_convert_records_streams():
    rows = list(convert_records([STAGED_ROW, staged(source_row_number=2)]))
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# String -> Decimal (ADR-008: NUMERIC(12,2), never FLOAT)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", ["base_fare", "tax_surcharge", "total_fare"])
def test_money_is_decimal_not_float(field):
    value = convert_record(STAGED_ROW)[field]
    assert isinstance(value, Decimal)
    assert not isinstance(value, float)


def test_money_is_quantized_to_two_decimal_places():
    row = convert_record(STAGED_ROW)
    assert row["base_fare"] == Decimal("21131.23")
    assert row["tax_surcharge"] == Decimal("5169.68")
    assert row["total_fare"] == Decimal("26300.91")
    for field in ("base_fare", "tax_surcharge", "total_fare"):
        assert row[field].as_tuple().exponent == -2


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1600.9756881504618", "1600.98"),
        ("200.0", "200.00"),
        ("558987.332444109", "558987.33"),
        ("0.005", "0.01"),      # ROUND_HALF_UP, not banker's rounding
        ("0.015", "0.02"),      # banker's would give 0.02 here too
        ("0.025", "0.03"),      # banker's would give 0.02 -- this is the tell
        ("1.005", "1.01"),
    ],
)
def test_money_rounding_is_half_up(raw, expected):
    assert convert_record(staged(base_fare_bdt=raw))["base_fare"] == Decimal(expected)


def test_total_fare_is_carried_across_not_recomputed():
    """Level 3 owns the reconciliation; this stage must not rewrite the value.

    Base + tax here is 3.00, but the source says 999.00. Re-deriving would
    silently replace the source's number and destroy the evidence that the
    Level 3 check was ever meaningful. (Such a row would never reach here in
    practice -- validation would have quarantined it.)
    """
    row = convert_record(
        staged(base_fare_bdt="1.00", tax_surcharge_bdt="2.00", total_fare_bdt="999.00")
    )
    assert row["total_fare"] == Decimal("999.00")


@pytest.mark.parametrize(
    ("base", "tax", "total"),
    [
        ("100.005", "200.005", "300.01"),   # every column on a rounding midpoint
        ("100.004", "200.004", "300.008"),  # every column rounding down
        ("21131.22502141266", "5169.683753211899", "26300.90877462456"),  # real row
        ("1600.9756881504618", "200.0", "1800.9756881504618"),
        ("0.005", "0.005", "0.01"),
    ],
)
def test_rounding_adds_almost_no_drift(base, tax, total):
    """Bounds the drift ROUNDING introduces, which is the claim that matters.

    Each column moves by at most 0.005, so rounding can add at most 0.015 to
    whatever drift the source row already carried. It is the *added* drift that
    is bounded — the source's own drift is the larger term, and Level 3 has
    already confirmed that one is within 1.00.
    """
    raw_drift = abs(Decimal(total) - (Decimal(base) + Decimal(tax)))
    row = convert_record(
        staged(base_fare_bdt=base, tax_surcharge_bdt=tax, total_fare_bdt=total)
    )
    rounded_drift = abs(row["total_fare"] - (row["base_fare"] + row["tax_surcharge"]))

    assert rounded_drift - raw_drift <= Decimal("0.015")


def test_an_exactly_reconciling_row_still_reconciles_after_rounding():
    """The case that actually reaches the fact table: 95.58% of real rows
    reconcile exactly, and rounding must not break the CHECK constraint."""
    row = convert_record(STAGED_ROW)  # the real first row of the source file
    drift = abs(row["total_fare"] - (row["base_fare"] + row["tax_surcharge"]))
    assert drift <= Decimal("1.00")


# ---------------------------------------------------------------------------
# String -> datetime
# ---------------------------------------------------------------------------

def test_departure_timestamp_is_a_datetime():
    value = convert_record(STAGED_ROW)["departure_datetime"]
    assert isinstance(value, datetime)
    assert value == naive(2025, 11, 17, 6, 25, 0)


def test_arrival_timestamp_is_a_datetime():
    assert convert_record(STAGED_ROW)["arrival_datetime"] == naive(
        2025, 11, 17, 7, 38, 10
    )


@pytest.mark.parametrize(
    "raw",
    ["2025-11-17 06:25:00", "2025-11-17T06:25:00", "2025-11-17 06:25:00.000000"],
)
def test_accepted_timestamp_spellings(raw):
    assert convert_record(staged(departure_datetime=raw))[
        "departure_datetime"
    ] == naive(2025, 11, 17, 6, 25, 0)


def test_timestamp_is_naive_no_timezone_invented():
    """The column is TIMESTAMP WITHOUT TIME ZONE: a scheduled departure is
    wall-clock time at the origin, and the source carries no offset."""
    assert convert_record(STAGED_ROW)["departure_datetime"].tzinfo is None


# ---------------------------------------------------------------------------
# String -> int / text
# ---------------------------------------------------------------------------

def test_days_before_departure_is_an_int():
    value = convert_record(STAGED_ROW)["days_before_departure"]
    assert isinstance(value, int)
    assert value == 10


def test_whole_number_written_as_float_is_still_an_int():
    """A re-export through a float-typing tool must not lose the column."""
    assert convert_record(staged(days_before_departure="10.0"))[
        "days_before_departure"
    ] == 10


def test_duration_is_quantized_decimal():
    value = convert_record(STAGED_ROW)["duration_hrs"]
    assert value == Decimal("1.22")
    assert value.as_tuple().exponent == -2


def test_text_fields_carry_across():
    row = convert_record(STAGED_ROW)
    assert row["airline"] == "Malaysian Airlines"
    assert row["stopovers"] == "Direct"
    assert row["source_name"] == "Cox's Bazar Airport"


# ---------------------------------------------------------------------------
# Normalization, so stored values match the hashed ones
# ---------------------------------------------------------------------------

def test_iata_codes_are_normalized():
    row = convert_record(staged(source=" cxb ", destination="ccu"))
    assert row["source"] == "CXB"
    assert row["destination"] == "CCU"


def test_airline_whitespace_stripped_case_preserved():
    assert convert_record(staged(airline="  Biman  "))["airline"] == "Biman"


def test_hash_matches_the_shared_implementation():
    """Identical by construction to what src/validation/ writes to quarantine —
    that is what makes the quarantine-to-fact join meaningful."""
    assert convert_record(STAGED_ROW)["source_record_hash"] == compute_record_hash(
        STAGED_ROW
    )


def test_hash_is_unaffected_by_formatting():
    assert (
        convert_record(staged(source=" cxb "))["source_record_hash"]
        == convert_record(STAGED_ROW)["source_record_hash"]
    )


def test_hash_is_sha256_shaped():
    value = convert_record(STAGED_ROW)["source_record_hash"]
    assert len(value) == 64
    assert set(value) <= set("0123456789abcdef")


# ---------------------------------------------------------------------------
# Required fields are strict: a failure means validation was bypassed
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "field",
    ["airline", "source", "destination", "fare_class", "seasonality"],
)
def test_missing_required_text_raises(field):
    with pytest.raises(RecordConversionError) as excinfo:
        convert_record(staged(**{field: ""}))
    assert field in str(excinfo.value)


@pytest.mark.parametrize(
    "field", ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]
)
@pytest.mark.parametrize("bad", ["", "N/A", "NaN", "Infinity"])
def test_unusable_required_money_raises(field, bad):
    with pytest.raises(RecordConversionError):
        convert_record(staged(**{field: bad}))


@pytest.mark.parametrize("bad", ["", "not-a-date", "17/11/2025"])
def test_unusable_departure_timestamp_raises(bad):
    with pytest.raises(RecordConversionError):
        convert_record(staged(departure_datetime=bad))


def test_required_failure_message_names_the_source_row():
    with pytest.raises(RecordConversionError) as excinfo:
        convert_record(staged(source_row_number=1753, airline=""))
    assert "1753" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Carried-through fields are lenient: no contract rule to have violated
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", CARRIED_THROUGH_FIELDS)
def test_empty_carried_through_field_becomes_null(field):
    staging_field = {
        "duration_hrs": "duration_hrs",
        "days_before_departure": "days_before_departure",
        "arrival_datetime": "arrival_datetime",
    }.get(field, field)
    row = convert_record(staged(**{staging_field: ""}))
    assert row[field] is None


@pytest.mark.parametrize(
    ("staging_field", "fact_field", "bad"),
    [
        ("arrival_datetime", "arrival_datetime", "not-a-date"),
        ("duration_hrs", "duration_hrs", "N/A"),
        ("days_before_departure", "days_before_departure", "soon"),
    ],
)
def test_unusable_carried_through_field_becomes_null_with_a_warning(
    staging_field, fact_field, bad, caplog
):
    """These columns have no rule in docs/data_contract.md, so they were never
    validated — NULL is honest, but it must leave a trace."""
    import logging

    with caplog.at_level(logging.WARNING, logger="src.transformation.converters"):
        row = convert_record(staged(**{staging_field: bad}))
    assert row[fact_field] is None
    assert staging_field in caplog.text


def test_a_broken_carried_through_field_does_not_fail_the_row():
    row = convert_record(staged(arrival_datetime="garbage", stopovers=""))
    assert row["arrival_datetime"] is None
    assert row["stopovers"] is None
    # The contract-required half of the row is untouched.
    for field in REQUIRED_FACT_FIELDS:
        assert row[field] is not None
