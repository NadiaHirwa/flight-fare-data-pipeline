"""Unit tests for Level 2/3 validation rules.

Every rule is exercised on its own against a fixture record, with no database
and no Airflow — src/validation/rules.py is pure by design, and these tests
depend on that. Quarantine writing is database behaviour and belongs to the
integration tests in docs/testing_strategy.md, not here.

The fixture is a valid row: each test mutates one field and asserts that
exactly the expected rule fires. That shape is deliberate — it catches a rule
that is too strict (the untouched fields must stay clean) as well as one that
is too loose.
"""

import pytest

from src.shared.normalization import (
    BUSINESS_KEY_FIELDS,
    compute_record_hash,
    normalize_record,
    normalize_value,
)
from src.validation.reference_data import (
    DOMESTIC_AIRPORT_CODES,
    DUPLICATE_GRAIN_KEY_FIELDS,
    INTERNATIONAL_AIRPORT_CODES,
    REQUIRED_FIELDS,
    VALID_DESTINATION_CODES,
    VALID_FARE_CLASSES,
    VALID_SEASONALITY_VALUES,
    VALID_SOURCE_CODES,
)
from src.validation.rules import (
    LEVEL_BUSINESS_RULE,
    LEVEL_DATA_QUALITY,
    DuplicateGrainKeyTracker,
    check_base_fare_non_negative,
    check_departure_datetime,
    check_destination_airport,
    check_fare_class,
    check_fares_are_numeric,
    check_required_fields,
    check_route_is_distinct,
    check_seasonality,
    check_source_airport,
    check_tax_surcharge_non_negative,
    check_total_fare_reconciles,
    validate_batch,
    validate_record,
)

# A valid staged row. Fares reconcile exactly: 21131.22 + 5169.68 = 26300.90.
VALID_RECORD = {
    "source_row_number": 1,
    "pipeline_run_id": "manual__test",
    "airline": "Biman Bangladesh Airlines",
    "source": "DAC",
    "source_name": "Hazrat Shahjalal International Airport",
    "destination": "CGP",
    "destination_name": "Shah Amanat International Airport, Chittagong",
    "departure_datetime": "2025-11-17 06:25:00",
    "arrival_datetime": "2025-11-17 07:38:10",
    "duration_hrs": "1.2195264539377717",
    "stopovers": "Direct",
    "aircraft_type": "Airbus A320",
    "fare_class": "Economy",
    "booking_source": "Online Website",
    "base_fare_bdt": "21131.22",
    "tax_surcharge_bdt": "5169.68",
    "total_fare_bdt": "26300.90",
    "seasonality": "Regular",
    "days_before_departure": "10",
}


def record_with(**overrides):
    """A copy of the valid record with specific fields replaced."""
    return {**VALID_RECORD, **overrides}


def rules_fired(record):
    """The set of rule identifiers that fire for a record, via the full pass."""
    return {violation.rule for violation in validate_record(record)}


# ---------------------------------------------------------------------------
# The reference domain itself (ADR-010)
# ---------------------------------------------------------------------------

def test_reference_domain_has_the_documented_sizes():
    """docs/data_contract.md: 8 domestic, 12 international, 20 total."""
    assert len(DOMESTIC_AIRPORT_CODES) == 8
    assert len(INTERNATIONAL_AIRPORT_CODES) == 12
    assert len(VALID_DESTINATION_CODES) == 20
    assert len(VALID_SOURCE_CODES) == 8


def test_reference_domain_matches_the_contract_verbatim():
    """Transcription check against the codes listed in docs/data_contract.md."""
    assert set(DOMESTIC_AIRPORT_CODES) == {
        "BZL", "CGP", "CXB", "DAC", "JSR", "RJH", "SPD", "ZYL",
    }
    assert set(INTERNATIONAL_AIRPORT_CODES) == {
        "BKK", "CCU", "DEL", "DOH", "DXB", "IST", "JED", "JFK",
        "KUL", "LHR", "SIN", "YYZ",
    }


def test_international_codes_are_not_valid_as_a_source():
    """The asymmetry is the point: Source is always Bangladesh-domestic."""
    assert not (INTERNATIONAL_AIRPORT_CODES & VALID_SOURCE_CODES)


def test_accepted_value_sets_match_the_contract():
    assert set(VALID_FARE_CLASSES) == {"Business", "Economy", "First Class"}
    assert set(VALID_SEASONALITY_VALUES) == {
        "Regular", "Winter Holidays", "Hajj", "Eid",
    }


def test_required_fields_are_the_contract_non_nullable_columns():
    assert set(REQUIRED_FIELDS) == {
        "airline", "source", "destination", "departure_datetime", "fare_class",
        "seasonality", "base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt",
    }


# ---------------------------------------------------------------------------
# The valid baseline
# ---------------------------------------------------------------------------

def test_valid_record_produces_no_violations():
    assert validate_record(VALID_RECORD) == []


def test_every_valid_iata_pairing_passes():
    """All 8 sources against all 20 destinations, minus same-airport pairs."""
    for source in sorted(VALID_SOURCE_CODES):
        for destination in sorted(VALID_DESTINATION_CODES):
            if source == destination:
                continue
            record = record_with(source=source, destination=destination)
            assert validate_record(record) == [], f"{source}->{destination} rejected"


@pytest.mark.parametrize("fare_class", sorted(VALID_FARE_CLASSES))
def test_every_valid_fare_class_passes(fare_class):
    assert check_fare_class(record_with(fare_class=fare_class)) == []


@pytest.mark.parametrize("season", sorted(VALID_SEASONALITY_VALUES))
def test_every_valid_seasonality_passes(season):
    assert check_seasonality(record_with(seasonality=season)) == []


# ---------------------------------------------------------------------------
# Level 2 — required fields
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", REQUIRED_FIELDS)
@pytest.mark.parametrize("empty", [None, "", "   "])
def test_each_required_field_is_checked(field, empty):
    violations = check_required_fields(record_with(**{field: empty}))
    assert len(violations) == 1
    assert violations[0].rule == "required_field_missing"
    assert violations[0].level == LEVEL_DATA_QUALITY
    assert field in violations[0].reason


def test_optional_field_may_be_empty():
    """Carried-through columns have no contract rule (data_contract.md)."""
    assert validate_record(record_with(aircraft_type="", stopovers="")) == []


def test_multiple_missing_fields_produce_one_violation_each():
    violations = check_required_fields(record_with(airline="", seasonality=""))
    assert len(violations) == 2
    reasons = " ".join(violation.reason for violation in violations)
    assert "airline" in reasons
    assert "seasonality" in reasons


# ---------------------------------------------------------------------------
# Level 2 — fare typing and sign
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["N/A", "abc", "1,234.56", "12.3.4", ""])
def test_non_numeric_fare_is_rejected(bad):
    if bad == "":
        # Empty is a missing-field problem, not a typing one.
        assert check_fares_are_numeric(record_with(base_fare_bdt=bad)) == []
        return
    violations = check_fares_are_numeric(record_with(base_fare_bdt=bad))
    assert [v.rule for v in violations] == ["fare_not_numeric"]


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_fare_is_rejected(bad):
    """Decimal parses these happily; a NaN would make every later comparison
    silently False rather than raising."""
    violations = check_fares_are_numeric(record_with(total_fare_bdt=bad))
    assert [v.rule for v in violations] == ["fare_not_numeric"]


def test_negative_base_fare_is_rejected():
    violations = check_base_fare_non_negative(record_with(base_fare_bdt="-1.00"))
    assert [v.rule for v in violations] == ["base_fare_negative"]
    assert violations[0].level == LEVEL_DATA_QUALITY


def test_negative_tax_surcharge_is_rejected():
    violations = check_tax_surcharge_non_negative(
        record_with(tax_surcharge_bdt="-0.01")
    )
    assert [v.rule for v in violations] == ["tax_surcharge_negative"]


def test_zero_fares_pass_the_sign_check():
    """docs/data_contract.md states 'value >= 0' for both fare columns, so zero
    is inside the contract. See the note raised with this implementation."""
    assert check_base_fare_non_negative(record_with(base_fare_bdt="0.00")) == []
    assert check_tax_surcharge_non_negative(record_with(tax_surcharge_bdt="0")) == []


def test_full_precision_fares_are_accepted():
    """The real file carries full float precision, e.g. 21131.22502141266."""
    record = record_with(
        base_fare_bdt="21131.22502141266",
        tax_surcharge_bdt="5169.683753211899",
        total_fare_bdt="26300.90877462456",
    )
    assert validate_record(record) == []


# ---------------------------------------------------------------------------
# Level 2 — departure timestamp
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["not-a-date", "2025-13-45 00:00:00", "17/11/2025"])
def test_unparseable_departure_timestamp_is_rejected(bad):
    violations = check_departure_datetime(record_with(departure_datetime=bad))
    assert [v.rule for v in violations] == ["departure_datetime_invalid"]


@pytest.mark.parametrize("good", ["2025-11-17 06:25:00", "2025-11-17T06:25:00"])
def test_parseable_departure_timestamps_are_accepted(good):
    assert check_departure_datetime(record_with(departure_datetime=good)) == []


def test_departure_date_outside_the_observed_range_is_accepted():
    """The contract's rule is parseability; the 2025-2026 span is an
    observation about this file, not a constraint on a future one."""
    assert check_departure_datetime(record_with(departure_datetime="2030-01-01 00:00:00")) == []


# ---------------------------------------------------------------------------
# Level 2 — IATA domain (ADR-010)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["ZZZ", "XXX", "LOL", "DA", "DACC"])
def test_unknown_source_code_is_rejected(bad):
    violations = check_source_airport(record_with(source=bad))
    assert [v.rule for v in violations] == ["source_not_in_iata_domain"]
    assert violations[0].level == LEVEL_DATA_QUALITY


@pytest.mark.parametrize("bad", ["ZZZ", "AAA", "QQQ"])
def test_unknown_destination_code_is_rejected(bad):
    violations = check_destination_airport(record_with(destination=bad))
    assert [v.rule for v in violations] == ["destination_not_in_iata_domain"]


@pytest.mark.parametrize("code", sorted(INTERNATIONAL_AIRPORT_CODES))
def test_international_code_is_rejected_as_a_source(code):
    """A real IATA code, but not a valid origin for this dataset — the check
    that would be impossible if the domain were derived from the file."""
    violations = check_source_airport(record_with(source=code))
    assert [v.rule for v in violations] == ["source_not_in_iata_domain"]


@pytest.mark.parametrize("code", sorted(INTERNATIONAL_AIRPORT_CODES))
def test_international_code_is_accepted_as_a_destination(code):
    assert check_destination_airport(record_with(destination=code)) == []


def test_city_name_instead_of_iata_code_is_rejected():
    """The assignment's 'invalid city names' requirement, concretely."""
    assert rules_fired(record_with(source="Dhaka")) == {"source_not_in_iata_domain"}


# ---------------------------------------------------------------------------
# Level 2 — categorical value sets
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad", ["Premium Economy", "economy", "ECONOMY", "Coach"])
def test_unrecognized_fare_class_is_rejected(bad):
    """Case-sensitive on purpose: the contract lists exact strings, and
    accepting 'economy' would mean the check no longer verifies the contract."""
    violations = check_fare_class(record_with(fare_class=bad))
    assert [v.rule for v in violations] == ["fare_class_not_recognized"]


@pytest.mark.parametrize("bad", ["Monsoon", "Summer", "regular", "Winter Holiday"])
def test_unrecognized_seasonality_is_rejected(bad):
    violations = check_seasonality(record_with(seasonality=bad))
    assert [v.rule for v in violations] == ["seasonality_not_recognized"]


# ---------------------------------------------------------------------------
# Level 3 — business rules
# ---------------------------------------------------------------------------

def test_source_equal_to_destination_is_rejected():
    violations = check_route_is_distinct(record_with(source="DAC", destination="DAC"))
    assert [v.rule for v in violations] == ["source_equals_destination"]
    assert violations[0].level == LEVEL_BUSINESS_RULE


def test_route_check_is_level_three_not_two():
    """Both codes are individually valid; only the combination is wrong."""
    record = record_with(source="DAC", destination="DAC")
    assert check_source_airport(record) == []
    assert check_destination_airport(record) == []
    assert rules_fired(record) == {"source_equals_destination"}


def test_total_fare_mismatch_is_rejected():
    violations = check_total_fare_reconciles(record_with(total_fare_bdt="99999.99"))
    assert [v.rule for v in violations] == ["total_fare_reconciliation"]
    assert violations[0].level == LEVEL_BUSINESS_RULE


@pytest.mark.parametrize("total", ["26301.90", "26299.90", "26300.90"])
def test_total_fare_within_tolerance_is_accepted(total):
    """Boundary: base + tax = 26300.90, tolerance is exactly 1.00."""
    assert check_total_fare_reconciles(record_with(total_fare_bdt=total)) == []


@pytest.mark.parametrize("total", ["26301.91", "26299.89"])
def test_total_fare_just_outside_tolerance_is_rejected(total):
    violations = check_total_fare_reconciles(record_with(total_fare_bdt=total))
    assert [v.rule for v in violations] == ["total_fare_reconciliation"]


def test_reconciliation_uses_exact_decimal_arithmetic():
    """0.1 + 0.2 == 0.30000000000000004 in binary floating point. With Decimal
    this reconciles exactly, which is why ADR-008 forbids float for money."""
    record = record_with(
        base_fare_bdt="0.1", tax_surcharge_bdt="0.2", total_fare_bdt="0.3"
    )
    assert check_total_fare_reconciles(record) == []


# ---------------------------------------------------------------------------
# Rule interaction
# ---------------------------------------------------------------------------

def test_a_row_can_fail_several_rules_at_once():
    """Quarantine stores one row per violation, so all of them must surface."""
    record = record_with(source="ZZZ", fare_class="Coach", total_fare_bdt="1.00")
    assert rules_fired(record) == {
        "source_not_in_iata_domain",
        "fare_class_not_recognized",
        "total_fare_reconciliation",
    }


def test_missing_fare_does_not_also_report_reconciliation():
    """One defect must not cascade into a second, misleading one."""
    assert rules_fired(record_with(base_fare_bdt="")) == {"required_field_missing"}


def test_unparseable_fare_does_not_also_report_reconciliation():
    assert rules_fired(record_with(base_fare_bdt="N/A")) == {"fare_not_numeric"}


def test_missing_source_does_not_also_report_domain_or_route():
    assert rules_fired(record_with(source="")) == {"required_field_missing"}


# ---------------------------------------------------------------------------
# Level 2 — duplicate grain key (batch-level)
# ---------------------------------------------------------------------------

def batch_rules(records):
    """Rules fired per row when validated as an ordered batch."""
    return [
        (record["source_row_number"], {v.rule for v in violations})
        for record, violations in validate_batch(records)
    ]


def test_grain_key_excludes_class():
    """docs/data_profile.md tested the grain WITHOUT Class; this key follows
    that, and is therefore stricter than the fact table's business key."""
    assert DUPLICATE_GRAIN_KEY_FIELDS == (
        "airline", "source", "destination", "departure_datetime",
    )
    assert "fare_class" not in DUPLICATE_GRAIN_KEY_FIELDS
    assert set(DUPLICATE_GRAIN_KEY_FIELDS) < set(BUSINESS_KEY_FIELDS)


def test_distinct_rows_are_not_duplicates():
    records = [
        record_with(source_row_number=1, destination="CGP"),
        record_with(source_row_number=2, destination="CXB"),
        record_with(source_row_number=3, destination="ZYL"),
    ]
    assert batch_rules(records) == [(1, set()), (2, set()), (3, set())]


def test_second_row_sharing_the_grain_key_is_quarantined():
    records = [record_with(source_row_number=1), record_with(source_row_number=2)]
    assert batch_rules(records) == [(1, set()), (2, {"duplicate_grain_key"})]


def test_first_occurrence_by_row_number_is_the_one_kept():
    records = [record_with(source_row_number=n) for n in (7, 12, 40)]
    assert batch_rules(records) == [
        (7, set()),
        (12, {"duplicate_grain_key"}),
        (40, {"duplicate_grain_key"}),
    ]


def test_rejection_reason_names_the_original_row_number():
    records = [record_with(source_row_number=7), record_with(source_row_number=99)]
    _, violations = list(validate_batch(records))[1]
    assert "7" in violations[0].reason
    assert violations[0].rule == "duplicate_grain_key"
    assert violations[0].level == LEVEL_DATA_QUALITY


def test_rows_differing_only_by_class_are_duplicates():
    """The consequence of excluding Class from the key — Phase 0 confirmed zero
    duplicates even without it, so this cannot fire on the real file."""
    records = [
        record_with(source_row_number=1, fare_class="Economy"),
        record_with(source_row_number=2, fare_class="Business"),
    ]
    assert batch_rules(records) == [(1, set()), (2, {"duplicate_grain_key"})]


@pytest.mark.parametrize("field", DUPLICATE_GRAIN_KEY_FIELDS)
def test_differing_in_any_key_field_is_not_a_duplicate(field):
    # Replacements must stay valid on every other rule, or an unrelated
    # violation would mask what this test is checking. Note source must not
    # become CGP: that is the fixture's destination, and the row would then
    # fail the Level 3 route check instead.
    replacement = {
        "airline": "US-Bangla Airlines",
        "source": "ZYL",
        "destination": "CXB",
        "departure_datetime": "2025-12-25 09:00:00",
    }[field]
    records = [
        record_with(source_row_number=1),
        record_with(source_row_number=2, **{field: replacement}),
    ]
    assert batch_rules(records) == [(1, set()), (2, set())]


def test_duplicate_detection_uses_normalized_values():
    """' dac ' and 'DAC' are the same route, so the second row is a duplicate."""
    records = [
        record_with(source_row_number=1, source="DAC"),
        record_with(source_row_number=2, source=" dac "),
    ]
    assert batch_rules(records) == [(1, set()), (2, {"duplicate_grain_key"})]


def test_incomplete_grain_key_is_not_reported_as_a_duplicate():
    """Two rows both missing an airline are not evidence of duplication — the
    missing field is the real defect and must not cascade."""
    records = [
        record_with(source_row_number=1, airline=""),
        record_with(source_row_number=2, airline=""),
    ]
    assert batch_rules(records) == [
        (1, {"required_field_missing"}),
        (2, {"required_field_missing"}),
    ]


def test_duplicate_row_can_also_fail_other_rules():
    """One quarantine row per violation, so both must surface."""
    records = [
        record_with(source_row_number=1),
        record_with(source_row_number=2, total_fare_bdt="99999.99"),
    ]
    assert batch_rules(records) == [
        (1, set()),
        (2, {"duplicate_grain_key", "total_fare_reconciliation"}),
    ]


def test_three_way_duplicate_quarantines_two_rows():
    records = [record_with(source_row_number=n) for n in (1, 2, 3)]
    fired = batch_rules(records)
    assert sum("duplicate_grain_key" in rules for _, rules in fired) == 2


def test_single_row_validation_never_reports_duplicates():
    """Without a batch, 'is this a duplicate' has no answer — validate_record
    used alone must not invent one."""
    assert "duplicate_grain_key" not in rules_fired(VALID_RECORD)


def test_tracker_is_independent_per_batch():
    """A fresh batch must not inherit keys from a previous one, or an Airflow
    retry would quarantine rows the first attempt accepted."""
    records = [record_with(source_row_number=1)]
    assert batch_rules(records) == [(1, set())]
    assert batch_rules(records) == [(1, set())]


def test_tracker_can_be_driven_directly():
    tracker = DuplicateGrainKeyTracker()
    normalized = normalize_record(VALID_RECORD)
    assert tracker.check(normalized, 1) == []
    assert [v.rule for v in tracker.check(normalized, 2)] == ["duplicate_grain_key"]


# ---------------------------------------------------------------------------
# Normalization (MASTER_PLAN Level 2 bullet)
# ---------------------------------------------------------------------------

def test_whitespace_and_casing_are_normalized_before_checks():
    """' dac ' is a formatting artifact, not an invalid airport."""
    assert validate_record(record_with(source=" dac ", destination="  cgp")) == []


def test_airline_whitespace_is_stripped_but_case_preserved():
    assert normalize_value("airline", "  Biman  ") == "Biman"
    assert normalize_value("source", " dac ") == "DAC"
    assert normalize_value("fare_class", " Economy ") == "Economy"


def test_whitespace_only_airline_is_still_missing():
    """Normalization must not rescue a field that has no content."""
    assert rules_fired(record_with(airline="   ")) == {"required_field_missing"}


def test_normalize_record_does_not_mutate_its_input():
    """Quarantine stores the original values, so the source row must survive
    validation unchanged."""
    original = record_with(source=" dac ")
    normalize_record(original)
    assert original["source"] == " dac "


# ---------------------------------------------------------------------------
# Record hash
# ---------------------------------------------------------------------------

def test_record_hash_is_deterministic_and_sha256_shaped():
    first = compute_record_hash(VALID_RECORD)
    assert first == compute_record_hash(dict(VALID_RECORD))
    assert len(first) == 64
    assert set(first) <= set("0123456789abcdef")


def test_record_hash_ignores_formatting_differences():
    """The same logical row must hash identically before and after cleanup."""
    assert compute_record_hash(record_with(source=" dac ")) == compute_record_hash(
        record_with(source="DAC")
    )


@pytest.mark.parametrize(
    "field", ["airline", "source", "destination", "departure_datetime", "fare_class"]
)
def test_record_hash_changes_with_each_business_key_field(field):
    changed = record_with(**{field: "CHANGED"})
    assert compute_record_hash(changed) != compute_record_hash(VALID_RECORD)


def test_record_hash_ignores_non_key_fields():
    """Fares are not identity: a corrected fare is the same flight offer."""
    assert compute_record_hash(record_with(total_fare_bdt="1.00")) == (
        compute_record_hash(VALID_RECORD)
    )


def test_record_hash_does_not_collide_on_field_boundaries():
    """A naive join would make ('AB','C') and ('A','BC') hash identically."""
    left = compute_record_hash(record_with(airline="AB", source="C"))
    right = compute_record_hash(record_with(airline="A", source="BC"))
    assert left != right
