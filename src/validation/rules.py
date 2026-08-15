"""Level 2 and Level 3 row-level validation rules (ADR-003).

No database, no Airflow, no I/O. Each rule is independently callable and
independently tested, and every rule implements a line that is already written
in docs/data_contract.md or docs/data_profile.md — the documented contract is
the source of truth, the code follows it.

Level 2 — data quality:
    required_field_missing        the contract's nullable=false columns
    fare_not_numeric              a fare that will not parse as a number
    base_fare_not_positive        Base Fare rule "value > 0"
    tax_surcharge_not_positive    Tax & Surcharge rule "value > 0"
    departure_datetime_invalid    "parseable timestamp"
    source_not_in_iata_domain     the 8 domestic codes (ADR-010)
    destination_not_in_iata_domain  all 20 codes (ADR-010)
    fare_class_not_recognized     Business / Economy / First Class
    seasonality_not_recognized    Regular / Winter Holidays / Hajj / Eid
    duplicate_grain_key           batch-level; see DuplicateGrainKeyTracker

Level 3 — business rules:
    source_equals_destination     Destination "!= Source"
    total_fare_reconciliation     abs(total - (base + tax)) <= 1.00

All rules but one are pure functions of a single record. Duplicate detection
is inherently batch-level — a row is only a duplicate relative to the rest of
the batch — so it is a small stateful accumulator (DuplicateGrainKeyTracker)
rather than a function, and validate_batch is the entry point that drives it.

A row that fails several rules produces several violations, and quarantine
stores one row per violation — a row is never silently reduced to its first
problem, because the point of quarantine is to explain the row completely.

Checks that depend on a value that is missing or unparseable return no
violation of their own: the missing/unparseable rule has already reported the
real problem, and adding "total fare does not reconcile with None" underneath
it would turn one defect into two misleading ones.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

from ..shared.normalization import normalize_record
from .reference_data import (
    DUPLICATE_GRAIN_KEY_FIELDS,
    REQUIRED_FIELDS,
    TOTAL_FARE_TOLERANCE,
    VALID_DESTINATION_CODES,
    VALID_FARE_CLASSES,
    VALID_SEASONALITY_VALUES,
    VALID_SOURCE_CODES,
)

FARE_FIELDS: tuple[str, ...] = ("base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt")

LEVEL_DATA_QUALITY = 2
LEVEL_BUSINESS_RULE = 3


@dataclass(frozen=True)
class Violation:
    """One rule failure for one row — becomes exactly one quarantine row.

    Field names mirror the quarantine columns they populate, so the mapping
    from a rule firing to a row landing in the table is direct:

        rule    -> quarantine.rule_violated     (stable, machine-readable,
                                                 safe to GROUP BY)
        level   -> quarantine.validation_level  (2 or 3)
        reason  -> quarantine.rejection_reason  (human-readable, names the
                                                 offending value)
    """

    rule: str
    level: int
    reason: str


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_decimal(value: str) -> Decimal | None:
    """Parse a fare, or None if it is not a usable finite number.

    Decimal, never float: these values feed the 1.00 reconciliation tolerance,
    and binary floating point would put rounding error inside the very check
    that exists to detect arithmetic that does not add up (ADR-008).

    is_finite() matters more than it looks — Decimal("NaN") and
    Decimal("Infinity") both parse successfully, and a NaN would silently make
    every downstream comparison False rather than raising.
    """
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError, ArithmeticError):
        return None
    return parsed if parsed.is_finite() else None


def _is_blank(value: object) -> bool:
    return value is None or str(value).strip() == ""


# ---------------------------------------------------------------------------
# Level 2 — data quality
# ---------------------------------------------------------------------------

def check_required_fields(record: Mapping[str, str]) -> list[Violation]:
    """Every nullable=false column in the contract must be present and non-empty.

    One violation per missing field rather than one for the row: a row missing
    three fields is three distinct facts, and collapsing them would hide two.

    Phase 0 found zero nulls anywhere (docs/data_profile.md), so this rule is
    not expected to fire on the current file. It stays because the contract
    declares these columns non-nullable, and a check that only exists once it
    has already failed in production is a check written too late.
    """
    return [
        Violation(
            rule="required_field_missing",
            level=LEVEL_DATA_QUALITY,
            reason=f"Required field '{field}' is null or empty.",
        )
        for field in REQUIRED_FIELDS
        if _is_blank(record.get(field))
    ]


def check_fares_are_numeric(record: Mapping[str, str]) -> list[Violation]:
    """Each fare must parse as a finite number.

    Staging is deliberately all-VARCHAR (a raw landing zone preserves what the
    file said), so this is the first point at which a fare's type is actually
    established. Without it, a fare of "N/A" would raise inside a later
    comparison and fail the task instead of quarantining the one bad row.
    """
    violations = []
    for field in FARE_FIELDS:
        value = record.get(field)
        if _is_blank(value):
            continue  # already reported by check_required_fields
        if _parse_decimal(str(value)) is None:
            violations.append(
                Violation(
                    rule="fare_not_numeric",
                    level=LEVEL_DATA_QUALITY,
                    reason=f"Field '{field}' value {value!r} is not a valid number.",
                )
            )
    return violations


def check_base_fare_is_positive(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Base Fare (BDT): "value > 0".

    Zero is rejected, not only negatives — which is why the rule identifier is
    base_fare_not_positive rather than base_fare_negative. A fare of nothing is
    not a fare, and MASTER_PLAN's Level 2 list has always said "negative or
    zero fares"; the contract now agrees.
    """
    return _check_fare_is_positive(record, "base_fare_bdt", "base_fare_not_positive")


def check_tax_surcharge_is_positive(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Tax & Surcharge (BDT): "value > 0"."""
    return _check_fare_is_positive(
        record, "tax_surcharge_bdt", "tax_surcharge_not_positive"
    )


def _check_fare_is_positive(
    record: Mapping[str, str], field: str, rule: str
) -> list[Violation]:
    value = record.get(field)
    if _is_blank(value):
        return []
    amount = _parse_decimal(str(value))
    if amount is None:
        return []  # already reported by check_fares_are_numeric
    if amount <= 0:
        return [
            Violation(
                rule=rule,
                level=LEVEL_DATA_QUALITY,
                reason=(
                    f"Field '{field}' is {amount}; contract requires a positive "
                    "value (> 0)."
                ),
            )
        ]
    return []


def check_departure_datetime(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Departure Date & Time: "parseable timestamp".

    No range check: the contract's rule is parseability. The 2025-01-03 to
    2026-03-31 span in the contract is an observation about this file, not a
    constraint on future ones, and enforcing it would reject a legitimately
    newer extract.
    """
    value = record.get("departure_datetime")
    if _is_blank(value):
        return []  # already reported by check_required_fields

    text = str(value).strip()
    try:
        datetime.fromisoformat(text)
    except ValueError:
        return [
            Violation(
                rule="departure_datetime_invalid",
                level=LEVEL_DATA_QUALITY,
                reason=f"Departure timestamp {text!r} is not a parseable timestamp.",
            )
        ]
    return []


def check_source_airport(record: Mapping[str, str]) -> list[Violation]:
    """Source must be one of the 8 Bangladesh domestic IATA codes (ADR-010).

    Checked against the transcribed IATA registry list in reference_data.py,
    never against the distinct values found in the file being validated — a
    file compared to itself cannot fail.
    """
    value = record.get("source")
    if _is_blank(value):
        return []
    code = str(value)
    if code not in VALID_SOURCE_CODES:
        return [
            Violation(
                rule="source_not_in_iata_domain",
                level=LEVEL_DATA_QUALITY,
                reason=(
                    f"Source {code!r} is not one of the "
                    f"{len(VALID_SOURCE_CODES)} Bangladesh domestic IATA codes "
                    "in docs/data_contract.md."
                ),
            )
        ]
    return []


def check_destination_airport(record: Mapping[str, str]) -> list[Violation]:
    """Destination must be one of the 20 confirmed IATA codes (ADR-010)."""
    value = record.get("destination")
    if _is_blank(value):
        return []
    code = str(value)
    if code not in VALID_DESTINATION_CODES:
        return [
            Violation(
                rule="destination_not_in_iata_domain",
                level=LEVEL_DATA_QUALITY,
                reason=(
                    f"Destination {code!r} is not one of the "
                    f"{len(VALID_DESTINATION_CODES)} IATA codes "
                    "in docs/data_contract.md."
                ),
            )
        ]
    return []


def check_fare_class(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Class: "must be one of the three"."""
    return _check_accepted_values(
        record,
        field="fare_class",
        accepted=VALID_FARE_CLASSES,
        rule="fare_class_not_recognized",
        label="Class",
    )


def check_seasonality(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Seasonality: "must be one of the four"."""
    return _check_accepted_values(
        record,
        field="seasonality",
        accepted=VALID_SEASONALITY_VALUES,
        rule="seasonality_not_recognized",
        label="Seasonality",
    )


def _check_accepted_values(
    record: Mapping[str, str],
    field: str,
    accepted: frozenset[str],
    rule: str,
    label: str,
) -> list[Violation]:
    value = record.get(field)
    if _is_blank(value):
        return []
    text = str(value)
    if text not in accepted:
        return [
            Violation(
                rule=rule,
                level=LEVEL_DATA_QUALITY,
                reason=(
                    f"{label} {text!r} is not one of {sorted(accepted)} "
                    "per docs/data_contract.md."
                ),
            )
        ]
    return []


# ---------------------------------------------------------------------------
# Level 3 — business rules
# ---------------------------------------------------------------------------

def check_route_is_distinct(record: Mapping[str, str]) -> list[Violation]:
    """docs/data_contract.md, Destination: "!= Source" — a flight cannot end
    where it started. Level 3: both codes are individually valid, the
    combination is not."""
    source = record.get("source")
    destination = record.get("destination")
    if _is_blank(source) or _is_blank(destination):
        return []
    if str(source) == str(destination):
        return [
            Violation(
                rule="source_equals_destination",
                level=LEVEL_BUSINESS_RULE,
                reason=(
                    f"Source and Destination are both {str(source)!r}; "
                    "a route must connect two different airports."
                ),
            )
        ]
    return []


def check_total_fare_reconciles(record: Mapping[str, str]) -> list[Violation]:
    """abs(total - (base + tax)) <= 1.00, per docs/data_contract.md.

    This is the dataset's one real data quality issue: Phase 0 measured it
    failing on 4.42% of rows, with differences from ~445 to ~93,165 BDT spread
    evenly across every Seasonality and Class — injected noise, not a
    systematic calculation bug (docs/data_profile.md). It is also what sets the
    6% quality gate in ADR-005.

    The tolerance covers 2dp rounding only. Real violations here are orders of
    magnitude larger, so widening it would not rescue any genuine row.
    """
    values = {}
    for field in FARE_FIELDS:
        raw = record.get(field)
        if _is_blank(raw):
            return []  # missing fare already reported
        parsed = _parse_decimal(str(raw))
        if parsed is None:
            return []  # unparseable fare already reported
        values[field] = parsed

    expected = values["base_fare_bdt"] + values["tax_surcharge_bdt"]
    difference = abs(values["total_fare_bdt"] - expected)
    if difference > Decimal(TOTAL_FARE_TOLERANCE):
        return [
            Violation(
                rule="total_fare_reconciliation",
                level=LEVEL_BUSINESS_RULE,
                reason=(
                    f"Total Fare {values['total_fare_bdt']} does not reconcile with "
                    f"Base {values['base_fare_bdt']} + Tax "
                    f"{values['tax_surcharge_bdt']} = {expected} "
                    f"(difference {difference} exceeds tolerance "
                    f"{TOTAL_FARE_TOLERANCE})."
                ),
            )
        ]
    return []


# ---------------------------------------------------------------------------
# Registry and entry point
# ---------------------------------------------------------------------------

# Order is presentation only — every rule runs regardless, so a row's full set
# of problems is reported in one pass. Listed Level 2 first to match ADR-003.
ALL_RULES = (
    check_required_fields,
    check_fares_are_numeric,
    check_base_fare_is_positive,
    check_tax_surcharge_is_positive,
    check_departure_datetime,
    check_source_airport,
    check_destination_airport,
    check_fare_class,
    check_seasonality,
    check_route_is_distinct,
    check_total_fare_reconciles,
)


class DuplicateGrainKeyTracker:
    """Level 2, batch-level: at most one row per grain key, keep the first.

    Grain key is (Airline, Source, Destination, Departure Date & Time) — the
    exact composite Phase 0 tested, Class excluded (see
    DUPLICATE_GRAIN_KEY_FIELDS). Phase 0 found zero duplicates on it across all
    57,000 rows, so this is not expected to fire on the current file; it exists
    because "no duplicates today" is an observation, not a guarantee, and a
    duplicated quote would silently inflate every COUNT-based KPI.

    Policy: the lowest source_row_number for a key survives, every later row
    sharing it is quarantined. Keeping the first occurrence rather than the
    last means the surviving row is the one nearest the top of the source file,
    which is what a human reconciling against the CSV would expect.

    ORDERING IS LOAD-BEARING. "First" means first *seen*, so records must
    arrive in ascending source_row_number order for that to equal the lowest
    row number. _iter_staged_rows guarantees this with ORDER BY
    source_row_number; a streaming check cannot retroactively un-quarantine a
    row if a lower-numbered one turns up later.

    Compared by normalized value, so " dac " and "DAC" are recognised as the
    same route rather than as two distinct keys.
    """

    def __init__(self) -> None:
        self._first_occurrence: dict[tuple[str, ...], int] = {}

    def check(
        self, normalized: Mapping[str, str], source_row_number: int
    ) -> list[Violation]:
        key = tuple(
            normalized.get(field, "") for field in DUPLICATE_GRAIN_KEY_FIELDS
        )

        # An incomplete key cannot identify a row, and two rows both missing
        # an airline are not evidence of duplication. required_field_missing
        # has already reported the real defect; adding a spurious duplicate
        # violation on top would be the cascade the other rules avoid.
        if any(part == "" for part in key):
            return []

        first_seen = self._first_occurrence.get(key)
        if first_seen is None:
            self._first_occurrence[key] = source_row_number
            return []

        return [
            Violation(
                rule="duplicate_grain_key",
                level=LEVEL_DATA_QUALITY,
                reason=(
                    f"Duplicate of source row {first_seen} on grain key "
                    f"(Airline, Source, Destination, Departure Date & Time) = "
                    f"{key}. First occurrence is kept; this row is rejected."
                ),
            )
        ]


def validate_record(
    record: Mapping[str, object],
    duplicate_tracker: DuplicateGrainKeyTracker | None = None,
) -> list[Violation]:
    """Run every Level 2/3 rule against one staging row.

    Normalizes first (MASTER_PLAN's Level 2 bullet), then applies every rule to
    the normalized copy. Returns an empty list for a valid row.

    The caller keeps the original, un-normalized record: that is what belongs
    in quarantine.original_record.

    duplicate_tracker is optional so a single row can still be validated in
    isolation — without a batch to compare against, "is this a duplicate" is
    not a question that has an answer. Pass one (or use validate_batch) to
    include the batch-level check.
    """
    normalized = normalize_record(record)
    violations: list[Violation] = []
    for rule in ALL_RULES:
        violations.extend(rule(normalized))

    if duplicate_tracker is not None:
        violations.extend(
            duplicate_tracker.check(normalized, record.get("source_row_number"))
        )
    return violations


def validate_batch(
    records: Iterable[Mapping[str, object]],
) -> Iterator[tuple[Mapping[str, object], list[Violation]]]:
    """Validate an ordered stream of rows, including the batch-level check.

    Yields (record, violations) pairs, streaming rather than materializing:
    the caller writes quarantine rows as it goes, so memory stays bounded by
    the distinct grain keys seen rather than by the size of the batch.

    Records must arrive in ascending source_row_number order — see
    DuplicateGrainKeyTracker.
    """
    tracker = DuplicateGrainKeyTracker()
    for record in records:
        yield record, validate_record(record, tracker)
