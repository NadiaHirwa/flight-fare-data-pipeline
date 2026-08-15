"""Staging strings -> the fact table's declared types.

This is pure type conversion, deliberately not re-validation. Every row
reaching this module has already passed Levels 2 and 3 (src/validation/), so
re-checking the contract here would duplicate logic that already has a single
home and could drift from it. The Total Fare reconciliation in particular is
Level 3's job and is not repeated: total_fare is carried across as the source
stated it, converted, not recomputed from base + tax.

Types come from docs/data_contract.md's Columns table and must match
include/sql/analytics/create_fact_table.sql exactly.

Standard library plus src/shared only — no database — so every conversion is
unit-testable without infrastructure.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from ..shared.normalization import compute_record_hash, normalize_record
from .exceptions import RecordConversionError

logger = logging.getLogger(__name__)

# Insert order for flight_fare_quotes. Kept as one tuple so the INSERT
# statement and the converted row dict cannot disagree about column order.
FACT_COLUMNS: tuple[str, ...] = (
    "airline",
    "source",
    "destination",
    "departure_datetime",
    "fare_class",
    "seasonality",
    "base_fare",
    "tax_surcharge",
    "total_fare",
    "source_name",
    "destination_name",
    "arrival_datetime",
    "duration_hrs",
    "stopovers",
    "aircraft_type",
    "booking_source",
    "days_before_departure",
    "source_record_hash",
)

# NUMERIC(12,2) for money (ADR-008 — never FLOAT), NUMERIC(5,2) for duration.
MONEY_EXPONENT = Decimal("0.01")
DURATION_EXPONENT = Decimal("0.01")

# ROUND_HALF_UP, not Python's default ROUND_HALF_EVEN. Banker's rounding is the
# right default for repeated statistical aggregation but is not what a fare is
# expected to do: 0.005 rounding down half the time reads as a bug to anyone
# reconciling a fare by hand. ADR-008 fixes the type, not the rounding mode, so
# this is stated here rather than assumed.
#
# Rounding cannot push a row past the fact table's reconciliation CHECK. Each
# of the three money columns moves by at most 0.005, so rounding ADDS at most
# 0.015 to whatever drift the source row already carried. Level 3 has already
# confirmed that source drift is within 1.00, and 0.015 of headroom against a
# 1.00 tolerance is not close. (Note this bounds the drift rounding introduces,
# not the absolute drift — the source's own drift is the larger term.)
MONEY_ROUNDING = ROUND_HALF_UP


def convert_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Convert one validated staging row into a typed fact row.

    Normalizes first, so the values stored in the fact table are the same ones
    the hash was computed over — that is what lets a quarantined row be joined
    against the fact table on source_record_hash and either found or provably
    absent.

    Strictness is split along the contract:

      * the nine nullable=false columns raise RecordConversionError if they
        cannot be converted. Validation guarantees they are convertible, so a
        failure means a row bypassed validation — a fault, not bad data.
      * the eight carried-through columns become NULL with a warning. They have
        no contract rule (docs/data_contract.md's closing paragraph) and are
        nullable in the fact table, so there is no rule to have violated. The
        warning exists so a silent NULL still leaves a trace.

    Returns a dict keyed by FACT_COLUMNS, ready to bind to the INSERT.
    """
    normalized = normalize_record(record)
    row_number = record.get("source_row_number")

    return {
        # --- contract-required: strict ---
        "airline": _required_text(normalized, "airline", row_number),
        "source": _required_text(normalized, "source", row_number),
        "destination": _required_text(normalized, "destination", row_number),
        "departure_datetime": _required_timestamp(
            normalized, "departure_datetime", row_number
        ),
        "fare_class": _required_text(normalized, "fare_class", row_number),
        "seasonality": _required_text(normalized, "seasonality", row_number),
        "base_fare": _required_money(normalized, "base_fare_bdt", row_number),
        "tax_surcharge": _required_money(
            normalized, "tax_surcharge_bdt", row_number
        ),
        # Carried across as stated, NOT recomputed from base + tax. Level 3
        # already confirmed the two agree within 1.00; recomputing here would
        # silently rewrite the source's number and destroy the evidence that
        # the check was ever meaningful.
        "total_fare": _required_money(normalized, "total_fare_bdt", row_number),
        # --- carried through: lenient, nullable ---
        "source_name": _optional_text(normalized, "source_name"),
        "destination_name": _optional_text(normalized, "destination_name"),
        "arrival_datetime": _optional_timestamp(
            normalized, "arrival_datetime", row_number
        ),
        "duration_hrs": _optional_decimal(
            normalized, "duration_hrs", DURATION_EXPONENT, row_number
        ),
        "stopovers": _optional_text(normalized, "stopovers"),
        "aircraft_type": _optional_text(normalized, "aircraft_type"),
        "booking_source": _optional_text(normalized, "booking_source"),
        "days_before_departure": _optional_int(
            normalized, "days_before_departure", row_number
        ),
        # Computed from the source row via the shared implementation, so it is
        # identical by construction to the digest src/validation/ wrote onto
        # any quarantine row for the same logical record.
        "source_record_hash": compute_record_hash(record),
    }


def convert_records(
    records: Any,
) -> Any:
    """Convert an iterable of staging rows, streaming rather than materializing."""
    for record in records:
        yield convert_record(record)


# ---------------------------------------------------------------------------
# Required conversions — a failure here is a fault, not bad data
# ---------------------------------------------------------------------------

def _required_text(record: Mapping[str, str], field: str, row_number: Any) -> str:
    value = record.get(field, "")
    if value == "":
        raise RecordConversionError(
            f"Required field {field!r} is empty on source row {row_number}. "
            "Validation should have quarantined this row."
        )
    return value


def _required_money(
    record: Mapping[str, str], field: str, row_number: Any
) -> Decimal:
    amount = _parse_decimal(record.get(field, ""))
    if amount is None:
        raise RecordConversionError(
            f"Required money field {field!r} is not a usable number "
            f"({record.get(field)!r}) on source row {row_number}. "
            "Validation should have quarantined this row."
        )
    return amount.quantize(MONEY_EXPONENT, rounding=MONEY_ROUNDING)


def _required_timestamp(
    record: Mapping[str, str], field: str, row_number: Any
) -> datetime:
    parsed = _parse_timestamp(record.get(field, ""))
    if parsed is None:
        raise RecordConversionError(
            f"Required timestamp {field!r} is not parseable "
            f"({record.get(field)!r}) on source row {row_number}. "
            "Validation should have quarantined this row."
        )
    return parsed


# ---------------------------------------------------------------------------
# Optional conversions — no contract rule, so NULL with a warning
# ---------------------------------------------------------------------------

def _optional_text(record: Mapping[str, str], field: str) -> str | None:
    value = record.get(field, "")
    return value or None


def _optional_timestamp(
    record: Mapping[str, str], field: str, row_number: Any
) -> datetime | None:
    raw = record.get(field, "")
    if raw == "":
        return None
    parsed = _parse_timestamp(raw)
    if parsed is None:
        _warn_unusable(field, raw, row_number)
    return parsed


def _optional_decimal(
    record: Mapping[str, str], field: str, exponent: Decimal, row_number: Any
) -> Decimal | None:
    raw = record.get(field, "")
    if raw == "":
        return None
    parsed = _parse_decimal(raw)
    if parsed is None:
        _warn_unusable(field, raw, row_number)
        return None
    return parsed.quantize(exponent, rounding=MONEY_ROUNDING)


def _optional_int(
    record: Mapping[str, str], field: str, row_number: Any
) -> int | None:
    raw = record.get(field, "")
    if raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        # Accept "10.0" as 10: the source writes whole numbers, but a
        # re-export through a tool that types everything as float would
        # otherwise lose a column that is perfectly usable.
        parsed = _parse_decimal(raw)
        if parsed is not None and parsed == parsed.to_integral_value():
            return int(parsed)
        _warn_unusable(field, raw, row_number)
        return None


def _warn_unusable(field: str, raw: str, row_number: Any) -> None:
    logger.warning(
        "Source row %s: carried-through field %r value %r is not convertible; "
        "storing NULL. This column has no rule in docs/data_contract.md, so it "
        "was never validated.",
        row_number,
        field,
        raw,
    )


# ---------------------------------------------------------------------------
# Parsing primitives
# ---------------------------------------------------------------------------

def _parse_decimal(value: str) -> Decimal | None:
    """Parse to Decimal, or None if unusable.

    Decimal, never float — ADR-008. is_finite() matters because Decimal("NaN")
    and Decimal("Infinity") both parse, and a NaN would reach the database as
    a value no comparison can order.
    """
    if value == "":
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError, ArithmeticError):
        return None
    return parsed if parsed.is_finite() else None


def _parse_timestamp(value: str) -> datetime | None:
    """Parse an ISO-8601 timestamp, or None if unusable.

    The source format is "2025-11-17 06:25:00"; fromisoformat accepts both that
    and the "T"-separated spelling. No timezone is attached: the fact table
    column is TIMESTAMP WITHOUT TIME ZONE because a scheduled departure is a
    wall-clock time at the origin airport, and coercing it to UTC would invent
    information the source does not carry.
    """
    if value == "":
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
