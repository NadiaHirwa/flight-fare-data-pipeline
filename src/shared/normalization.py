"""Record normalization and the deterministic record hash.

Shared because two stages need the identical implementation:

  src/validation/    normalizes before every Level 2/3 check, and writes the
                     record hash onto each quarantine row.
  src/transformation/  normalizes before loading, and writes the same hash into
                     analytics.flight_fare_quotes.source_record_hash, which
                     carries a UNIQUE constraint.

If those two ever computed the digest differently, the quarantine table could
no longer be joined against the fact table to prove a rejected row never
landed, and the idempotency safety net in docs/MASTER_PLAN.md would be
comparing two different things. One implementation, imported by both.

Normalization runs before every check, per the Level 2 bullet in
docs/MASTER_PLAN.md: "Whitespace/casing normalization on Airline/Source/
Destination before the above checks run". Phase 0 found this data already
clean (docs/data_profile.md), so on today's file it is a no-op — it is here so
that " dac " from a future file is recognised as DAC rather than quarantined
for a formatting artifact.

Normalization is applied to the *copy of the row being checked*, never to what
gets written to quarantine. The quarantine table records what the source
actually said (see include/sql/staging/create_quarantine_table.sql), which is
the whole reason it stores original_record.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping

# Fields whose values identify a row for the fact table's UNIQUE
# source_record_hash. Phase 0 confirmed zero duplicates on (Airline, Source,
# Destination, Departure Date & Time) across all 57,000 rows; fare_class is
# included here because the fact table's grain is explicitly "airline x route x
# departure datetime x class" (ADR-011).
#
# Not to be confused with DUPLICATE_GRAIN_KEY_FIELDS in
# src/validation/reference_data.py, which deliberately excludes fare_class to
# match how Phase 0 actually tested the grain. That key is the stricter of the
# two: any pair of rows colliding on these five fields necessarily collides on
# those four, so duplicate detection cannot let through a pair that would then
# violate this hash's UNIQUE constraint downstream.
BUSINESS_KEY_FIELDS: tuple[str, ...] = (
    "airline",
    "source",
    "destination",
    "departure_datetime",
    "fare_class",
)

# Fields normalized to uppercase: IATA codes are canonically uppercase, and the
# reference domain in src/validation/reference_data.py is written that way.
_UPPERCASE_FIELDS = frozenset({"source", "destination"})

# Field separator for the hash input. \x1f (ASCII unit separator) cannot occur
# in this data, so "AB" + "C" and "A" + "BC" cannot collide into the same
# digest the way a comma or pipe separator would allow.
_HASH_SEPARATOR = "\x1f"


def normalize_value(field: str, value: object) -> str:
    """Normalize one field's value to the form the checks expect.

    Whitespace is stripped from everything; only IATA code fields are
    case-folded. Airline, fare_class and seasonality keep their casing on
    purpose: the contract's value sets are exact strings ("First Class",
    "Winter Holidays"), and silently accepting "economy" would mean the
    accepted-value check no longer verifies what the contract says it does.
    """
    if value is None:
        return ""
    text = str(value).strip()
    if field in _UPPERCASE_FIELDS:
        text = text.upper()
    return text


def normalize_record(record: Mapping[str, object]) -> dict[str, str]:
    """Return a normalized copy of a staging row. The input is not mutated."""
    return {field: normalize_value(field, value) for field, value in record.items()}


def compute_record_hash(record: Mapping[str, object]) -> str:
    """SHA-256 over the row's normalized business-key fields.

    Deterministic across runs and across processes: the same logical row always
    produces the same digest, which is what lets the fact table's UNIQUE
    constraint act as the idempotency safety net described in
    docs/MASTER_PLAN.md, and what makes a quarantined row joinable to its fact
    row (or provably absent from it).

    Normalizes internally rather than trusting the caller, so a row hashed
    before validation and the same row hashed after transformation agree.
    """
    parts = [
        normalize_value(field, record.get(field)) for field in BUSINESS_KEY_FIELDS
    ]
    return hashlib.sha256(_HASH_SEPARATOR.join(parts).encode("utf-8")).hexdigest()
