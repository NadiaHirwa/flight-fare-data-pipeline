"""Reference domains for validation, transcribed from docs/data_contract.md.

Every constant here is a direct transcription of a value set already written
down in the contract. Nothing in this module is derived from the CSV being
validated — that distinction is the entire point of ADR-010: deriving the
"valid" domain from the same file you are checking is circular and produces a
check that can never fail.

The contract is the single source of truth; if a value set needs to change, it
changes in docs/data_contract.md first and is transcribed here second.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Airport codes — docs/data_contract.md, "Reference domain for city/route
# validation (ADR-010)". Cited source: the official IATA airport code registry.
# ---------------------------------------------------------------------------

# "Bangladesh domestic (valid for Source or Destination)"
DOMESTIC_AIRPORT_CODES: frozenset[str] = frozenset(
    {
        "BZL",  # Barisal
        "CGP",  # Chittagong
        "CXB",  # Cox's Bazar
        "DAC",  # Dhaka
        "JSR",  # Jessore
        "RJH",  # Rajshahi
        "SPD",  # Saidpur
        "ZYL",  # Sylhet
    }
)

# "International hubs (valid for Destination only)"
INTERNATIONAL_AIRPORT_CODES: frozenset[str] = frozenset(
    {
        "BKK",  # Bangkok
        "CCU",  # Kolkata
        "DEL",  # Delhi
        "DOH",  # Doha
        "DXB",  # Dubai
        "IST",  # Istanbul
        "JED",  # Jeddah
        "JFK",  # New York
        "KUL",  # Kuala Lumpur
        "LHR",  # London
        "SIN",  # Singapore
        "YYZ",  # Toronto
    }
)

# "`Source` must be one of the 8 domestic codes. `Destination` must be one of
# all 20."
VALID_SOURCE_CODES: frozenset[str] = DOMESTIC_AIRPORT_CODES
VALID_DESTINATION_CODES: frozenset[str] = (
    DOMESTIC_AIRPORT_CODES | INTERNATIONAL_AIRPORT_CODES
)

# ---------------------------------------------------------------------------
# Categorical value sets — docs/data_contract.md, Columns table.
# ---------------------------------------------------------------------------

# Class: "Business, Economy, First Class"
VALID_FARE_CLASSES: frozenset[str] = frozenset({"Business", "Economy", "First Class"})

# Seasonality: "Regular, Winter Holidays, Hajj, Eid"
VALID_SEASONALITY_VALUES: frozenset[str] = frozenset(
    {"Regular", "Winter Holidays", "Hajj", "Eid"}
)

# ---------------------------------------------------------------------------
# Required fields — the contract's nullable=false columns, named by their
# staging (raw_flights) column names.
# ---------------------------------------------------------------------------

REQUIRED_FIELDS: tuple[str, ...] = (
    "airline",
    "source",
    "destination",
    "departure_datetime",
    "fare_class",
    "seasonality",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_bdt",
)

# ---------------------------------------------------------------------------
# Grain — docs/data_profile.md, "Grain and identity"
# ---------------------------------------------------------------------------

# The composite key Phase 0 actually tested: "Confirmed by checking for
# duplicate (Airline, Source, Destination, Departure Date & Time) combinations
# across all 57,000 rows: zero duplicates found, even without including Class."
#
# Class is deliberately excluded, matching how the grain was tested rather than
# how ADR-011 describes the fact table's grain. That makes this the stricter of
# the two keys: any pair of rows colliding on the five-field business key in
# src/shared/normalization.py necessarily collides on these four as well, so
# duplicate detection here cannot let through a pair that would later violate
# the fact table's UNIQUE source_record_hash constraint.
DUPLICATE_GRAIN_KEY_FIELDS: tuple[str, ...] = (
    "airline",
    "source",
    "destination",
    "departure_datetime",
)

# ---------------------------------------------------------------------------
# Tolerances
# ---------------------------------------------------------------------------

# docs/data_contract.md, Total Fare: "abs(total - (base + tax)) <= 1.00".
# A string, not a float literal: Decimal("1.00") is exactly one, whereas
# Decimal(1.00) inherits binary floating-point error — the precise failure
# ADR-008 exists to prevent, and it would sit inside the tolerance check itself.
TOTAL_FARE_TOLERANCE = "1.00"
