"""Names of the MySQL staging objects, and the CSV -> raw_flights column map.

Shared because more than one stage addresses these tables:

  raw_flights     written by src/ingestion/, read by src/validation/ and
                  src/transformation/
  pipeline_runs   written by all three, via src/shared/pipeline_runs.py
  COLUMN_MAP      used by src/ingestion/ to build the staging INSERT, and by
                  src/validation/ to decide which columns make up the
                  "original record" stored on a quarantine row

They lived in src/ingestion/staging_loader.py while ingestion was the only
writer. Keeping them there once validation and transformation also needed them
meant those stages imported the ingestion module purely to borrow a string,
which is the coupling this package exists to remove.

The DDL these names refer to is in include/sql/staging/.
"""

from __future__ import annotations

STAGING_TABLE = "raw_flights"
PIPELINE_RUNS_TABLE = "pipeline_runs"

# CSV header -> raw_flights column. The staging table snake_cases identifiers
# but mirrors the source column set exactly; this is the single place that
# mapping is written down (see the header comment in
# include/sql/staging/create_staging_table.sql).
COLUMN_MAP: dict[str, str] = {
    "Airline": "airline",
    "Source": "source",
    "Source Name": "source_name",
    "Destination": "destination",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "fare_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}
