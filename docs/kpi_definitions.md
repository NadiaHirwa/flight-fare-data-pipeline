# KPI Definitions

Status: **finalized**, based on Phase 0 findings (`docs/data_profile.md`).
No PENDING markers remain. Exact SQL lives in `include/sql/analytics/`.

```
Average Fare by Airline:
    AVG(total_fare) GROUP BY airline

Flight Offer Count by Airline:  (renamed from "Booking Count by Airline" — ADR-011)
    COUNT(*) GROUP BY airline
    -- Renamed because the dataset contains no booking/customer/reservation
    -- entity — each row is a flight fare quote/offer, not a booking.

Top Routes:
    route = source || '-' || destination
    metric = COUNT(*) GROUP BY route, ORDER BY metric DESC

Seasonal Fare Variation:  (resolved — no date-range derivation needed)
    AVG(total_fare) GROUP BY seasonality
    -- The dataset already provides a `Seasonality` column with four values:
    -- Regular, Winter Holidays, Hajj, Eid. Peak season = Winter Holidays,
    -- Hajj, and Eid combined; non-peak = Regular. No date-boundary logic
    -- was needed — this is a direct grouping on an existing column, which
    -- is a simpler and more reliable implementation than deriving date
    -- ranges would have been.
```

## Reconciliation sanity checks (run in `post_load_quality_check`)

```
SUM(flight_offer_count_by_airline) = total valid fact rows
SUM(top_routes counts)             = total valid fact rows
per airline: MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)
```

These catch a KPI table that exists and is non-null but is logically wrong —
a different failure class than a missing table or a null value.
