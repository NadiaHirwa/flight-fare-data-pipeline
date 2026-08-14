# KPI Definitions

Status: finalized pending Phase 0 grain and date-column confirmation
(`docs/data_profile.md`). Exact SQL lives in `include/sql/analytics/`.

```
Average Fare by Airline:
    AVG(total_fare) GROUP BY airline

Booking Count by Airline:
    COUNT(*) GROUP BY airline
    -- Name/meaning depends on Phase 0 grain finding — see ADR and
    -- data_profile.md. If rows are fare quotes rather than bookings,
    -- this KPI and its table are renamed accordingly.

Top Routes:
    route = source || '-' || destination
    metric = COUNT(*) GROUP BY route, ORDER BY metric DESC

Seasonal Fare Variation:
    PENDING — requires a confirmed date field (see data_profile.md).
    Peak season boundaries (e.g. Eid, winter holidays) will be defined as
    explicit date ranges once a date column is confirmed to exist. If no
    date field exists, this KPI is either redefined on the best available
    proxy (documented as a deviation) or explicitly marked out of scope
    with justification in final_report.md.
```

## Reconciliation sanity checks (run in `post_load_quality_check`)

```
SUM(booking_count_by_airline) = total valid fact rows
SUM(top_routes counts)        = total valid fact rows
per airline: MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)
```

These catch a KPI table that exists and is non-null but is logically wrong —
a different failure class than a missing table or a null value.
