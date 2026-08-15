# Data Contract

Status: **finalized**, based on Phase 0 findings (`docs/data_profile.md`).
This is the single source of truth for validation logic — code implements
this contract, not the reverse.

## Reference domain for city/route validation (ADR-010)

**Source, cited:** the official IATA airport code registry.

**Confirmed valid codes for this dataset (20 total):**
```
Bangladesh domestic (valid for Source or Destination):
  BZL (Barisal), CGP (Chittagong), CXB (Cox's Bazar), DAC (Dhaka),
  JSR (Jessore), RJH (Rajshahi), SPD (Saidpur), ZYL (Sylhet)

International hubs (valid for Destination only):
  BKK (Bangkok), CCU (Kolkata), DEL (Delhi), DOH (Doha), DXB (Dubai),
  IST (Istanbul), JED (Jeddah), JFK (New York), KUL (Kuala Lumpur),
  LHR (London), SIN (Singapore), YYZ (Toronto)
```

`Source` must be one of the 8 domestic codes. `Destination` must be one of
all 20. This is a real, independently verifiable domain (not derived from
the file's own values) — a genuinely invalid or misspelled code would still
be caught by this check.

## Columns

| column | type | nullable | business meaning | valid domain/range | validation rule | action on violation | level |
|---|---|---|---|---|---|---|---|
| Airline | VARCHAR | false | operating airline | one of 24 confirmed values (see `data_profile.md`) | non-empty | quarantine | 2 |
| Source | VARCHAR(3) | false | departure airport (IATA) | 8 Bangladesh domestic codes above | must be in domestic set | quarantine | 2 |
| Destination | VARCHAR(3) | false | arrival airport (IATA) | 20 codes above (domestic + international) | must be in full set; `!= Source` | quarantine | 2 / 3 |
| Departure Date & Time | TIMESTAMP | false | scheduled departure | 2025-01-03 to 2026-03-31 (observed range) | parseable timestamp | quarantine | 2 |
| Class | VARCHAR | false | fare class | Business, Economy, First Class | must be one of the three | quarantine | 2 |
| Seasonality | VARCHAR | false | season classification (already provided by source) | Regular, Winter Holidays, Hajj, Eid | must be one of the four | quarantine | 2 |
| Base Fare (BDT) | NUMERIC(12,2) | false | pre-tax fare | > 0 (observed min 1,600.98) | value > 0 | quarantine | 2 |
| Tax & Surcharge (BDT) | NUMERIC(12,2) | false | tax/surcharge amount | > 0 (observed min 200.00) | value > 0 | quarantine | 2 |
| Total Fare (BDT) | NUMERIC(12,2) | false | final fare | — | `abs(total - (base + tax)) <= 1.00` (fails on 4.42% of real rows — see ADR-005) | quarantine | 3 |

Other columns present in the source (`Source Name`, `Destination Name`,
`Arrival Date & Time`, `Duration (hrs)`, `Stopovers`, `Aircraft Type`,
`Booking Source`, `Days Before Departure`) are carried through to the fact
table but have no dedicated validation rule — they aren't required by the
assignment's KPIs and showed no data quality issues during profiling.