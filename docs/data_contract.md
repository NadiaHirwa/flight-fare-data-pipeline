# Data Contract

Status: **pending Phase 0** (`docs/data_profile.md`). This is the single source
of truth for validation logic — code implements this contract, not the reverse.

## Reference domain for city/route validation (ADR-010)

Source cited here once established — see Implementation Reminders in
`docs/MASTER_PLAN.md`. Do not derive this domain from the CSV being validated.

## Columns

| column | type | nullable | business meaning | valid domain/range | validation rule | action on violation | level |
|---|---|---|---|---|---|---|---|
| Airline | | | | | | | |
| Source | | | | | | | |
| Destination | | | | | | | |
| Base Fare | NUMERIC(12,2) | false | | >= 0 | | quarantine | 2 |
| Tax & Surcharge | NUMERIC(12,2) | false | | >= 0 | | quarantine | 2 |
| Total Fare | NUMERIC(12,2) | false | | | abs(total - (base+tax)) <= tolerance | quarantine | 3 |

Fill in remaining cells after Phase 0. Add rows for any additional columns
profiling reveals (e.g. a date field, if one exists).
