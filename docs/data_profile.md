# Data Profile — Phase 0 Findings

Status: **not yet started.** Nothing in `docs/data_contract.md`, the SQL DDL files,
or the DAG task bodies should be finalized until this document is filled in
against the actual `Flight_Price_Dataset_of_Bangladesh.csv`.

## Grain and identity

- What does one row represent?
- Natural or composite unique identifier, if any:
- Are duplicate rows legitimate or defects?

## Schema reality check

- Full column inventory (beyond the six named in the assignment):
- **Does a travel date / booking date / any date-time column exist? (blocks Seasonal Fare Variation if not)**
- Currency/unit of fare columns:
- Null rates per column:
- Value ranges per fare column (min/max, any negative or zero values?):
- Does `Total Fare = Base Fare + Tax & Surcharge` hold exactly / within rounding / not at all?
- Distinct values observed in `Airline`, `Source`, `Destination`:

## Dataset size

- Row count:
- File size:
- Observed load time (for the truncate-and-reload decision, ADR-001):

## Decisions this profiling unblocks

- [ ] Confirm or revise ADR-001 (truncate-and-reload vs. reconsidering at scale)
- [ ] Confirm or revise the Seasonal Fare Variation KPI (buildable as specified / needs a proxy / out of scope)
- [ ] Finalize the fact table name (`fact_flight_prices` is provisional)
- [ ] Finalize `docs/data_contract.md`
- [ ] Finalize the city/route reference domain for ADR-010, with its source cited
