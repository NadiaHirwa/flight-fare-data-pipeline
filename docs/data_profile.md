# Data Profile — Phase 0 Findings

Status: **complete.** Profiled against the real
`Flight_Price_Dataset_of_Bangladesh.csv` (13.49 MB, 57,000 rows, from Kaggle
user mahatiratusher).

## Grain and identity

**One row = one flight fare quote/offer** — a specific airline operating a
specific route at a specific departure date/time, at a specific class. This
is **not booking data** — there is no passenger, customer, or booking
reference field anywhere in the file. Confirmed by checking for duplicate
(`Airline`, `Source`, `Destination`, `Departure Date & Time`) combinations
across all 57,000 rows: **zero duplicates found**, even without including
`Class` in the key. This changes naming decisions — see "Decisions this
profiling unblocks" below.

No natural single-column unique identifier exists; a composite key (the four
columns above, or those plus `Class`) uniquely identifies each row.

## Full column inventory

Seventeen columns total — eleven more than the six named in the assignment:

```
Airline, Source, Source Name, Destination, Destination Name,
Departure Date & Time, Arrival Date & Time, Duration (hrs), Stopovers,
Aircraft Type, Class, Booking Source, Base Fare (BDT), Tax & Surcharge (BDT),
Total Fare (BDT), Seasonality, Days Before Departure
```

Two findings here materially change the plan:

- **A date column exists** (`Departure Date & Time`, also `Arrival Date & Time`)
  — fully parseable, zero unparseable values, range 2025-01-03 to 2026-03-31.
  This unblocks the Seasonal Fare Variation KPI.
- **A `Seasonality` column already exists**, with exactly four values:
  `Regular` (44,525 rows), `Winter Holidays` (10,930), `Hajj` (942), `Eid`
  (603). **We do not need to derive peak-season date boundaries ourselves —
  the dataset already provides the classification.** This is a significant
  simplification over what the plan assumed.

## Currency and units

Confirmed **BDT (Bangladeshi Taka)** — explicit in the column names
(`Base Fare (BDT)`, etc.), no separate currency column needed.

## Null rates

**Zero nulls in every column.** No missing-value handling is actually needed
for Level 2 validation on this dataset — the rule stays in the contract as a
defensive check, but it won't trigger on this data.

## Fare value ranges

```
Base Fare (BDT):        min 1,600.98   max 449,222.93   negative: 0   zero: 0
Tax & Surcharge (BDT):  min 200.00     max 73,383.44     negative: 0   zero: 0
Total Fare (BDT):       min 1,800.98   max 558,987.33    negative: 0   zero: 0
```

No negative or zero fares anywhere — this Level 2 check also won't trigger,
but stays in the contract.

## Total Fare reconciliation — the one real data quality issue

`Total Fare = Base Fare + Tax & Surcharge` holds for **95.58%** of rows.
**4.42% (2,522 rows)** fail this check, and the failures are not small
rounding noise — the differences range from ~445 to ~93,165 BDT. Checked
whether this concentrates in any category (season, class): it doesn't —
the mismatch rate is roughly 2-5% across every `Seasonality` value and every
`Class`, consistent with randomly injected bad data rather than a systemic
calculation bug. This is very likely deliberate — exactly the kind of
inconsistency the assignment explicitly asks the pipeline to catch.

**This is important: this single rule alone produces a 4.42% rejection
rate, with zero other violations found anywhere else in the dataset.** See
the ADR-005 discussion below — this is dangerously close to the 5%
provisional threshold.

## Distinct categorical values

```
Airline (24):         all clean, real airline names, no case/whitespace issues found
Class (3):             Business, Economy, First Class
Stopovers (3):         Direct, 1 Stop, 2 Stops
Booking Source (3):    Direct Booking, Online Website, Travel Agency
Aircraft Type (5):     Airbus A320, Airbus A350, Boeing 737, Boeing 777, Boeing 787
```

## Source and Destination airport codes — defines ADR-010's reference domain

```
Source (8 distinct):       BZL, CGP, CXB, DAC, JSR, RJH, SPD, ZYL
                            — all Bangladesh domestic airports
Destination (20 distinct): the same 8 domestic codes, PLUS 12 international
                            hub codes: BKK, CCU, DEL, DOH, DXB, IST, JED,
                            JFK, KUL, LHR, SIN, YYZ
```

`Source` is always Bangladesh-domestic; `Destination` can be domestic or a
major international hub. `Source == Destination` never occurs (0 rows).

**This is a small, finite, real-world-verifiable set** — all 20 codes are
legitimate IATA airport codes. The reference domain for ADR-010 can cite the
official IATA airport code registry, confirming these 20 specific codes,
rather than being derived from the file's own distinct values — satisfying
both the assignment's requirement and the anti-circularity fix from ADR-010.

## Dataset size (for ADR-001)

57,000 rows, 13.49 MB. **Comfortably small** — truncate-and-reload is
confirmed as final, not provisional. No reason to reconsider this decision.

## Decisions this profiling unblocks

- [x] ADR-001 confirmed final: truncate-and-reload (57K rows is small; no reconsideration needed)
- [x] Seasonal Fare Variation KPI: buildable directly via `GROUP BY Seasonality` — no date-range derivation needed
- [x] ADR-005 decided: threshold set to 6%, above the 4.42% natural noise floor found here — full reasoning in `engineering_decisions.md`
- [x] Fact table and KPI renamed: `flight_fare_quotes` (not `fact_flight_prices`) and "Flight Offer Count by Airline" (not "Booking Count") — ADR-011, since no booking entity exists in the source data
- [x] ADR-010 reference domain: the 20 IATA codes listed above, citable against the official IATA registry
- [x] `docs/data_contract.md` — finalized using the findings above