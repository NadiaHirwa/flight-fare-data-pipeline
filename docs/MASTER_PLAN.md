# Master Plan — Flight Fare Data Pipeline

## Project Overview

**Assignment (restated in full, so this document stands alone):** build an
end-to-end Apache Airflow pipeline that ingests the Flight Price Dataset of
Bangladesh (a CSV), loads it into a MySQL staging table, validates and
transforms it, computes four KPIs (average fare by airline, seasonal fare
variation, booking count by airline, most popular routes), and loads the
results into a PostgreSQL analytics database — with a written report
covering architecture, DAG/task descriptions, KPI logic, and challenges
encountered.

**Definition of done** — every item below must be true for the project to
be considered complete. Anyone picking up this document should be able to
work through this checklist top to bottom without needing to ask what's
left:

- [x] Phase 0 dataset profiling complete, findings recorded in `docs/data_profile.md`
- [x] `docs/data_contract.md` fully filled in (no blank cells)
- [ ] All DDL written and applied: `include/sql/staging/`, `include/sql/analytics/`
- [ ] Every task body in `dags/flight_price_pipeline_dag.py` implemented (no `NotImplementedError` remaining)
- [ ] All four KPIs computed, including a resolved decision on Seasonal Fare Variation either way
- [ ] KPI-level reconciliation checks (sum-of-counts, min≤avg≤max) passing
- [ ] Source-to-target reconciliation equations hold on a full pipeline run (`source = valid + rejected`, `valid = loaded`)
- [ ] Full pipeline runs end-to-end successfully via `make up` + manually triggering the DAG in the UI
- [x] `docs/kpi_definitions.md` finalized with no "PENDING" markers left
- [x] `docs/engineering_decisions.md` ADRs updated if Phase 0 changed any provisional decision (ADR-001, ADR-005, ADR-010 are the ones flagged as provisional)
- [ ] Tests passing locally (`make test`) and in CI
- [ ] `docs/performance_metrics.md` populated from a real run, not left as a template
- [ ] `docs/final_report.md` written with real findings, not the outline

Everything below this point is the reasoning and architecture that gets you
to that checklist — read it once, then use the checklist above as the
actual execution tracker.


**Repository name:** `flight-fare-data-pipeline`

**Guiding principle:** optimize for engineering maturity, not technology count. Every database table, Airflow task, validation rule, test, and document must trace back to an identifiable problem. Where a common "production" pattern is deliberately *not* used, that decision is documented with the same rigor as one that is.

This is a DEM06 (Data Transformation and Orchestration) lab. The core graded skill is demonstrating sound ETL/ELT and orchestration judgment on a dataset of manageable size — not deploying the largest possible toolchain.

---

## Phase 0 — Dataset Profiling (must run before any schema or DAG code is written)

Nothing in Phases 1+ is finalized until these questions are answered against the real CSV. This phase produces `docs/data_profile.md`.

**Grain and identity**
- What does one row represent — a booking, a fare quote/offer, or a flight option? This determines what "Booking Count by Airline" actually counts.
- Is there a natural or composite unique identifier? If not, is a synthetic row hash needed for idempotency?
- Are duplicate rows legitimate (e.g., multiple fare classes for the same flight) or data defects?

**Schema reality check**
- Full column inventory beyond the six named in the assignment (`Airline`, `Source`, `Destination`, `Base Fare`, `Tax & Surcharge`, `Total Fare`).
- **Is there a travel date, booking date, or any date/time column at all?** This is a hard blocker for the Seasonal Fare Variation KPI as literally specified. If no date field exists:
  - Fallback A: check for a categorical "season" or "month" field that could substitute.
  - Fallback B: if neither exists, the report will state this limitation explicitly and either redefine the KPI on the nearest available proxy (documented as a deviation) or mark it out of scope with justification. We do not fabricate a season assumption not supported by the data.
- Currency and unit of the fare columns (assume BDT unless the data says otherwise — confirm, don't assume).
- Null rates and value ranges per column (especially fare columns — check for negative or zero fares, and unrealistic outliers).
- Does `Total Fare = Base Fare + Tax & Surcharge` hold exactly, approximately (rounding), or not at all in the raw data? This determines the transformation rule and its tolerance.
- What values actually occur in `Airline`, `Source`, `Destination` — is there a fixed, known set (enum-like) or free text requiring normalization (e.g., "Dhaka" vs "DAC" vs "dhaka ")?

**Output of Phase 0:** `docs/data_profile.md` with findings, plus a go/no-go decision on each KPI as literally specified.

---

## Data Contract

`docs/data_contract.md` — filled in after Phase 0, structured per column:

```
column | type | nullable | business meaning | valid domain/range | validation rule | action on violation
```

Example entries (to be confirmed/adjusted after profiling):

```
Base Fare
  type: NUMERIC(12,2)
  nullable: false
  rule: value >= 0
  action: reject to quarantine

Total Fare
  type: NUMERIC(12,2)
  rule: abs(total_fare - (base_fare + tax_surcharge)) <= 1.00
  action: reject to quarantine (tolerance covers rounding only)

Source / Destination
  type: VARCHAR
  nullable: false
  rule: non-empty, member of the confirmed city/airport set from profiling
  action: reject to quarantine

Airline
  type: VARCHAR
  nullable: false
  rule: non-empty
  action: reject to quarantine
```

This contract is the single source of truth for validation logic — the code implements the contract, not the other way around.

---

## Validation Strategy — Three Explicit Levels

**Level 1 — Schema validation** (fails fast, stops the pipeline)
- Required columns present
- Columns parseable to their declared types
- Failure here is a pipeline-stopping error, not a quarantine event — it means the source file itself is structurally wrong.

**Level 2 — Data quality validation** (row-level, quarantine on failure)
- Nulls in required fields
- Duplicate rows (per the grain established in Phase 0)
- Negative or zero fares
- Invalid city names in `Source`/`Destination` — checked against a small, **independently-defined** reference list of known Bangladesh airports/cities, not against "whatever values happen to appear in this CSV." Deriving the valid domain from the same file being validated is circular and would make the check meaningless. Phase 0 profiling identifies which values actually occur; those are then cross-checked against the authoritative reference, and any mismatch (typos, unrecognized entries) is what gets flagged — this satisfies the assignment's explicit "invalid city names" requirement without validating a file against itself.
- Whitespace/casing normalization on `Airline`/`Source`/`Destination` before the above checks run

**Level 3 — Business rule validation** (row-level, quarantine on failure)
- `Source != Destination`
- `Total Fare ≈ Base Fare + Tax & Surcharge` within tolerance
- Fare within a reasonable bound (defined after profiling — e.g., no fare below a minimum floor or above a plausible ceiling for domestic Bangladesh routes)

**Quarantine design.** Invalid rows are never silently dropped. Each rejected row is written to a `staging.quarantine` table with:

```
original_record (raw values)
source_row_number
source_record_hash
rejection_reason
validation_level (1/2/3)
rule_violated
pipeline_run_id
ingested_at
```

`source_record_hash` reuses the same deterministic hash computed for idempotency purposes elsewhere in the pipeline — near-zero additional cost, and it makes tracing a specific rejected row back to its exact source position ("CSV row 1,753 was rejected, here's exactly why") a direct lookup instead of a manual search.

**Data quality gate (threshold, justified not arbitrary — finalized after Phase 0).**
- Rejection rate `< 6%` → pipeline continues, warning logged.
- Rejection rate `>= 6%` → pipeline fails the quality gate; downstream tasks do not run. Phase 0 profiling found the dataset's only real violation — the `Total Fare` reconciliation check — failing at a natural rate of 4.42%, consistently across every `Seasonality` and `Class` value (consistent with deliberately injected noise, not a systemic bug). The original 5% placeholder would have passed by under one percentage point — too fragile to trust. 6% is set deliberately above this known noise floor so the gate still catches something genuinely abnormal.

---

## Pipeline Architecture — Why Two Databases

```
CSV (source, static file)
    |
    v
MySQL — staging.raw_flights        <- raw landing zone, loose types, mirrors source structure
    |
    v  (Level 1/2/3 validation, quarantine)
MySQL — staging.quarantine         <- rejected rows with reasons
    |
    v  (valid rows only)
transform (Total Fare calc, type normalization)
    |
    v
PostgreSQL — analytics.flight_fare_quotes   <- clean, typed, query-ready
    |
    v
PostgreSQL — analytics.kpi_* tables          <- one table per KPI, computed via SQL
```

**Why MySQL for staging specifically:** it exists as the raw/staging layer requested by the assignment — not an arbitrary second database. MySQL holds `raw_flights` (as loaded, minimally typed) and `quarantine` (rejected rows). It does not hold a second "validated" copy — valid rows pass directly into the transformation step rather than being persisted twice in the same database, avoiding redundant storage for no benefit at this data volume.

**Why PostgreSQL for analytics:** it is the serving layer — clean, strongly typed (`NUMERIC(12,2)` for all money columns, never `FLOAT`), indexed for the query patterns the KPIs need, and completely decoupled from the raw ingestion format.

**Schema decision:** a single `flight_fare_quotes` table (finalized name — see ADR-011; Phase 0 confirmed each row is a flight fare quote/offer, not a booking) plus flat KPI tables, not a dimensional star schema. Per Phase 0, this dataset is a single static extract with no update cadence and no evidence of needing separately-managed dimension tables. A star schema was evaluated and rejected — see ADR-004.

---

## Idempotency and Reproducibility

**Strategy: full truncate-and-reload** (final — confirmed by Phase 0 at 57,000 rows / 13.49 MB, comfortably small enough that no reconsideration is warranted), at both the MySQL staging layer and the PostgreSQL analytics layer, on every DAG run. This dataset is a static, one-time file — not date-partitioned daily data — so the standard Airflow `data_interval` partitioning pattern does not apply here, and applying it anyway would be a misuse of the pattern rather than a correct implementation of it.

**Safety net beyond truncate-and-reload:** the analytics fact table carries a deterministic row hash (computed from the row's business-key fields) with a `UNIQUE` constraint. This means that even in a future scenario where truncate-and-reload isn't used (e.g., an updated CSV arrives incrementally), duplicate rows still cannot silently accumulate — the constraint would surface a conflict rather than allow silent duplication.

**Retries must be safe by construction.** Airflow automatically retries failed tasks; every task is designed so that a retry produces the same end state as a single successful run (no `INSERT`-without-upsert, no `datetime.now()` inside business logic, per the official Airflow best-practices guidance already studied in this module).

**File-level protection (lightweight addition, not new infrastructure).** The pipeline run/audit table (below) records the source file's SHA-256 checksum alongside its row counts. This is nearly free to add given the audit table already exists, and it lets us detect "the exact same file was submitted twice" without building a separate subsystem for it.

---

## Pipeline Audit / Run Metadata

A `staging.pipeline_runs` table, populated by the DAG itself:

```
pipeline_run_id (PK)
source_file
source_file_checksum
source_row_count
staged_row_count
valid_row_count
rejected_row_count
loaded_row_count
started_at
completed_at
status (running / success / failed / quality_gate_failed)
```

This makes reconciliation a query, not a guess:

```
source_row_count = valid_row_count + rejected_row_count   (must hold)
valid_row_count  = loaded_row_count                        (must hold, given truncate-and-reload)
```

If these equations don't hold on any run, that's a detectable data-loss signal — exactly the kind of "production engineering, inexpensively added" the review correctly emphasized.

---

## Airflow DAG Design

**Principle:** DAG files orchestrate; they do not contain business logic.

```
dags/
    flight_price_pipeline.py        <- thin: task definitions and dependencies only

src/
    ingestion/                      <- CSV -> MySQL staging
    validation/                     <- Level 1/2/3 rules, quarantine writer
    transformation/                 <- Total Fare calc, type normalization
    loading/                        <- MySQL valid rows -> Postgres fact table
    kpi/                            <- one module per KPI's SQL/logic
    quality/                        <- post-load checks, reconciliation
```

**Task boundaries** (meaningful, independently retryable units — not one task per tiny function):

```
check_source_file
  -> validate_source_schema         (file-level: readable, required headers present,
                                      no unexpected/missing columns, basic parseability —
                                      before any database insertion is attempted)
  -> load_to_mysql_staging
  -> validate_and_quarantine        (row-level: implements Levels 2-3, writes quarantine rows)
  -> quality_gate_check             (rejection-rate threshold; short-circuits downstream on failure)
  -> transform_and_load_fact        (valid rows -> Postgres fact table)
  -> [fan-out, parallel]
       compute_kpi_avg_fare_by_airline
       compute_kpi_seasonal_fare_variation   (conditional on Phase 0 date-column finding)
       compute_kpi_flight_offer_count_by_airline
       compute_kpi_top_routes
  -> [fan-in]
     post_load_quality_check        (row counts, nulls, key uniqueness, referential checks,
                                      plus KPI-level sanity checks below)
  -> reconciliation_check           (source vs. valid vs. loaded, per pipeline_runs table)
```

**KPI-level reconciliation, not just "does the KPI table exist."** Cheap, semantic checks that catch a wrong aggregation logic bug, not just a missing-table bug:

```
SUM(flight_offer_count_by_airline) = total valid fact rows   (each row has exactly one airline)
SUM(top_routes counts)        = total valid fact rows   (each row has exactly one route)
per airline: MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)
```

These verify the *logic* of the aggregation, not just its presence — a KPI table can exist, be non-null, and still be silently wrong; these checks catch that class of bug specifically.

**XCom discipline.** XCom carries only small values: `pipeline_run_id`, row counts, the source file path, table names, and the validation summary (counts, not raw rows). Data itself never travels through XCom — it lives in MySQL, PostgreSQL, or on disk, and tasks pass *references* to it.

**Failure strategy, differentiated by cause (not a single blanket try/except):**

```
Transient DB connection issue  -> retry (Airflow default_args retry/retry_delay)
Source CSV missing              -> fail immediately, no retry
Schema mismatch (Level 1)       -> fail fast, no retry
A few invalid rows (Level 2/3)  -> quarantine + continue
Rejection rate over threshold   -> quality_gate_check fails the pipeline explicitly
```

---

## KPI Definitions (exact, finalized after Phase 0)

```
Average Fare by Airline:
    AVG(total_fare) GROUP BY airline

Flight Offer Count by Airline:  (renamed from "Booking Count by Airline" — ADR-011)
    COUNT(*) GROUP BY airline
    -- No booking/customer/reservation entity exists in the source data;
    -- each row is a flight fare quote/offer, confirmed by zero duplicate
    -- rows on (Airline, Source, Destination, Departure Date & Time).

Top Routes:
    route = source || '-' || destination
    metric = COUNT(*) GROUP BY route, ORDER BY metric DESC

Seasonal Fare Variation:  (resolved — no date-range derivation needed)
    AVG(total_fare) GROUP BY seasonality
    -- The source data already provides a Seasonality column (Regular,
    -- Winter Holidays, Hajj, Eid) — no peak/non-peak date boundaries
    -- needed to be invented.
```

---

## Security

- No credentials in code, DAG files, or SQL files — all database access goes through **Airflow Connections**, referenced by `conn_id`.
- Dedicated, least-privilege database users: a `staging_loader` MySQL user scoped to the staging schema only; an `analytics_writer` Postgres user scoped to the analytics schema only. Neither uses `root`/`postgres` superuser credentials.
- All SQL from Python uses parameterized queries (SQLAlchemy or Airflow hooks) — never raw string interpolation.
- Docker Compose secrets live in `.env` (git-ignored); only `.env.example` is committed.
- A real, unique Airflow Fernet key and webserver secret key are generated for this project — never left as defaults, since the webserver is genuinely reachable on `localhost:8080`.

---

## Testing Strategy

```
Unit tests
    - fare/total calculation and tolerance check
    - each validation rule (Level 1/2/3), independently
    - route normalization / season classification (if applicable)

Database/integration tests
    - MySQL ingestion produces expected row/type outcomes
    - Postgres load matches expected fact table state
    - reconciliation equations hold on a known fixture dataset

DAG tests
    - DAG imports without error (DAG loader test)
    - expected task IDs and dependency structure
    - no cycles

Data quality tests
    - not-null / uniqueness / accepted-value checks against the data contract
    - quality-gate threshold logic (rejection rate boundary cases: just under, just over)
```

---

## CI (lightweight, not a platform)

A single GitHub Actions workflow: lint → unit tests → DAG import validation. No dedicated CI/CD platform lab scope creep here — that belongs to a different, larger assignment, not this one.

---

## ETL or ELT? — Explicit Classification

**The core pipeline follows ETL architecture: validation and core transformation (the `Total Fare` calculation, type normalization) occur in Python before the data is loaded into its final PostgreSQL analytics destination.** That is the defining trait of ETL, not ELT — transform happens before load into the destination that matters.

**Additional KPI transformations are executed inside PostgreSQL, applying an ELT-style pattern at the serving layer** — the fact table is loaded first, and the four KPI tables are then computed via SQL directly against already-landed data.

This is stated precisely rather than as a vague "hybrid" label: the system is primarily ETL, with one clearly-scoped ELT-shaped step layered on top for analytical aggregation. MySQL staging itself is closer to a raw landing zone (light typing only) and is not where the classification-defining transformation happens.

---

## dbt Adoption Decision (explicit ADR)

**Question:** should dbt be used for the PostgreSQL KPI transformations?

**Considered:**
- *For:* SQL models, automatic lineage, built-in testing, auto-generated documentation, direct reinforcement of DEM06's dbt lessons.
- *Against:* adds a second orchestration-adjacent tool to an already Airflow-centric lab; the KPI layer is four small, independent aggregate queries — not enough transformation complexity or layering to need dbt's dependency graph or modularity features; introduces additional project configuration for a dataset of manageable size.

**Decision: dbt is not used in this lab.** The four KPI tables are built as plain SQL scripts executed via Airflow's Postgres operator. This keeps the lab's scope aligned with its actual assignment (an Airflow lab), while the ETL/ELT and transformation-layer reasoning learned from dbt directly informed *how* the KPI layer is structured (clean separation of raw fact vs. aggregate "mart-style" tables), even without using the tool itself.

---

## Observability

Kept intentionally simple — no Prometheus/Grafana for this lab's scope. Tracked and written to `docs/performance_metrics.md` after a real run:

```
pipeline duration (total, and per task)
records processed / rejected / rejection rate
records loaded
load throughput
```

---

## Docker / Local Environment

- Services: Airflow (webserver + scheduler), MySQL (staging), PostgreSQL (analytics + Airflow metadata, in separate logical databases within one instance).
- **Explicit health checks** on MySQL and PostgreSQL, with Airflow's `depends_on` configured against those health checks — not just "container started." A started-but-not-yet-accepting-connections database is a common, avoidable source of flaky first-run failures.
- Reproducibility target: `git clone` → `cp .env.example .env` → `docker compose up -d` → pipeline is triggerable, with no undocumented manual steps.
- Key dependencies pinned to specific versions in `requirements.txt` (not unbounded `apache-airflow`, `pandas`, etc.) for reproducible builds.

---

## Documentation Package (trimmed — only what carries real information)

```
README.md                       - setup and run instructions
docs/
    data_profile.md              - Phase 0 findings
    data_contract.md             - column-level contract
    engineering_decisions.md     - all ADRs in one file, short format each
    kpi_definitions.md           - exact KPI logic, including the seasonality resolution
    testing_strategy.md          - brief, what's tested and why
    performance_metrics.md       - populated after a real run
    final_report.md              - the deliverable: architecture, DAG description,
                                    KPI logic, challenges and resolutions
```

No separate troubleshooting.md or empty placeholder docs — issues encountered go into `final_report.md`'s challenges section, where they belong.

---

## Engineering Decisions Log (ADR index — full detail lives in `docs/engineering_decisions.md`)

```
ADR-001  Static-dataset idempotency: truncate-and-reload, confirmed final
         after Phase 0 (57K rows) — not date-partitioning
ADR-002  MySQL staging vs. PostgreSQL analytics separation
ADR-003  Three-level validation (file-level schema, then row-level quality/business
         rules) with quarantine, not silent drop
ADR-004  Fact table + KPI tables, star schema rejected for this scope
ADR-005  Data-quality gate threshold — finalized at 6% after Phase 0 found
         a real 4.42% natural noise rate (the Total Fare reconciliation check)
ADR-006  dbt rejected for this lab's KPI layer
ADR-007  Airflow task boundaries (meaningful units, not per-function granularity)
ADR-008  Money stored as NUMERIC(12,2), never FLOAT
ADR-009  Primarily ETL, with an ELT-style KPI layer inside PostgreSQL
ADR-010  City/route validation against the IATA registry, confirmed 20
         real codes from Phase 0 — not derived from the file being validated
ADR-011  Fact table renamed flight_fare_quotes and "Booking Count by Airline"
         renamed "Flight Offer Count by Airline" — Phase 0 confirmed no
         booking entity exists in the source data
```

Each ADR follows: Context / Options considered / Decision / Reason / Consequences.

---

## Implementation Reminders (tracked, not architecture changes)

All three items originally tracked here have been resolved by Phase 0 profiling:

1. ~~The Bangladesh airport/city reference list must cite an authoritative source~~ — **resolved.** 20 confirmed IATA codes, cited against the official IATA registry, documented in full in `docs/data_contract.md`.
2. ~~The 5% rejection threshold is provisional~~ — **resolved.** Finalized at 6%, based on the real 4.42% natural noise rate found in the data. See ADR-005.
3. ~~`fact_flight_prices` is a working name~~ — **resolved.** Renamed to `flight_fare_quotes`, since Phase 0 confirmed the grain is a fare quote, not a booking. See ADR-011.

## Immediate Next Step

Phase 0 dataset profiling. Nothing downstream (schema DDL, validation code, KPI SQL, DAG structure) is finalized until the CSV has actually been inspected against the questions above — particularly the date-column question that determines whether Seasonal Fare Variation can be built as specified.
