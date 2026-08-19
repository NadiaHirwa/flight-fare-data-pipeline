# Engineering Decisions

Short format: Context / Options / Decision / Reason / Consequences.
Full reasoning and history lives in `docs/MASTER_PLAN.md`; this is the
condensed reference version for the final report.

---

## ADR-001 — Idempotency strategy for a static dataset

**Context:** The source is a one-time Kaggle CSV, not daily incremental data.
**Options:** (a) `data_interval`-based partitioning, the Airflow default assumption; (b) full truncate-and-reload per run.
**Phase 0 finding:** 57,000 rows / 13.49 MB. A measured full run reloads the analytics layer in 14.34 s and completes the whole pipeline in 32.43 s (`docs/performance_metrics.md`), so a full reload has no meaningful performance cost at this size.
**Decision:** Full truncate-and-reload — **final, no longer conditional.** Confirmed by the Phase 0 size finding above.
**Reason:** There's no "yesterday's slice" to reprocess; applying date-partitioning to a static file would misuse the pattern rather than correctly implement it.
**Consequences:** Simple to reason about, and every task is retry-safe by construction as a result. The PostgreSQL truncate and insert share one transaction, so a mid-load failure rolls back rather than leaving the serving layer half-written. This would only need revisiting if the pipeline were pointed at a substantially larger source, or at genuinely incremental data — neither of which is this dataset.

## ADR-002 — MySQL staging vs. PostgreSQL analytics separation

**Context:** The assignment specifies both a MySQL staging layer and a PostgreSQL analytics layer.
**Decision:** MySQL holds `raw_flights` and `quarantine` only (raw landing zone). PostgreSQL holds the fact table and KPI tables (serving layer), plus Airflow's own metadata in a separate database on the same instance.
**Reason:** Mirrors real heterogeneous stacks and the staging/marts separation studied in DEM06's dbt lessons, without persisting a redundant "validated" copy inside MySQL.
**Consequences:** Two connection types to secure — both handled via Airflow Connections with dedicated least-privilege roles, as set out in the Security section of `docs/MASTER_PLAN.md`; clean separation of raw vs. served data.

## ADR-003 — Three-level validation with quarantine

**Context:** Assignment requires handling nulls, type errors, and inconsistencies without specifying a mechanism.
**Decision:** File-level schema validation (own task, fail-fast) → row-level data-quality checks (Level 2) → row-level business-rule checks (Level 3), all row-level failures written to a `quarantine` table with full traceability, never silently dropped.
**Reason:** A missing column is a structurally different failure than a handful of bad rows and should be handled differently (stop vs. quarantine-and-continue).
**Consequences:** More tasks/tables than a single validation step, each with a clear, distinct responsibility.

## ADR-004 — Fact table + KPI tables, star schema rejected

**Context:** DEM04 covered dimensional modeling; this dataset is a single static extract.
**Phase 0 finding:** no slowly-changing attributes anywhere. `Airline` is 24 clean fixed values, `Class` 3, `Seasonality` 4, `Stopovers` 3, `Booking Source` 3, `Aircraft Type` 5 — all enum-like, none carrying attributes that would ever need their own row history. There is also no update cadence: the source is one static file.
**Decision:** One fact table plus four flat KPI tables. No `dim_airline`/`dim_route`/`dim_date`. **Final** — the Phase 0 finding above removed the only condition under which a star schema would have been warranted.
**Reason:** The current dataset and analytical scope don't justify the joins, surrogate keys, and maintenance overhead a star schema would add. Demonstrating restraint here reflects DEM04 knowledge better than forcing a pattern that doesn't fit.
**Consequences:** KPI queries read one table with no joins. This would only be reopened if a future source introduced genuinely slowly-changing dimension attributes — profiling confirmed this one does not.

## ADR-005 — Data-quality gate threshold

**Context:** Some invalid rows are expected; the pipeline needs a policy, not ad hoc handling.
**Phase 0 finding:** the only real violation found in the actual dataset is the `Total Fare` reconciliation check, failing on 4.42% of all 57,000 rows — consistently in the 2-5% range across every `Seasonality` and `Class` value, consistent with deliberately injected noise rather than a systemic bug. No nulls, no negative fares, no duplicates were found anywhere.
**Decision:** Rejection rate `< 6%` → continue with a warning. `>= 6%` → fail the quality gate.
**Reason:** The original 5% figure was a placeholder set before seeing real data. Once profiling showed the dataset's own known, expected noise sits at 4.42%, a 5% gate would have been passing by a margin of well under one percentage point — too fragile to trust as a real safety check. 6% is set deliberately above the known noise floor, so the gate continues to do its actual job (catching a genuinely abnormal batch) without being tripped by data we've already confirmed is fine.
**Consequences:** This is now the final threshold for this dataset, not provisional. It should still be revisited if the pipeline is ever pointed at a different or updated source file with a different noise profile.

## ADR-006 — dbt rejected for the KPI layer

**Context:** DEM06 taught dbt; the assignment is explicitly an Airflow project.
**Decision:** The four KPI tables are built as plain SQL executed via Airflow's Postgres operator, not dbt models.
**Reason:** Four small, independent aggregate queries don't have enough transformation complexity or layering to need dbt's dependency graph, modularity, or testing features — adding it would be a second orchestration-adjacent tool without solving a real problem at this scope.
**Consequences:** If future iterations add several transformation layers or reusable models, this ADR should be reopened.

## ADR-007 — Airflow task boundaries

**Context:** Business logic could live inside the DAG file or in separate modules; tasks could be split arbitrarily fine or coarse.
**Decision:** DAG files orchestrate only (task definitions, dependencies). Business logic lives in `src/`. Tasks represent meaningful, independently retryable units — not one task per tiny function.
**Reason:** Keeps the DAG file thin and readable, and makes retries meaningful (a retried task re-does one coherent unit of work, not an arbitrary code fragment).
**Consequences:** More files/modules than a monolithic DAG script, but each with a single clear responsibility.

## ADR-008 — Money stored as NUMERIC, never FLOAT

**Context:** Fare values are currency.
**Decision:** Every money column in the PostgreSQL analytics layer is `NUMERIC(12,2)` — never `FLOAT` or `DOUBLE PRECISION` — and Python holds fares as `Decimal`, never `float`, throughout validation and transformation. MySQL staging is deliberately *not* covered by this: all 17 source columns there are `VARCHAR`, so typing happens exactly once, in the transform, on the way into the serving layer.
**Reason:** Floating-point types introduce rounding errors unacceptable for monetary values, and specifically for the `Total Fare` reconciliation check — with binary floats the rounding error would sit inside the very comparison meant to detect arithmetic that doesn't add up, making the project's headline data-quality finding untrustworthy.
**Why staging is exempt:** a raw landing zone has to accept whatever the file contains. Typing `base_fare_bdt` as `DECIMAL` in `raw_flights` would make a malformed value either fail the bulk load or be silently coerced — and `quarantine.original_record` exists precisely to preserve what the source actually said. You cannot quarantine a value your schema already rejected. `VARCHAR` is lossless for this purpose: it preserves the source digits exactly, including the full float precision the CSV carries (e.g. `21131.22502141266`). See ADR-003 and the header comment in `include/sql/staging/create_staging_table.sql`.
**Consequences:** None significant for the serving layer — this is strictly the correct choice for money. The one cost is that the staging/serving split has to be stated explicitly, as above, or the rule reads as though it were violated by the staging DDL.

## ADR-009 — Primarily ETL, with an ELT-style KPI layer

**Context:** DEM06 requires an explicit ETL/ELT classification, not just a working pipeline.
**Decision:** The pipeline is classified as primarily ETL — validation and the `Total Fare` transformation happen in Python before data lands in PostgreSQL, which is the defining trait of ETL. The KPI aggregation layer, computed in SQL after the fact table is loaded, is ELT-style.
**Reason:** Precise about *where* transformation occurs rather than defaulting to a vague "hybrid" label.
**Consequences:** None — this is a classification of the architecture as designed, not a constraint on it.

## ADR-010 — City/route validation against an independent reference domain

**Context:** The assignment explicitly requires flagging invalid city names. Deriving "valid" values from the same file being validated is circular and can't actually detect bad data.
**Phase 0 finding:** `Source` uses exactly 8 distinct Bangladesh domestic airport codes; `Destination` uses those same 8 plus 12 international hub codes (20 total). All 20 are real, verifiable IATA codes.
**Decision:** Validate `Source`/`Destination` against these 20 IATA codes, confirmed against the official IATA airport code registry as the cited authoritative source — not against the distinct values found in the CSV itself, even though the observed values happen to be exactly this set.
**Reason:** Satisfies the assignment's explicit requirement while avoiding a validation check that can never actually fail (since it would only ever compare the file against itself). Citing IATA as the source (rather than "the values we happened to see") means a genuinely invalid code in a future file would still be caught.
**Consequences:** None further — the reference list is small, finite, and now finalized in `docs/data_contract.md`.

## ADR-011 — Fact table and KPI renamed to match the confirmed grain

**Context:** The assignment's wording used "Booking Count by Airline," and the plan initially used `fact_flight_prices` as a provisional table name pending grain confirmation.
**Phase 0 finding:** the dataset contains no booking, customer, or reservation information anywhere — no such entity exists in the data. Each row is a flight fare quote/offer (a specific airline, route, and departure time, at a specific class), confirmed by finding zero duplicate rows on that composite key across all 57,000 rows.
**Decision:** The fact table is named `flight_fare_quotes`, not `fact_flight_prices`. The KPI "Booking Count by Airline" is renamed "Flight Offer Count by Airline."
**Reason:** Calling data "bookings" when no booking entity exists in the source would misrepresent what the pipeline actually measures — a table or KPI name should communicate what one row actually means, not just match the assignment's original wording when that wording turns out not to fit the real data.
**Consequences:** Any reference to `fact_flight_prices` or "Booking Count" in earlier drafts of this project (DAG task names, docs) is superseded by this ADR.
