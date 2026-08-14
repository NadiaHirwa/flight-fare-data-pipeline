# Engineering Decisions

Short format: Context / Options / Decision / Reason / Consequences.
Full reasoning and history lives in `docs/MASTER_PLAN.md`; this is the
condensed reference version for the final report.

---

## ADR-001 — Idempotency strategy for a static dataset

**Context:** The source is a one-time Kaggle CSV, not daily incremental data.
**Options:** (a) `data_interval`-based partitioning, the Airflow default assumption; (b) full truncate-and-reload per run.
**Decision:** Full truncate-and-reload — **preferred, conditional on Phase 0 confirming dataset size/load duration.**
**Reason:** There's no "yesterday's slice" to reprocess; applying date-partitioning to a static file would misuse the pattern rather than correctly implement it.
**Consequences:** Simple to reason about; would need revisiting only if profiling reveals an unexpectedly large dataset where a full reload has a real performance cost.

## ADR-002 — MySQL staging vs. PostgreSQL analytics separation

**Context:** The assignment specifies both a MySQL staging layer and a PostgreSQL analytics layer.
**Decision:** MySQL holds `raw_flights` and `quarantine` only (raw landing zone). PostgreSQL holds the fact table and KPI tables (serving layer), plus Airflow's own metadata in a separate database on the same instance.
**Reason:** Mirrors real heterogeneous stacks and the staging/marts separation studied in DEM06's dbt lessons, without persisting a redundant "validated" copy inside MySQL.
**Consequences:** Two connection types to secure (Decision covered in ADR — see Security section of MASTER_PLAN.md); clean separation of raw vs. served data.

## ADR-003 — Three-level validation with quarantine

**Context:** Assignment requires handling nulls, type errors, and inconsistencies without specifying a mechanism.
**Decision:** File-level schema validation (own task, fail-fast) → row-level data-quality checks (Level 2) → row-level business-rule checks (Level 3), all row-level failures written to a `quarantine` table with full traceability, never silently dropped.
**Reason:** A missing column is a structurally different failure than a handful of bad rows and should be handled differently (stop vs. quarantine-and-continue).
**Consequences:** More tasks/tables than a single validation step, each with a clear, distinct responsibility.

## ADR-004 — Fact table + KPI tables, star schema rejected

**Context:** DEM04 covered dimensional modeling; this dataset is a single static extract.
**Decision:** One fact table plus four flat KPI tables. No `dim_airline`/`dim_route`/`dim_date`.
**Reason:** The current dataset and analytical scope don't justify the joins, surrogate keys, and maintenance overhead a star schema would add. Demonstrating restraint here reflects DEM04 knowledge better than forcing a pattern that doesn't fit.
**Consequences:** Revisit only if profiling reveals a genuine need (e.g. slowly-changing airline metadata).

## ADR-005 — Data-quality gate threshold

**Context:** Some invalid rows are expected; the pipeline needs a policy, not ad hoc handling.
**Decision:** Rejection rate `< 5%` → continue with a warning. `>= 5%` → fail the quality gate.
**Reason:** A real threshold, not a silent `try/except`, distinguishes "some noisy data" from "something is structurally wrong with this batch."
**Consequences:** **Provisional** — must be revisited against Phase 0's actual observed rejection rate before being treated as final.

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
**Decision:** All fare columns are `NUMERIC(12,2)` in both MySQL and PostgreSQL.
**Reason:** Floating-point types introduce rounding errors unacceptable for monetary values and for the `Total Fare` reconciliation check specifically.
**Consequences:** None significant — this is strictly the correct choice for money.

## ADR-009 — Primarily ETL, with an ELT-style KPI layer

**Context:** DEM06 requires an explicit ETL/ELT classification, not just a working pipeline.
**Decision:** The pipeline is classified as primarily ETL — validation and the `Total Fare` transformation happen in Python before data lands in PostgreSQL, which is the defining trait of ETL. The KPI aggregation layer, computed in SQL after the fact table is loaded, is ELT-style.
**Reason:** Precise about *where* transformation occurs rather than defaulting to a vague "hybrid" label.
**Consequences:** None — this is a classification of the architecture as designed, not a constraint on it.

## ADR-010 — City/route validation against an independent reference domain

**Context:** The assignment explicitly requires flagging invalid city names. Deriving "valid" values from the same file being validated is circular and can't actually detect bad data.
**Decision:** Validate `Source`/`Destination` against a small, independently-sourced reference list of Bangladesh airports/cities — not against the distinct values found in the CSV itself.
**Reason:** Satisfies the assignment's explicit requirement while avoiding a validation check that can never actually fail (since it would only ever compare the file against itself).
**Consequences:** Requires sourcing and citing an authoritative reference list during implementation (see Implementation Reminders, `docs/MASTER_PLAN.md`) — not to be silently invented.
