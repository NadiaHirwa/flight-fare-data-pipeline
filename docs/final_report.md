# Flight Fare Data Pipeline — Final Report

**Author:** Hirwa Nadia

**Dataset:** Flight Price Dataset of Bangladesh (Kaggle) — 57,000 rows × 17 columns, 13.49 MB

**Status:** complete. Written after a full end-to-end run against the real dataset (run `manual__zero_setup_001`). Every figure below is drawn directly from that run.

---

## Contents

1. [Pipeline architecture and execution flow](#1-pipeline-architecture-and-execution-flow)
2. [Description of each Airflow DAG and task](#2-description-of-each-airflow-dag-and-task)
3. [ETL/ELT classification](#3-etletl-classification)
4. [KPI definitions and computation logic](#4-kpi-definitions-and-computation-logic)
5. [Engineering decisions](#5-engineering-decisions)
6. [Challenges encountered and how they were resolved](#6-challenges-encountered-and-how-they-were-resolved)
7. [Reconciliation and data quality results](#7-reconciliation-and-data-quality-results)

---

## 1. Pipeline architecture and execution flow

![System architecture diagram](architecture.svg)
*Full diagram with legend and reading notes: [`architecture.md`](architecture.md).*

### 1.1 Two databases, opposite guarantees

| | MySQL — staging | PostgreSQL — analytics |
|---|---|---|
| Role | Landing zone: must accept everything | Serving layer: must reject everything invalid |
| Types | `VARCHAR(255)`, no constraints | `NUMERIC(12,2)` never `FLOAT` (ADR-008), `NOT NULL`, CHECK constraints, `UNIQUE` on row hash |
| Why | A malformed fare must survive the load so quarantine can record it. Typing it `DECIMAL` here would crash or silently coerce the row, destroying that evidence. | Bad data can't reach this layer — it's already been validated before load. |

One database can't be both permissive and strict, which is why the split exists at all. The assignment mandated two databases; this reasoning justifies the split, it didn't invent it. One deliberate choice beyond the requirement: no third "validated" table in MySQL — valid rows pass to transformation via an anti-join against `quarantine`, so nothing is stored twice.

**Spec departure, named explicitly:** the assignment asks for the CSV loaded "with appropriate column types matching the original structure." Staging doesn't do that — it's `VARCHAR` throughout, on purpose, for the reason above. The typed columns the spec wants do exist; they're one layer later, in `flight_fare_quotes`.

### 1.2 Why the DAG has the shape it does

| Point in the DAG | Why |
|---|---|
| Linear head (5 tasks) | Real data dependency — each task consumes the prior task's return value |
| Barrier before `transform_and_load_fact` | It truncates the fact table. A KPI reading mid-truncate wouldn't crash — it would aggregate a half-loaded table and report a plausible but wrong number |
| 4 KPI tasks run in parallel | No real dependency between them — each reads the fact table, writes its own disjoint table |
| Barrier at `post_load_quality_check` | Compares `SUM(KPI counts)` to the fact table's row count — must wait for all 4 KPIs or it reads a partial table |
| Explicit `>>` edges, not threaded values | Tasks need their upstream to have *happened*, not to have *returned* something |

The parallel-KPI payoff isn't speed (each task runs ~0.4s) — it's failure isolation, proven in practice: when `compute_kpi_top_routes` hit a table-ownership bug, the other three KPI tasks still succeeded in the same run. One broken task was diagnosed immediately, instead of being found three sequential runs later.

---

## 2. Description of each Airflow DAG and task

**DAG id:** `flight_price_pipeline` · **Schedule:** `None` (manually triggered — static dataset, ADR-001) · **Executor:** LocalExecutor

### 2.1 Task inventory

| # | Task | What it does |
|---|---|---|
| 1 | `check_source_file` | Confirms the CSV exists and is readable |
| 2 | `validate_source_schema` | Confirms the 17 expected headers are present and the file parses as CSV |
| 3 | `load_to_mysql_staging` | Truncates and bulk-loads the CSV into `raw_flights`, assigning `source_row_number` |
| 4 | `validate_and_quarantine` | Runs Level 2/3 validation, writes rejected rows to `quarantine` |
| 5 | `quality_gate_check` | Compares the rejection rate against the 6% threshold |
| 6 | `transform_and_load_fact` | Converts types, computes the row hash, loads valid rows into `flight_fare_quotes` |
| 7–10 | `compute_kpi_*` (×4) | Each runs its corresponding pre-written SQL file, in parallel |
| 11 | `post_load_quality_check` | KPI-level sanity checks (sum-of-counts, min≤avg≤max) |
| 12 | `reconciliation_check` | Verifies source/valid/loaded equations, marks the run `success` |

### 2.2 Why specific tasks are split the way they are

| Tasks | Why split |
|---|---|
| `check_source_file` / `validate_source_schema` | Different owners for different failures. A missing file is an ops problem (nobody dropped the CSV in `include/data/`); a wrong header is a data-contract problem (the source changed shape). One red square in the UI tells you which, before opening a log. `check_source_file` also imports nothing from `src/`, so it still works even if the project's own modules are broken. |
| `validate_and_quarantine` / `quality_gate_check` | Different retry policies. Validation keeps the DAG's default `retries: 1` (real DB work, a transient blip deserves a retry). The gate is `retries=0` — its verdict is a fixed comparison against 6%, and retrying reaches the same answer five minutes later. Measuring and judging are different responsibilities: the gate applies a policy (ADR-005) that's already changed once, from 5% to 6%; changing it again means clearing one task, not re-validating 57,000 rows. |
| `transform_and_load_fact` (kept as one task, not split further) | Can't be split. The truncate and the row inserts share a single Postgres transaction — a mid-load failure rolls back cleanly instead of leaving the table half-written — and a transaction can't span two Airflow tasks. The 54,478-row intermediate result also can't cross a task boundary without violating XCom discipline (small values only). |

**On the Total Fare calculation specifically:** the assignment says to calculate it "if not already present." It already is present (`Total Fare (BDT)`), so the real requirement here is validation, not calculation — exactly what the Level 3 reconciliation check does. `transform_and_load_fact` carries the reported value across unchanged rather than recomputing it, since recomputing would silently overwrite the 2,522 disagreements this pipeline exists to surface.

**`reconciliation_check`** adds a third equation beyond the original spec — comparing `loaded_row_count` against the fact table's actual `COUNT(*)` — since the first two equations are self-reported and a miscounting stage would satisfy both anyway. It's also the task that marks the run `status='success'`; before this existed, a fully successful run stayed at `'running'` forever.

---

## 3. ETL/ELT classification

**Primarily ETL, with an ELT layer added on top — the boundary is the fact table.**

| Stage | Pattern | Why |
|---|---|---|
| MySQL → Python → PostgreSQL (`transform_and_load_fact`) | ETL | Data is fully typed and validated in Python *before* reaching Postgres. MySQL does zero transformation — it's `VARCHAR`-only, on purpose, so bad values survive to be quarantined. |
| PostgreSQL → KPI tables (`compute_kpi_*`) | ELT | Each task just hands a SQL file to Postgres. Data is already loaded; Postgres aggregates it in place. No Python logic involved. |

Everything before the fact table is transform-then-load; everything after is load-then-transform. Calling it simply "a hybrid" is true but incomplete — the useful answer is knowing exactly where the switch happens.

---

## 4. KPI definitions and computation logic

*Source columns carry a currency suffix, e.g. `Base Fare (BDT)`; referred to below without the suffix.*

| KPI | Definition | Result |
|---|---|---|
| Average Fare by Airline | `AVG(total_fare) GROUP BY airline` | 24 airlines; Turkish Airlines tops the list at ~74,170 BDT; spread across airlines is relatively narrow |
| Flight Offer Count by Airline | `COUNT(*) GROUP BY airline` | Renamed from the assignment's "Booking Count" (ADR-011) — counts listings, not sales; no booking/customer entity exists in this data |
| Top Routes | `COUNT(*) GROUP BY source, destination` | 152 routes, nearly flat at 299–399 offers each. The flatness is the finding — treating RJH–SIN as a meaningful "winner" would be reading noise as signal |
| Seasonal Fare Variation | `AVG(total_fare) GROUP BY seasonality` | See table below — the strongest signal in the dataset |

**Seasonal Fare Variation, in full:**

| Seasonality | Peak | Rows | Avg fare (BDT) | vs Regular |
|---|:---:|---:|---:|---:|
| Hajj | ✓ | 910 | 94,632 | **+41.2%** |
| Eid | ✓ | 579 | 90,549 | **+35.1%** |
| Winter Holidays | ✓ | 10,710 | 78,739 | **+17.5%** |
| Regular | — | 42,279 | 67,018 | baseline |

Hajj is under 2% of the 54,478 valid rows, yet produces the strongest signal in the data — easy to miss if the only thing examined is an average across everything.

**Why this KPI almost couldn't be built:** doing "seasonal variation" properly requires knowing which dates count as peak, and it wasn't known upfront whether the file had a date field at all. Inventing our own peak-season boundaries was explicitly ruled out. Profiling found an existing `Seasonality` column instead, already labelled with all four values — so the KPI became a direct `GROUP BY`, with no invented assumptions.

---

## 5. Engineering decisions

Eleven ADRs are recorded in full in [`engineering_decisions.md`](engineering_decisions.md). Five stand out:

| ADR | Decision | Why it mattered |
|---|---|---|
| ADR-010 | Validate city/airport codes against the external IATA registry, not the file's own values | A self-referential check can never fail. An outside authority actually catches something. Strongest piece of judgment in the project. |
| ADR-003 | Three-level validation with quarantine; never drop rows silently | The source of almost everything downstream — the quarantine table, the anti-join design, the quality gate, the reconciliation equations |
| ADR-008 | Money as `NUMERIC(12,2)`, never `FLOAT` (analytics layer) | Total Fare reconciliation is the only rule that ever fires (all 2,522 rejections) — float rounding error would sit inside that exact comparison |
| ADR-001 | Truncate-and-reload, not date-partitioning | Static one-time dataset, no "yesterday's slice" to reprocess. Makes every task retry-safe by construction |
| ADR-011 | Renamed to `flight_fare_quotes` / "Flight Offer Count" | No booking entity exists in the source — keeping "booking" language would misrepresent every count |

---

## 6. Challenges encountered and how they were resolved

| Challenge | Problem | Resolution |
|---|---|---|
| **City-validation check that couldn't fail** | Checking city names against the file's own distinct values is circular — it can never fail | Validate against the external IATA registry (20 codes) instead. Phase 0 confirmed the file's values happen to match, but the check would now catch a genuinely wrong code in a future file |
| **Airflow 3 moved the furniture** | DAG parsed and tested clean but wouldn't run: `DagNotFound`, then `[Errno 111] Connection refused`, then `Invalid auth token` | Three separate Airflow 3 architecture changes, found one at a time: a missing `dag-processor` service, the task Execution API pointing at the wrong host, and each process generating its own JWT secret. All fixed in `docker-compose.yml`. Zero bugs in the DAG itself — a version bump had relocated the runtime contract. |
| **Grants aren't ownership** | `analytics_writer` couldn't use its own tables: `permission denied`, then `must be owner of table kpi_top_routes` | Schema-level grants don't cover tables created later, and `CREATE INDEX` needs ownership specifically, which no grant provides. Fixed by making `analytics_writer` own the `public` schema, so it creates and owns its own tables. Verified by rebuilding from a destroyed volume. |
| **"It works from scratch" kept being untrue** | Testing `make clean && make up` from nothing repeatedly surfaced steps that had quietly become manual: applying the DDL, then creating the Airflow Connections | Both automated now (`airflow-init` creates Connections, `make db-init` applies schema). Reproducibility wasn't being misrepresented — it decayed quietly each time something was fixed interactively instead of in the scripts. |

Two lessons worth keeping: **a passing check isn't evidence of anything until you know what would make it fail.** And the only reliable way to know reproducibility still holds is to destroy the environment and rebuild it, repeatedly, after every change.

---

## 7. Reconciliation and data quality results

**Headline numbers, run `manual__zero_setup_001`:**

```
source_row_count    57,000
staged_row_count    57,000
valid_row_count     54,478
rejected_row_count   2,522   (4.42%)
loaded_row_count    54,478
status              success
```

**All three equations held** (the third goes beyond the original spec — it checks the fact table directly, so a self-reported miscount can't hide):

```
57,000 = 54,478 + 2,522                      source = valid + rejected     ✓
54,478 = 54,478                              valid = loaded                ✓
54,478 = COUNT(*) in flight_fare_quotes      loaded = what's actually there ✓
```

**One rule accounted for every rejection:**

| Rule violated | Level | Rows |
|---|:---:|---:|
| `total_fare_reconciliation` | 3 | 2,522 |

`abs(total_fare - (base_fare + tax_surcharge)) > 1.00`. Nothing else fired — no nulls, no non-positive fares, no invalid IATA codes, no unrecognised categories, no duplicate grain keys.

**What that tells us:**
- Matches Phase 0's independent profiling exactly (4.42%) — the pipeline found what a separate pass had already measured, no more, no less
- Not rounding noise — gaps run 445 to 93,165 BDT
- Flat across every season and class — consistent with random noise, not a systemic bug
- Comfortably under the 6% gate, ~1.6 points of headroom

**KPI-level sanity checks all passed:** `SUM(flight_offer_count_by_airline)` and `SUM(top_routes counts)` both equal 54,478; `MIN(total_fare) <= AVG(total_fare) <= MAX(total_fare)` holds with zero violations across all 24 airlines.

None of the 2,522 rejected rows were discarded — all recorded in `staging.quarantine` with original values, exact CSV row number, rule, and reason. A separate check confirmed none made it into the fact table.

---

*Full architecture reasoning: [`MASTER_PLAN.md`](MASTER_PLAN.md). All engineering decisions in full: [`engineering_decisions.md`](engineering_decisions.md). Measured run timings: [`performance_metrics.md`](performance_metrics.md).*