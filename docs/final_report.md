# Final Report — Flight Fare Data Pipeline

Status: outline only — to be written after implementation and a real run.
This is the graded deliverable; the sections below map directly to what the
lab specification asks for.

## 1. Pipeline architecture and execution flow

Summarize from `docs/MASTER_PLAN.md`: the two-database design, why each
exists, and the full task flow diagram.

## 2. Description of each Airflow DAG and task

One paragraph per task in `dags/flight_price_pipeline_dag.py` — what it
does and why it's a separate task rather than folded into a neighbor.

## 3. ETL/ELT classification

Pull directly from ADR-009 (`docs/engineering_decisions.md`): primarily ETL,
with an ELT-style KPI layer, and why.

## 4. KPI definitions and computation logic

Pull from `docs/kpi_definitions.md` — including how the Seasonal Fare
Variation question was actually resolved once Phase 0 completed.

## 5. Engineering decisions

Reference `docs/engineering_decisions.md` in full, or summarize the most
consequential ones (idempotency, validation/quarantine, dbt rejection,
star-schema rejection).

## 6. Challenges encountered and how they were resolved

This is where the ADR reasoning process itself becomes the answer — e.g.
the city-validation circularity problem and how it was fixed without
dropping the assignment's explicit requirement; whatever Phase 0 actually
revealed about the dataset that changed a provisional decision.

## 7. Reconciliation and data quality results

Actual numbers from a real run: source vs. valid vs. rejected vs. loaded,
and the outcome of the KPI-level sanity checks.
