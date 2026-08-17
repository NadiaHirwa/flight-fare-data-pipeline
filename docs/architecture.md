# System Architecture

![Flight Fare Data Pipeline architecture diagram](architecture.svg)

This diagram shows the system as it actually runs: a static CSV, Apache
Airflow orchestrating every step, a MySQL staging layer that accepts data
raw, and a PostgreSQL analytics layer that only ever holds validated data.

The full reasoning behind this shape — why two databases, why the DAG has
the specific task structure it does, and what each least-privilege role is
allowed to do — is written out in detail in
[`final_report.md` §1](final_report.md#1-pipeline-architecture-and-execution-flow)
and [`MASTER_PLAN.md`](MASTER_PLAN.md#pipeline-architecture--why-two-databases).
This page exists to give a reader the picture at a glance before reading
either of those in full.

## Reading the diagram

- **CSV → Airflow**: the source file is read once per run; nothing is
  scheduled daily (ADR-001 — this is a static, one-time dataset, not
  incremental data).
- **Airflow → MySQL (staging)**: ingestion and validation happen here.
  `raw_flights` holds the file as-is; `quarantine` holds every rejected row
  with full traceability back to its exact source position; `pipeline_runs`
  is the audit trail for every run.
- **MySQL → PostgreSQL**: the one arrow where data actually moves between
  the two databases, inside a single database transaction
  (`transform_and_load_fact`) — a failure here rolls back cleanly rather
  than leaving the analytics layer half-loaded.
- **Airflow → PostgreSQL (analytics)**: the four KPI tables are computed
  entirely in SQL, run by Airflow but executed by PostgreSQL itself against
  data already sitting in `flight_fare_quotes` — the ELT half of an
  otherwise-ETL pipeline (ADR-009).
- **Inspection tools**: pgAdmin and MySQL Workbench are dev-convenience
  tools for browsing results — neither is part of the pipeline itself.

Each database role only owns its own layer — `staging_loader` cannot touch
PostgreSQL, and `analytics_writer` cannot touch MySQL. Neither ever uses a
superuser account.
