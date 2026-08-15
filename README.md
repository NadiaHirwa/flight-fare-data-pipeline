# Flight Fare Data Pipeline

An Airflow-orchestrated pipeline that ingests, validates, transforms, and
computes KPIs for the Flight Price Dataset of Bangladesh — a MySQL staging
layer feeding a PostgreSQL analytics layer.

Full architecture, reasoning, and all engineering decisions live in
[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) and
[`docs/engineering_decisions.md`](docs/engineering_decisions.md). This
README covers only what's needed to run it.

## Status

**Running end to end.** Phase 0 profiling is complete
(`docs/data_profile.md`), the data contract, validation rules and KPI SQL are
finalized, and the DAG has been triggered against the real 57,000-row dataset
with all 12 tasks succeeding. Measured figures from that run are in
[`docs/performance_metrics.md`](docs/performance_metrics.md).

## Deliverables

Mapped directly to the lab's stated requirements:

| Requirement | Where it lives |
|---|---|
| Data ingestion (CSV → MySQL staging) | `src/ingestion/`, `include/sql/staging/create_staging_table.sql` |
| Data validation (nulls, types, inconsistencies, invalid city names) | `src/validation/`, `docs/data_contract.md` |
| Data transformation & KPI computation | `src/transformation/`, `src/kpi/`, `docs/kpi_definitions.md` |
| Data loading into PostgreSQL | `src/transformation/fact_loader.py`, `include/sql/analytics/create_fact_table.sql` |
| Pipeline architecture and execution flow | `docs/final_report.md` §1 |
| Description of each Airflow DAG and task | `docs/final_report.md` §2, `dags/flight_price_pipeline_dag.py` |
| KPI definitions and computation logic | `docs/final_report.md` §4, `docs/kpi_definitions.md` |
| Challenges encountered and how they were resolved | `docs/final_report.md` §6, `docs/engineering_decisions.md` (all ADRs) |
| Working, runnable pipeline | `docker-compose.yml`, `Makefile`, verified end-to-end — see Verification below and `docs/performance_metrics.md` for the measured run |
| Test suite | `tests/` (362+ unit tests, 5 DAG structure tests), CI-enforced |

## Setup

Repository: **https://github.com/NadiaHirwa/flight-fare-data-pipeline**

Prerequisites: Docker Desktop (or Docker Engine + Compose v2) and `make`.
Everything else runs in containers.

```bash
git clone https://github.com/NadiaHirwa/flight-fare-data-pipeline.git
cd flight-fare-data-pipeline
cp .env.example .env
# Fill in .env: generate a real Fernet key and secret key (commands are
# commented in .env.example), and set real passwords — never use the
# placeholder "change_me" values.
```

**Get the dataset.** The CSV is not committed (size and licensing — see
`.gitignore`), so download it and place it at exactly this path:

```
include/data/Flight_Price_Dataset_of_Bangladesh.csv
```

Source: [Flight Price Dataset of Bangladesh](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)
on Kaggle (57,000 rows, 13.49 MB). `check_source_file` fails with the expected
path in the message if it isn't there.

```bash
make up        # start every container; creates the Airflow admin user and
               # both Airflow Connections (mysql_staging, postgres_analytics)
make db-init   # apply all DDL — run once, after the containers are healthy
```

Then trigger `flight_price_pipeline` from the Airflow UI, or:

```bash
docker compose exec airflow-scheduler airflow dags trigger flight_price_pipeline
```

Airflow UI: http://localhost:8081 (login with the admin credentials set in `.env`)

There are no other manual steps — no connections to click together in the UI,
no permissions to grant by hand. `make up` is safe to re-run: the admin user
and both Connections are recreated idempotently, and the Connections are
rebuilt from `.env` each time, so changing a password there is picked up on
the next start rather than silently ignored.

`make db-init` applies each DDL file as the least-privilege role that owns
that layer — the staging files as `staging_loader`, the analytics files as
`analytics_writer`. The analytics half specifically must not run as the
superuser: whoever runs `CREATE TABLE` owns the table, and the `CREATE INDEX`
in `kpi_top_routes.sql` requires ownership rather than privileges, so
superuser-created tables fail the KPI task with `must be owner of table`.

## Verification

Don't take the Status line above on faith — this is how to confirm the whole
pipeline genuinely works, from nothing:

```bash
make clean          # destroy all volumes, start from zero
make up             # fresh containers, admin user + Connections created automatically
make db-init        # apply all DDL as the correct least-privilege roles
# place the CSV in include/data/ if not already there
docker compose exec airflow-scheduler airflow dags trigger flight_price_pipeline
```

Then check, in the Airflow UI (`localhost:8081`) or via `make psql`/`make mysql-shell`:
- All 12 tasks show green (Graph view)
- `SELECT COUNT(*) FROM flight_fare_quotes;` → `54478`
- The most recent run's rejects:
  ```sql
  SELECT COUNT(*) FROM quarantine
   WHERE pipeline_run_id = (SELECT pipeline_run_id FROM pipeline_runs
                             ORDER BY started_at DESC LIMIT 1);
  ```
  → `2522`. Note: `quarantine` accumulates across every run by design — each
  run only clears its own rows — while `flight_fare_quotes` is
  truncate-and-reload (ADR-001), so it always reflects just the latest run.
  Running `SELECT COUNT(*) FROM quarantine;` with no `WHERE` after triggering
  the DAG more than once will correctly return a higher number — that's
  expected history accumulating, not a duplication bug.
- `SELECT status FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;` → `success`
- `make test-docker` → all tests pass
- After pushing: the latest commit shows green on GitHub Actions

## What a run produces

A successful run takes about 30 seconds and writes:

| Where | What |
|---|---|
| `staging.raw_flights` | 57,000 rows, exactly as the CSV had them |
| `staging.quarantine` | 2,522 rejected rows, with the rule and reason for each |
| `staging.pipeline_runs` | one audit row per run — counts, checksum, status |
| `analytics.flight_fare_quotes` | 54,478 validated, typed rows |
| `analytics.kpi_*` | the four KPI tables (24 airlines, 152 routes, 4 seasons) |

Rejected rows are never dropped. The ~4.42% rejection rate is the dataset's own
known noise, not a pipeline fault — see
[`docs/final_report.md`](docs/final_report.md) §7.

## Ports and access

| Service | URL / port | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | `AIRFLOW_ADMIN_*` from `.env` |
| pgAdmin | http://localhost:5050 | `PGADMIN_DEFAULT_*` from `.env` |
| PostgreSQL | `localhost:5432` | `ANALYTICS_DB_*` from `.env` |
| MySQL | `localhost:3308` | `MYSQL_*` from `.env` |

MySQL is on 3308, not 3306, to avoid clashing with a local install. If a native
PostgreSQL already holds 5432 on your machine, it will shadow the container for
loopback connections — use `make psql` (which goes through the container) rather
than connecting to `localhost:5432`.

## Inspecting results

```bash
make psql          # psql shell on the analytics database
make mysql-shell   # mysql shell on the staging database
make help          # every available target
```

Both read credentials from `.env`; no password is typed by hand.

## Project structure

```
dags/               Thin DAG file — orchestration only, no business logic
src/                Business logic, organized by pipeline stage
  shared/           Cross-stage code: DB engines, normalization, audit writers
include/
  data/             Source CSV goes here (git-ignored — not committed)
  sql/              DDL and KPI SQL (staging/ and analytics/)
tests/              Unit and DAG structure tests
docs/               Master plan, ADRs, data contract, and the final report
postgres-init/      First-boot script creating the least-privilege analytics DB/user
docker-compose.yml  Airflow 3 (api-server, scheduler, dag-processor) + MySQL + Postgres
Makefile            Every operational command
ruff.toml           Lint rule set, pinned explicitly
```

## Documentation

| Document | What it covers |
|---|---|
| [`MASTER_PLAN.md`](docs/MASTER_PLAN.md) | Full architecture and reasoning |
| [`engineering_decisions.md`](docs/engineering_decisions.md) | All 11 ADRs |
| [`data_profile.md`](docs/data_profile.md) | Phase 0 findings on the real CSV |
| [`data_contract.md`](docs/data_contract.md) | Column-level rules — the source of truth for validation |
| [`kpi_definitions.md`](docs/kpi_definitions.md) | Exact KPI logic |
| [`performance_metrics.md`](docs/performance_metrics.md) | Measured timings from a real run |
| [`final_report.md`](docs/final_report.md) | The deliverable: architecture, DAG, KPIs, challenges |

## Running tests locally (outside Docker)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

`tests/test_dag_integrity.py` needs Airflow installed. Without it, run the rest
with `pytest tests/ --ignore=tests/test_dag_integrity.py`, or run the DAG tests
inside the container with `make test-docker`.