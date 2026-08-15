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

## Setup

```bash
git clone https://github.com/NadiaHirwa/flight-fare-data-pipeline.git
cd flight-fare-data-pipeline
cp .env.example .env
# Fill in .env: generate a real Fernet key and secret key (commands are
# commented in .env.example), and set real passwords — never use the
# placeholder "change_me" values.

# Place the source CSV here before running the pipeline:
#   include/data/Flight_Price_Dataset_of_Bangladesh.csv

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

## Project structure

```
dags/           Thin DAG file — orchestration only, no business logic
src/            Business logic, organized by pipeline stage
include/
  data/         Source CSV goes here (git-ignored — not committed)
  sql/          DDL and KPI SQL (staging/ and analytics/)
tests/          Unit, integration, and DAG structure tests
docs/           Master plan, ADRs, data contract, and the final report
postgres-init/  First-boot script creating the least-privilege analytics DB/user
```

## Running tests locally (outside Docker)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

`tests/test_dag_integrity.py` needs Airflow installed. Without it, run the rest
with `pytest tests/ --ignore=tests/test_dag_integrity.py`, or run the DAG tests
inside the container with `make test-docker`.