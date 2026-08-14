# Flight Fare Data Pipeline

An Airflow-orchestrated pipeline that ingests, validates, transforms, and
computes KPIs for the Flight Price Dataset of Bangladesh — a MySQL staging
layer feeding a PostgreSQL analytics layer.

Full architecture, reasoning, and all engineering decisions live in
[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) and
[`docs/engineering_decisions.md`](docs/engineering_decisions.md). This
README covers only what's needed to run it.

## Status

**Pre-implementation.** Phase 0 dataset profiling (`docs/data_profile.md`)
has not yet been run against the real CSV — see MASTER_PLAN.md for why
schema DDL, validation rules, and KPI SQL are intentionally not finalized
until that's done. The repo structure, Docker environment, and DAG skeleton
(with task bodies as explicit placeholders) are in place.

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

docker compose up -d
```

Airflow UI: http://localhost:8081 (login with the admin credentials set in `.env`)

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

`tests/test_dag_integrity.py` checks DAG structure only and is safe to run
before Phase 0 is complete — it does not execute any task's business logic.

## Next step

Phase 0 dataset profiling — see `docs/data_profile.md` for the checklist.