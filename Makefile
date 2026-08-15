# Load .env so targets that need credentials work when invoked directly.
# Without this, `$${VAR}` in a recipe expands in the host shell — where .env was
# never sourced — and silently becomes an empty string, so `make psql` ran as
# `psql -U -d` and failed on nothing more informative than a usage error.
#
# `-include`, not `include`: on a fresh clone .env does not exist yet, and a
# hard include makes *every* target fail — including `make init`, whose entire
# job is to create .env. The leading dash keeps that bootstrap path working.
#
# Caveat inherent to letting make parse .env: a value containing an unquoted
# `#` would be truncated as a make comment, and a `$` would be read as a
# variable reference. Neither appears in .env.example's generated keys.
-include .env

# Exported individually rather than with a bare `export`, so only the values
# these recipes genuinely need reach a subprocess environment — the rest of
# .env (Fernet key, admin password, app-role passwords) stays out of it.
# db-init deliberately needs none of these: it reads credentials from the
# containers' own environment instead.
export POSTGRES_SUPERUSER
export ANALYTICS_DB_NAME
export MYSQL_ROOT_PASSWORD
export MYSQL_DATABASE

.PHONY: help init up db-init down restart logs ps test test-docker lint psql mysql-shell clean

help:
	@echo "make init         - copy .env.example to .env (won't overwrite an existing .env)"
	@echo "make up           - start all containers"
	@echo "make db-init      - apply all DDL (run once after the first 'make up')"
	@echo "make down         - stop all containers"
	@echo "make restart      - down then up"
	@echo "make logs         - follow logs for all containers"
	@echo "make ps           - show container status and health"
	@echo "make test         - run tests locally (requires local venv + requirements.txt)"
	@echo "make test-docker  - run DAG integrity tests inside the scheduler container"
	@echo "make lint         - run ruff against dags/, src/, tests/"
	@echo "make psql         - open a psql shell against the analytics database"
	@echo "make mysql-shell  - open a mysql shell against the staging database"
	@echo "make clean        - stop containers AND remove volumes (destroys local DB data)"

init:
	@test -f .env || cp .env.example .env
	@echo "Now edit .env: generate a Fernet key and secret key, set real passwords."

up:
	docker compose up -d

# Applies every DDL file, each as the least-privilege role that owns that layer.
# Credentials are read from the containers' own environment (compose already
# passes them in), so no password is written here and .env stays the only place
# they live.
#
# Every script is CREATE TABLE IF NOT EXISTS, so re-running this is safe. The
# KPI scripts additionally TRUNCATE and re-INSERT, which is exactly what the
# DAG's KPI tasks do anyway.
db-init:
	@echo "==> MySQL staging DDL (as the staging_loader role)"
	@for f in create_staging_table create_quarantine_table create_pipeline_runs_table; do \
		echo "    $$f"; \
		docker compose exec -T mysql sh -c \
			'MYSQL_PWD="$$MYSQL_PASSWORD" exec mysql -u"$$MYSQL_USER" "$$MYSQL_DATABASE"' \
			< include/sql/staging/$$f.sql || exit 1; \
	done
	@echo "==> PostgreSQL analytics DDL (as the analytics_writer role)"
	@echo "    NOT the superuser: whoever runs CREATE TABLE owns the table, and"
	@echo "    CREATE INDEX in kpi_top_routes.sql requires ownership, not grants."
	@for f in create_fact_table kpi_avg_fare_by_airline kpi_flight_offer_count_by_airline kpi_top_routes kpi_seasonal_fare_variation; do \
		echo "    $$f"; \
		docker compose exec -T postgres sh -c \
			'PGPASSWORD="$$ANALYTICS_DB_PASSWORD" exec psql -q -v ON_ERROR_STOP=1 -U "$$ANALYTICS_DB_USER" -d "$$ANALYTICS_DB_NAME"' \
			< include/sql/analytics/$$f.sql || exit 1; \
	done
	@echo "==> Schema ready. Trigger the DAG from the UI or with:"
	@echo "    docker compose exec airflow-scheduler airflow dags trigger flight_price_pipeline"

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	pytest tests/ -v

test-docker:
	docker compose exec airflow-scheduler pytest /opt/airflow/tests -v

lint:
	ruff check dags/ src/ tests/

# $${VAR} rather than $(VAR) on purpose: make echoes each recipe line before
# running it, so $(VAR) would print the expanded value — including passwords —
# straight to the terminal. Leaving expansion to the shell keeps them out of
# the transcript.
psql:
	docker compose exec postgres psql -U $${POSTGRES_SUPERUSER} -d $${ANALYTICS_DB_NAME}

# MYSQL_PWD rather than -p$${MYSQL_ROOT_PASSWORD}: a password passed as an
# argument shows up in the container's process list and makes the client print
# "Using a password on the command line interface can be insecure" on every
# invocation. `-e MYSQL_PWD` with no value forwards it from this environment.
mysql-shell:
	MYSQL_PWD="$${MYSQL_ROOT_PASSWORD}" docker compose exec -e MYSQL_PWD mysql mysql -u root $${MYSQL_DATABASE}

clean:
	docker compose down -v
