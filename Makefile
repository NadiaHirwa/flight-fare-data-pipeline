.PHONY: help init up down restart logs ps test test-docker lint psql mysql-shell clean

help:
	@echo "make init         - copy .env.example to .env (won't overwrite an existing .env)"
	@echo "make up           - start all containers"
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

psql:
	docker compose exec postgres psql -U $${POSTGRES_SUPERUSER} -d $${ANALYTICS_DB_NAME}

mysql-shell:
	docker compose exec mysql mysql -u root -p$${MYSQL_ROOT_PASSWORD} $${MYSQL_DATABASE}

clean:
	docker compose down -v
