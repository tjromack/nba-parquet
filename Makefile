.PHONY: setup test lint format run-local airflow-init airflow-up airflow-down airflow-logs airflow-rebuild trigger-dag dag-list

setup:
	pip install -r requirements.txt -r requirements-dev.txt

test:
	pytest tests/ -m "not integration" -v

lint:
	ruff check .
	black --check .

format:
	black .
	ruff check --fix .

run-local:
	python scripts/run_local.py

airflow-init:
	docker compose -f infra/docker-compose.yml up airflow-init

airflow-up:
	docker compose -f infra/docker-compose.yml up -d

airflow-down:
	docker compose -f infra/docker-compose.yml down

airflow-logs:
	docker compose -f infra/docker-compose.yml logs -f --tail=200 airflow-scheduler airflow-webserver

airflow-rebuild:
	docker compose -f infra/docker-compose.yml build --no-cache

trigger-dag:
	docker compose -f infra/docker-compose.yml exec airflow-scheduler airflow dags trigger nba_etl_pipeline

dag-list:
	docker compose -f infra/docker-compose.yml exec airflow-scheduler airflow dags list-import-errors
