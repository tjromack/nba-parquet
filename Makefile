.PHONY: setup test lint format run-local airflow-up airflow-down trigger-dag

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

airflow-up:
	docker compose -f infra/docker-compose.yml up -d

airflow-down:
	docker compose -f infra/docker-compose.yml down

trigger-dag:
	docker compose -f infra/docker-compose.yml exec airflow-scheduler airflow dags trigger nba_etl_pipeline
