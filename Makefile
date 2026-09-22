.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help install lint format typecheck test test-unit test-spark up down logs ps topics smoke producer web bench clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install the package with dev extras
	python -m pip install -e ".[dev]"

lint: ## Run ruff
	ruff check src tests

format: ## Auto-format with ruff
	ruff format src tests
	ruff check --fix src tests

typecheck: ## Run mypy
	mypy

test: ## Run the full test suite
	pytest

test-unit: ## Run unit tests only
	pytest tests/unit

test-spark: ## Run Spark tests inside the spark container (no local Java needed)
	./scripts/test_spark.sh

up: ## Start the streaming stack
	$(COMPOSE) up -d --build

web: ## Start the Phase 2 API + React terminal on top of the stack
	$(COMPOSE) --profile web up -d --build

down: ## Stop the stack
	$(COMPOSE) down

ps: ## Show running services
	$(COMPOSE) ps

logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

topics: ## List Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

smoke: ## Run the end-to-end smoke test
	./scripts/smoke_test.sh

bench: ## Run the benchmark suite against the running stack
	./scripts/measure.sh

producer: ## Run the producer locally (requires local Kafka on localhost:29092)
	PYTHONPATH=src TICKSTREAM_KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python -m producer.main

clean: ## Remove build/test artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
