# Convenience wrappers. Everything here is a thin shell around docker compose.
SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "created .env")

.PHONY: up
up: env ## Build and start the whole platform
	$(COMPOSE) up -d --build
	@echo "wallet-service      http://localhost:8000/docs"
	@echo "blockchain-service  http://localhost:8001/docs"

.PHONY: down
down: ## Stop everything
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop everything and delete the volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail application logs
	$(COMPOSE) logs -f wallet-api wallet-worker blockchain-api blockchain-worker

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: demo
demo: ## Run the end-to-end scenario (2 users, 1000 deposit, 250 transfer)
	./scripts/demo.sh

.PHONY: test
test: env ## Run the full test suite in docker (unit + integration)
	$(COMPOSE) --profile test run --rm tests pytest -q

.PHONY: test-unit
test-unit: env ## Run unit tests only (no infrastructure needed)
	$(COMPOSE) --profile test run --rm tests pytest -q tests/unit

.PHONY: test-integration
test-integration: env ## Run integration tests only (postgres + redis)
	$(COMPOSE) --profile test run --rm tests pytest -q tests/integration

.PHONY: migrate
migrate: ## Apply database migrations
	$(COMPOSE) run --rm wallet-migrate
	$(COMPOSE) run --rm blockchain-migrate

.PHONY: lint
lint: ## Static checks
	$(COMPOSE) --profile test run --rm tests ruff check .

.PHONY: openapi
openapi: ## Dump both OpenAPI documents to docs/
	./scripts/dump-openapi.sh
