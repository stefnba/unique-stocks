.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-integration lint lint-fix format format-check typecheck check \
        prefect-server prefect-worker \
        docker-up docker-down docker-down-volumes docker-logs docker-logs-worker docker-ps \
        lake-init db-init blocks-save setup s3-landing-zone \
        deploy deploy-dry \
        dbt-install dbt-debug dbt-compile dbt-run dbt-test dbt-build dbt-clean dbt-run-staging dbt-run-marts

# Load .env if it exists so PREFECT_API_URL etc. are available to Make targets
-include .env
export

LOCAL_LAKE_PATH ?= unique_stocks.duckdb
LAKE_INIT_SQL := scripts/init_lake.sql
DBT_DIR := dbt
DBT_TARGET ?= dev
DBT_DUCKDB_PATH ?= $(LOCAL_LAKE_PATH)
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
export UV_CACHE_DIR
UV := UV_CACHE_DIR="$(UV_CACHE_DIR)" uv
DBT := DBT_DUCKDB_PATH="$(DBT_DUCKDB_PATH)" $(UV) run dbt
DBT_FLAGS := --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET)

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m

help: ## Show this help
	@echo ""
	@echo "  $(BOLD)unique-stocks / apps/pipelines$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

# ── Dependencies ───────────────────────────────────────────────────────────
install: ## Install all deps including dev
	uv sync

# ── Testing ────────────────────────────────────────────────────────────────
test: ## Run all tests
	uv run pytest tests/ -v

test-unit: ## Run unit tests only
	uv run pytest tests/unit/ -v

test-integration: ## Run integration tests only
	uv run pytest tests/integration/ -v

# ── Linting & formatting ───────────────────────────────────────────────────
lint: ## Check code style (ruff)
	uv run ruff check .

lint-fix: ## Auto-fix lint issues
	uv run ruff check . --fix

format: ## Format code (ruff)
	uv run ruff format .

format-check: ## Check formatting without modifying files
	uv run ruff format . --check

typecheck: ## Run static type checks (basedpyright)
	uv run basedpyright

# ── CI gate ────────────────────────────────────────────────────────────────
check: format-check lint typecheck test ## Run full quality gate (CI equivalent)

# ── Prefect — local dev ────────────────────────────────────────────────────
prefect-server: ## Start a local Prefect server (no Docker)
	@while lsof -ti :4200 > /dev/null 2>&1; do \
		echo "Port 4200 still in use, waiting..."; sleep 2; \
	done
	uv run prefect server start

prefect-worker: ## Start a Prefect worker connected to PREFECT_API_URL
	uv run prefect worker start --pool default-process-pool

# ── Docker (docker-dev only) ───────────────────────────────────────────────
DEPLOY_DIR   := deploy
COMPOSE_BASE := docker compose -f $(DEPLOY_DIR)/docker-compose.yml
COMPOSE_DEV  := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.dev.yml

docker-up: ## Start the docker-dev stack (Prefect server + Postgres + worker)
	$(COMPOSE_DEV) up -d

docker-down: ## Stop the docker-dev stack
	$(COMPOSE_DEV) down

docker-down-volumes: ## Stop the docker-dev stack and delete persistent data (⚠ destructive)
	$(COMPOSE_DEV) down -v

docker-logs: ## Tail logs for all services
	$(COMPOSE_DEV) logs -f

docker-logs-worker: ## Tail pipelines worker logs only
	$(COMPOSE_DEV) logs -f pipelines

docker-ps: ## Show docker-dev service status
	$(COMPOSE_DEV) ps

# ── One-time setup ─────────────────────────────────────────────────────────
# dev:        make prefect-server  (terminal 1)
#             make setup           (terminal 2)
# docker-dev: make docker-up, then make setup
# prod:       PREFECT_API_URL=... MOTHERDUCK_TOKEN=... make setup
lake-init: ## Initialise DuckDB/MotherDuck lake schemas (idempotent)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		echo "Initialising MotherDuck (md:unique_stocks)..."; \
		LAKE_INIT_SQL="$(LAKE_INIT_SQL)" uv run python -c "import os, duckdb; conn = duckdb.connect('md:unique_stocks'); conn.execute(open(os.environ['LAKE_INIT_SQL']).read())"; \
	else \
		echo "Initialising local DuckDB lake ($(LOCAL_LAKE_PATH))..."; \
		LOCAL_LAKE_PATH="$(LOCAL_LAKE_PATH)" LAKE_INIT_SQL="$(LAKE_INIT_SQL)" uv run python -c "import os, duckdb; conn = duckdb.connect(os.environ['LOCAL_LAKE_PATH']); conn.execute(open(os.environ['LAKE_INIT_SQL']).read())"; \
	fi

db-init: lake-init ## Alias for lake-init

blocks-save: ## Register Prefect blocks on the server defined in config/blocks.py (idempotent)
	uv run python -c "from config.blocks import BlockRegistry; BlockRegistry.save_all(if_exists='overwrite')"

s3-landing-zone: ## Create/update AWS S3 landing-zone resources (pass ARGS="--dry-run")
	uv run python scripts/setup_s3_landing_zone.py $(ARGS)

setup: ## Full one-time setup: wait for server, init lake, save blocks, create pool, deploy
	@echo "Waiting for Prefect server at $(PREFECT_API_URL)..."
	@until curl -sf "$(PREFECT_API_URL)/health" > /dev/null 2>&1; do \
		printf "."; sleep 2; \
	done; echo " ready."
	$(MAKE) lake-init
	$(MAKE) blocks-save
	uv run prefect work-pool create default-process-pool --type process --overwrite
	uv run prefect --no-prompt deploy --all

# ── Prefect — deployments ──────────────────────────────────────────────────
deploy: ## Register all deployments with the Prefect server
	uv run prefect --no-prompt deploy --all

deploy-dry: ## List deployments defined in prefect.yaml without registering
	@uv run python -c "\
import yaml; \
d = yaml.safe_load(open('prefect.yaml')); \
print('Deployments in prefect.yaml:'); \
[print(f'  {x[\"name\"]:20s}  {x[\"entrypoint\"]}') for x in d['deployments']]"

# ── dbt ────────────────────────────────────────────────────────────────────
dbt-install: ## Install app deps including dbt
	$(UV) sync

dbt-debug: ## Validate dbt project config and lake connection
	$(DBT) debug $(DBT_FLAGS)

dbt-compile: ## Compile dbt models without writing lake tables
	$(DBT) compile $(DBT_FLAGS)

dbt-run: ## Run all dbt models
	$(DBT) run $(DBT_FLAGS)

dbt-test: ## Run dbt data tests
	$(DBT) test $(DBT_FLAGS)

dbt-build: ## Run dbt models and data tests
	$(DBT) build $(DBT_FLAGS)

dbt-clean: ## Remove dbt build artifacts
	$(DBT) clean $(DBT_FLAGS)

dbt-run-staging: ## Run staging (Silver) models only
	$(DBT) run $(DBT_FLAGS) --select path:models/staging

dbt-run-marts: ## Run mart (Gold) models only
	$(DBT) run $(DBT_FLAGS) --select path:models/marts
