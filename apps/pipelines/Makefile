.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-integration lint lint-fix format format-check typecheck check \
        smoke operational-health dashboard \
        prefect-server prefect-worker \
        docker-up docker-down docker-down-volumes docker-logs docker-logs-worker docker-logs-dashboard docker-ps \
        docker-setup \
        lake-env-guard lake-reset-local lake-migration lake-migration-status lake-ensure-database lake-migrate blocks-save setup s3-landing-zone \
        deploy deploy-dry deploy-upsert \
        dbt-install dbt-debug dbt-compile dbt-seed dbt-run dbt-test dbt-build dbt-clean dbt-run-staging dbt-run-marts \
        dbt-docs-generate dbt-docs-serve dbt-docs \
        lake-ui

# Load .env if it exists so PREFECT_API_URL etc. are available to Make targets
-include .env
export

LAKE_NAME ?=
LOCAL_LAKE_PATH ?= $(if $(strip $(LAKE_NAME)),$(strip $(LAKE_NAME)).duckdb,)
STREAMLIT_SERVER_PORT ?= 8501
LAKE_MIGRATIONS_DIR := lake/migrations
LAKE_TABLES_REF := lake.schema:ALL_TABLES
DBT_DIR := dbt
DBT_TARGET ?= dev
DBT_DUCKDB_PATH ?= $(LOCAL_LAKE_PATH)
DBT_DOCS_HOST ?= 127.0.0.1
DBT_DOCS_PORT ?= 8080
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
export UV_CACHE_DIR
UV := UV_CACHE_DIR="$(UV_CACHE_DIR)" uv
LAKE_CLI_LOG_LEVEL ?= WARNING
LAKE_UV := UV_CACHE_DIR="$(UV_CACHE_DIR)" uv -q
LAKE_PY := PIPELINE_LOG_LEVEL="$(LAKE_CLI_LOG_LEVEL)" $(LAKE_UV) run python
DBT := DBT_DUCKDB_PATH="$(DBT_DUCKDB_PATH)" LAKE_NAME="$(LAKE_NAME)" LOCAL_LAKE_PATH="$(LOCAL_LAKE_PATH)" $(UV) run dbt
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
	@awk 'BEGIN {FS = ":.*##"} /^##@ / { printf "\n  $(BOLD)%s$(RESET)\n", substr($$0, 5); next } /^[a-zA-Z0-9_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

##@ Setup
install: ## Install all deps including dev
	uv sync

lake-env-guard:
	@if [ -z "$(LAKE_NAME)" ]; then \
		echo "LAKE_NAME is required. Copy apps/pipelines/.env.example to apps/pipelines/.env or export LAKE_NAME."; \
		exit 2; \
	fi

setup: lake-env-guard ## Full one-time setup: wait for server, migrate lake, save blocks, create pool, deploy
	@echo "Waiting for Prefect server at $(PREFECT_API_URL)..."
	@until curl -sf "$(PREFECT_API_URL)/health" > /dev/null 2>&1; do \
		printf "."; sleep 2; \
	done; echo " ready."
	$(MAKE) lake-migrate
	$(MAKE) blocks-save
	uv run prefect work-pool create default-process-pool --type process --overwrite
	$(MAKE) deploy

blocks-save: ## Register Prefect blocks on the server defined in config/blocks.py (idempotent)
	uv run python -c "from config.blocks import BlockRegistry; BlockRegistry.save_all(if_exists='overwrite')"

s3-landing-zone: ## Create/update AWS S3 landing-zone resources (pass ARGS="--dry-run")
	uv run python scripts/setup_s3_landing_zone.py $(ARGS)

##@ Quality
check: format-check lint typecheck test ## Run full quality gate (CI equivalent)

test: ## Run all tests
	uv run pytest tests/ -v

test-unit: ## Run unit tests only
	uv run pytest tests/unit/ -v

lint: ## Check code style (ruff)
	uv run ruff check .

lint-fix: ## Auto-fix lint issues
	uv run ruff check . --fix

format: ## Format code (ruff)
	uv run ruff format .

format-check: ## Check formatting without modifying files
	uv run ruff format . --check

typecheck: ## Run static type checks (basedpyright)
	$(UV) run --extra dashboard basedpyright

test-integration: ## Run integration tests only
	uv run pytest tests/integration/ -v

##@ Smoke
smoke: ## Run local smoke preset (FLOW=<preset>, pass ARGS="...")
	@if [ -z "$(FLOW)" ]; then \
		$(UV) run python scripts/run_smoke.py --help; \
		exit 2; \
	fi
	$(UV) run python scripts/run_smoke.py $(FLOW) $(ARGS)

##@ Operations
operational-health: ## Check Prefect API and lake audit health (pass ARGS="...")
	$(UV) run python scripts/check_operational_health.py $(ARGS)

dashboard: ## Start the Streamlit pipeline audit dashboard
	$(UV) run --extra dashboard streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port $(STREAMLIT_SERVER_PORT) --server.headless true

##@ Local Services
prefect-server: ## Start a local Prefect server (no Docker)
	@while lsof -ti :4200 > /dev/null 2>&1; do \
		echo "Port 4200 still in use, waiting..."; sleep 2; \
	done
	uv run prefect server start

prefect-worker: ## Start a Prefect worker connected to PREFECT_API_URL
	uv run prefect worker start --pool default-process-pool

DEPLOY_DIR   := deploy
COMPOSE_BASE := docker compose -f $(DEPLOY_DIR)/docker-compose.yml
COMPOSE_DEV  := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.dev.yml

docker-up: ## Build and start the docker-dev stack (Prefect server + Postgres + worker + dashboard)
	$(COMPOSE_DEV) up -d --build --remove-orphans

docker-down: ## Stop the docker-dev stack
	$(COMPOSE_DEV) down

docker-down-volumes: ## Stop the docker-dev stack and delete persistent data (⚠ destructive)
	$(COMPOSE_DEV) down -v

docker-logs: ## Tail logs for all services
	$(COMPOSE_DEV) logs -f

docker-logs-worker: ## Tail pipelines worker logs only
	$(COMPOSE_DEV) logs -f pipelines-worker

docker-logs-dashboard: ## Tail pipelines dashboard logs only
	$(COMPOSE_DEV) logs -f pipelines-dashboard

docker-ps: ## Show docker-dev service status
	$(COMPOSE_DEV) ps

docker-setup: ## Run one-time setup inside the docker-dev pipelines container
	@echo "Waiting for docker-dev Prefect server at http://localhost:4200/api..."
	@until curl -sf "http://localhost:4200/api/health" > /dev/null 2>&1; do \
		printf "."; sleep 2; \
	done; echo " ready."
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen python -m core.lake.migration.migrate --migrations-dir "$(LAKE_MIGRATIONS_DIR)"
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen python -c "from config.blocks import BlockRegistry; BlockRegistry.save_all(if_exists='overwrite')"
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen prefect work-pool create default-process-pool --type process --overwrite
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen python scripts/sync_prefect_deployments.py

##@ Lake
# dev:        make prefect-server  (terminal 1)
#             make setup           (terminal 2)
# docker-dev: make docker-up, then make docker-setup
# prod:       PREFECT_API_URL=... MOTHERDUCK_TOKEN=... make setup
lake-reset-local: lake-env-guard ## Delete and recreate the local DuckDB lake (destructive; dev only)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		echo "Refusing to reset local lake while MOTHERDUCK_TOKEN is set."; \
		exit 1; \
	fi
	@echo "Deleting local DuckDB lake ($(LOCAL_LAKE_PATH))..."
	@rm -f "$(LOCAL_LAKE_PATH)" "$(LOCAL_LAKE_PATH).wal"
	@$(MAKE) lake-migrate

lake-migration: lake-env-guard lake-ensure-database ## Generate a lake schema migration (NAME="..." optional, EMPTY=1 for manual)
	@$(LAKE_PY) -m core.lake.migration.generate --tables "$(LAKE_TABLES_REF)" --migrations-dir "$(LAKE_MIGRATIONS_DIR)"

lake-ensure-database: lake-env-guard ## Create MotherDuck database or local DuckDB file when missing
	@$(LAKE_PY) -c "from config.settings import get_settings; from core.lake.database import ensure_lake_database; ensure_lake_database(get_settings())"

lake-migrate: lake-env-guard lake-ensure-database ## Apply pending lake schema migrations
	@$(LAKE_PY) -m core.lake.migration.migrate --migrations-dir "$(LAKE_MIGRATIONS_DIR)"

lake-migration-status: lake-env-guard lake-ensure-database ## Show pending/applied lake schema migrations without applying
	@$(LAKE_PY) -m core.lake.migration.migrate --migrations-dir "$(LAKE_MIGRATIONS_DIR)" --dry-run

lake-ui: lake-env-guard lake-ensure-database ## Open DuckDB local UI (MotherDuck if MOTHERDUCK_TOKEN is set, else local lake)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		MOTHERDUCK_TOKEN="$(MOTHERDUCK_TOKEN)" duckdb -ui "md:$(LAKE_NAME)"; \
	else \
		duckdb -ui "$(LOCAL_LAKE_PATH)"; \
	fi

##@ Deployments
deploy: ## Sync deployments: prune app orphans, then apply prefect.yaml
	$(UV) run python scripts/sync_prefect_deployments.py

deploy-dry: ## Preview manifest entries and orphaned deployments to remove
	$(UV) run python scripts/sync_prefect_deployments.py --dry-run

deploy-upsert: ## Apply prefect.yaml without removing orphaned deployments
	$(UV) run prefect --no-prompt deploy --all

deploy-dry-manifest: ## List deployments declared in prefect.yaml (entrypoints only)
	@uv run python -c "\
import yaml; \
d = yaml.safe_load(open('prefect.yaml')); \
print('Deployments in prefect.yaml:'); \
[print(f'  {x[\"name\"]:20s}  {x[\"entrypoint\"]}') for x in d['deployments']]"

##@ dbt
dbt-install: ## Install app deps including dbt
	$(UV) sync

dbt-debug: lake-env-guard lake-ensure-database ## Validate dbt project config and lake connection
	$(DBT) debug $(DBT_FLAGS)

dbt-compile: lake-env-guard lake-ensure-database ## Compile dbt models without writing lake tables
	$(DBT) compile $(DBT_FLAGS)

dbt-seed: lake-env-guard lake-ensure-database ## Load dbt seed reference data
	$(DBT) seed $(DBT_FLAGS)

dbt-run: lake-env-guard lake-ensure-database dbt-seed ## Load seeds, then run all dbt models
	$(DBT) run $(DBT_FLAGS)

dbt-test: lake-env-guard lake-ensure-database ## Run dbt data tests
	$(DBT) test $(DBT_FLAGS)

dbt-build: lake-env-guard lake-ensure-database ## Run dbt models and data tests
	$(DBT) build $(DBT_FLAGS)

dbt-clean: ## Remove dbt build artifacts
	$(DBT) clean $(DBT_FLAGS)

dbt-run-staging: lake-env-guard lake-ensure-database ## Run staging (Silver) models only
	$(DBT) run $(DBT_FLAGS) --select path:models/staging

dbt-run-marts: lake-env-guard lake-ensure-database ## Run mart (Gold) models only
	$(DBT) run $(DBT_FLAGS) --select path:models/marts

dbt-docs-generate: lake-env-guard lake-ensure-database ## Generate dbt docs with live warehouse catalog metadata
	$(DBT) docs generate $(DBT_FLAGS)

dbt-docs-serve: lake-env-guard lake-ensure-database ## Serve generated dbt docs and lineage graph
	@echo "Serving dbt docs at http://$(DBT_DOCS_HOST):$(DBT_DOCS_PORT)"
	$(DBT) docs serve $(DBT_FLAGS) --host $(DBT_DOCS_HOST) --port $(DBT_DOCS_PORT) --no-browser

dbt-docs: dbt-docs-generate ## Generate and serve dbt docs
	$(MAKE) dbt-docs-serve
