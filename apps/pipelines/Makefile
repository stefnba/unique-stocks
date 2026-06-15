.DEFAULT_GOAL := help
.PHONY: help install setup blocks-save s3-landing-zone lake-env-guard prefect-api-guard confirm-destructive
.PHONY: ci-local check test test-unit test-integration lint lint-fix format format-check typecheck
.PHONY: python-lint python-lint-fix python-format python-format-check sql-lint sql-format text-format text-format-check
.PHONY: flow-check operational-health dashboard prefect-limits prefect-automations prefect-server prefect-worker
.PHONY: docker-up docker-down docker-down-volumes docker-logs docker-logs-worker docker-logs-dashboard docker-ps docker-setup docker-prod-config
.PHONY: lake-reset-local lake-migration lake-migration-status lake-ensure-database lake-migrate lake-ui
.PHONY: deploy deploy-plan deploy-dry deploy-upsert deploy-dry-manifest
.PHONY: dbt-install dbt-debug dbt-compile dbt-seed dbt-run dbt-test dbt-build dbt-clean dbt-run-staging dbt-run-marts
.PHONY: dbt-docs-generate dbt-docs-serve dbt-docs

# Load .env if it exists so PREFECT_API_URL etc. are available to Make targets
-include .env
export

LAKE_MIGRATIONS_DIR := lakehouse/migrations
LAKE_TABLES_REF := lakehouse.schema:ALL_TABLES
DBT_DIR := dbt
REPO_ROOT := ../..

STREAMLIT_SERVER_PORT ?= 8501
DBT_TARGET ?= dev
DBT_DOCS_HOST ?= 127.0.0.1
DBT_DOCS_PORT ?= 8080
LAKE_CLI_LOG_LEVEL ?= WARNING
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
export UV_CACHE_DIR

UV := uv
PY := $(UV) run python
LAKE_PY := PIPELINE_LOG_LEVEL="$(LAKE_CLI_LOG_LEVEL)" $(UV) -q run python
DBT := $(UV) run dbt
DBT_FLAGS := --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR) --target $(DBT_TARGET)
PRE_COMMIT := cd $(REPO_ROOT) && $(UV) run --project apps/pipelines pre-commit

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
CYAN  := \033[36m

help: ## Show this help
	@echo ""
	@echo "  $(BOLD)unique-stocks / apps/pipelines$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^##@ / { printf "\n  $(BOLD)%s$(RESET)\n", substr($$0, 5); next } /^[a-zA-Z0-9_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""

##@ Setup
install: ## Install all deps including dev
	$(UV) sync

lake-env-guard:
	@if [ -z "$(LAKE_NAME)" ]; then \
		echo "LAKE_NAME is required. Copy apps/pipelines/.env.example to apps/pipelines/.env or export LAKE_NAME."; \
		exit 2; \
	fi

prefect-api-guard:
	@if [ -z "$${PREFECT_API_URL:-}" ]; then \
		echo "PREFECT_API_URL is required. Copy apps/pipelines/.env.example to apps/pipelines/.env or export it."; \
		exit 2; \
	fi

confirm-destructive:
	@if [ "$(CONFIRM)" != "1" ]; then \
		echo "This target deletes persistent local data. Re-run with CONFIRM=1 to continue."; \
		exit 2; \
	fi

setup: lake-env-guard prefect-api-guard ## Full one-time setup: wait for server, migrate lake, save blocks, create pool, deploy
	@echo "Waiting for Prefect server at $${PREFECT_API_URL}..."
	@deadline=$$(( $$(date +%s) + $${PREFECT_WAIT_SECONDS:-60} )); \
	until curl -sf "$${PREFECT_API_URL%/}/health" > /dev/null 2>&1; do \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo ""; \
			echo "Timed out waiting for Prefect after $${PREFECT_WAIT_SECONDS:-60}s."; \
			exit 1; \
		fi; \
		printf "."; sleep 2; \
	done; echo " ready."
	$(MAKE) lake-migrate
	$(MAKE) blocks-save
	$(UV) run prefect work-pool create default-process-pool --type process --overwrite
	$(MAKE) prefect-limits
	$(MAKE) prefect-automations
	$(MAKE) deploy

blocks-save: ## Register Prefect blocks on the server defined in control_plane/prefect/blocks.py (idempotent)
	$(UV) run pipelines-prefect-blocks

s3-landing-zone: ## Create/update AWS S3 landing-zone resources (pass ARGS="--dry-run")
	$(UV) run pipelines-s3-landing-zone $(ARGS)

##@ Quality
ci-local: ## Run the same quality checks as GitHub CI
	$(MAKE) format-check
	$(MAKE) python-lint
	$(MAKE) typecheck
	$(MAKE) test-unit
	$(MAKE) dbt-compile
	$(MAKE) docker-prod-config

check: format-check python-lint typecheck test ## Run full local quality gate

test: ## Run all tests
	$(UV) run --extra dashboard pytest tests/ -v

test-unit: ## Run unit tests only
	$(UV) run --extra dashboard pytest tests/unit/ -v

lint: python-lint sql-lint text-format-check ## Check style and linting

python-lint: ## Check Python code style
	$(UV) run ruff check .

lint-fix: python-lint-fix sql-format text-format ## Auto-fix lint issues

python-lint-fix: ## Auto-fix Python lint issues
	$(UV) run ruff check . --fix

format: python-format sql-format text-format ## Format code and docs

python-format: ## Format Python code
	$(UV) run ruff format .

format-check: python-format-check sql-lint text-format-check ## Check formatting without modifying files

python-format-check: ## Check Python formatting without modifying files
	$(UV) run ruff format . --check

sql-lint: ## Lint SQL files
	$(UV) run sqlfluff lint .

sql-format: ## Format SQL files
	$(UV) run sqlfluff fix .

text-format: ## Format Markdown, YAML, and JSON files
	$(PRE_COMMIT) run prettier-format --all-files
	$(PRE_COMMIT) run markdownlint-format --all-files

text-format-check: ## Check Markdown, YAML, and JSON formatting/linting
	$(PRE_COMMIT) run prettier-check --hook-stage manual --all-files
	$(PRE_COMMIT) run markdownlint-check --hook-stage manual --all-files

typecheck: ## Run static type checks (basedpyright)
	$(UV) run --extra dashboard basedpyright

test-integration: ## Run integration tests only
	$(UV) run --extra dashboard pytest tests/integration/ -v

##@ Flow Checks
flow-check: ## Run local flow-check preset (FLOW=<preset>, pass ARGS="...")
	@if [ -z "$(FLOW)" ]; then \
		$(UV) run pipelines-flow-check --help; \
		exit 2; \
	fi
	$(UV) run pipelines-flow-check $(FLOW) $(ARGS)

##@ Operations
operational-health: ## Check Prefect API and lake audit health (pass ARGS="...")
	$(UV) run pipelines-operational-health $(ARGS)

dashboard: ## Start the Streamlit pipeline audit dashboard
	$(UV) run --extra dashboard streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port $(STREAMLIT_SERVER_PORT) --server.headless true

prefect-limits: ## Upsert Prefect global limits and provider rate limits (pass ARGS="--plan")
	$(UV) run pipelines-prefect-limits $(ARGS)

prefect-automations: ## Upsert Prefect event automations (pass ARGS="--plan")
	$(UV) run pipelines-prefect-automations $(ARGS)

##@ Local Services
prefect-server: ## Start a local Prefect server (no Docker)
	@deadline=$$(( $$(date +%s) + $${PREFECT_PORT_WAIT_SECONDS:-30} )); \
	while lsof -ti :4200 > /dev/null 2>&1; do \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo "Port 4200 is still in use after $${PREFECT_PORT_WAIT_SECONDS:-30}s."; \
			exit 1; \
		fi; \
		echo "Port 4200 still in use, waiting..."; sleep 2; \
	done
	$(UV) run prefect server start

prefect-worker: ## Start a Prefect worker connected to PREFECT_API_URL
	$(UV) run prefect worker start --pool default-process-pool --limit $${PREFECT_WORKER_LIMIT:-1}

DEPLOY_DIR   := deploy
COMPOSE_BASE := docker compose -f $(DEPLOY_DIR)/docker-compose.yml
COMPOSE_DEV  := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.dev.yml
COMPOSE_PROD := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.prod.yml

docker-up: ## Build and start the docker-dev stack (Prefect server + Postgres + worker + dashboard)
	$(COMPOSE_DEV) up -d --build --remove-orphans

docker-down: ## Stop the docker-dev stack
	$(COMPOSE_DEV) down

docker-down-volumes: confirm-destructive ## Stop the docker-dev stack and delete persistent data (requires CONFIRM=1)
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
	@deadline=$$(( $$(date +%s) + $${PREFECT_WAIT_SECONDS:-60} )); \
	until curl -sf "http://localhost:4200/api/health" > /dev/null 2>&1; do \
		if [ $$(date +%s) -ge $$deadline ]; then \
			echo ""; \
			echo "Timed out waiting for docker-dev Prefect after $${PREFECT_WAIT_SECONDS:-60}s."; \
			exit 1; \
		fi; \
		printf "."; sleep 2; \
	done; echo " ready."
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen python -m core.lake.migration.migrate --tables "$(LAKE_TABLES_REF)" --migrations-dir "$(LAKE_MIGRATIONS_DIR)"
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen pipelines-prefect-blocks
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen prefect work-pool create default-process-pool --type process --overwrite
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen pipelines-prefect-limits
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen pipelines-prefect-automations
	$(COMPOSE_DEV) exec pipelines-worker uv run --no-dev --frozen pipelines-prefect-deployments

docker-prod-config: ## Render production Docker Compose config with test secrets
	POSTGRES_PASSWORD=test-postgres-password MOTHERDUCK_TOKEN=test-motherduck-token PREFECT_UI_API_URL=https://prefect.example.com/api $(COMPOSE_PROD) config >/dev/null

##@ Lake
# dev:        make prefect-server  (terminal 1)
#             make setup           (terminal 2)
# docker-dev: make docker-up, then make docker-setup
# prod:       PREFECT_API_URL=... MOTHERDUCK_TOKEN=... make setup
lake-reset-local: lake-env-guard confirm-destructive ## Delete and recreate the local DuckDB lake (requires CONFIRM=1)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		echo "Refusing to reset local lake while MOTHERDUCK_TOKEN is set."; \
		exit 1; \
	fi
	@lake_path="$${LOCAL_LAKE_PATH:-$(LAKE_NAME).duckdb}"; \
	if [ "$${lake_path#/}" = "$$lake_path" ]; then \
		lake_path="$(CURDIR)/$$lake_path"; \
	fi; \
	echo "Deleting local DuckDB lake ($$lake_path)..."; \
	rm -f "$$lake_path" "$$lake_path.wal"
	@$(MAKE) lake-migrate

lake-migration: lake-env-guard lake-ensure-database ## Generate a lake schema migration (NAME="..." optional, EMPTY=1 for manual)
	@$(LAKE_PY) -m core.lake.migration.generate --tables "$(LAKE_TABLES_REF)" --migrations-dir "$(LAKE_MIGRATIONS_DIR)"

lake-ensure-database: lake-env-guard ## Create MotherDuck database or local DuckDB file when missing
	@$(LAKE_PY) -c "from config.settings import get_settings; from core.lake.database import ensure_lake_database; ensure_lake_database(get_settings())"

lake-migrate: lake-env-guard lake-ensure-database ## Apply pending lake schema migrations
	@$(LAKE_PY) -m core.lake.migration.migrate --tables "$(LAKE_TABLES_REF)" --migrations-dir "$(LAKE_MIGRATIONS_DIR)"

lake-migration-status: lake-env-guard lake-ensure-database ## Show pending/applied lake schema migrations without applying
	@$(LAKE_PY) -m core.lake.migration.migrate --tables "$(LAKE_TABLES_REF)" --migrations-dir "$(LAKE_MIGRATIONS_DIR)" --dry-run

lake-ui: lake-env-guard lake-ensure-database ## Open DuckDB local UI (MotherDuck if MOTHERDUCK_TOKEN is set, else local lake)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		MOTHERDUCK_TOKEN="$(MOTHERDUCK_TOKEN)" duckdb -ui "md:$(LAKE_NAME)"; \
	else \
		lake_path="$${LOCAL_LAKE_PATH:-$(LAKE_NAME).duckdb}"; \
		duckdb -ui "$$lake_path"; \
	fi

##@ Deployments
deploy: ## Sync deployments: prune app orphans, then apply prefect.yaml
	$(UV) run pipelines-prefect-deployments

deploy-plan: ## Preview manifest entries and orphaned deployments to remove
	$(UV) run pipelines-prefect-deployments --plan

deploy-upsert: ## Apply prefect.yaml without removing orphaned deployments
	$(UV) run prefect --no-prompt deploy --all

deploy-dry-manifest: ## List deployments declared in prefect.yaml (entrypoints only)
	@$(PY) -c "\
import yaml; \
d = yaml.safe_load(open('prefect.yaml')); \
print('Deployments in prefect.yaml:'); \
[print(f'  {x[\"name\"]:20s}  {x[\"entrypoint\"]}') for x in d['deployments']]"

##@ dbt
dbt-install: install ## Install app deps including dbt

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
