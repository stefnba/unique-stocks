.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-integration lint lint-fix format format-check typecheck check \
        prefect-server prefect-worker \
        docker-up docker-down docker-down-volumes docker-logs docker-logs-worker docker-ps \
        db-init blocks-save setup \
        deploy deploy-dry

# Load .env if it exists so PREFECT_API_URL etc. are available to Make targets
-include .env
export

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m

help: ## Show this help
	@echo ""
	@echo "  $(BOLD)unique-stocks / apps/pipelines$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
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
db-init: ## Initialise DuckDB/MotherDuck schemas from scripts/init_db.sql (idempotent)
	@if [ -n "$(MOTHERDUCK_TOKEN)" ]; then \
		echo "Initialising MotherDuck (md:unique_stocks)..."; \
		uv run python -c "import duckdb; conn = duckdb.connect('md:unique_stocks'); conn.execute(open('scripts/init_db.sql').read())"; \
	else \
		echo "Initialising local DuckDB (unique_stocks.db)..."; \
		uv run python -c "import duckdb; conn = duckdb.connect('unique_stocks.db'); conn.execute(open('scripts/init_db.sql').read())"; \
	fi

blocks-save: ## Register Prefect blocks on the server defined in config/blocks.py (idempotent)
	uv run python -c "from config.blocks import BlockRegistry; BlockRegistry.save_all(if_exists='overwrite')"

setup: ## Full one-time setup: wait for server, init DB, save blocks, create pool, deploy
	@echo "Waiting for Prefect server at $(PREFECT_API_URL)..."
	@until curl -sf "$(PREFECT_API_URL)/health" > /dev/null 2>&1; do \
		printf "."; sleep 2; \
	done; echo " ready."
	$(MAKE) db-init
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
