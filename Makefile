.DEFAULT_GOAL := help
.PHONY: help \
        pipelines-install pipelines-test pipelines-check pipelines-lint pipelines-typecheck pipelines-dashboard \
        pipelines-worker pipelines-deploy \
        infra-up infra-up-prod infra-down infra-down-volumes infra-logs infra-logs-pipelines infra-logs-dashboard infra-ps \
        dbt-install dbt-debug dbt-compile dbt-run dbt-test dbt-build dbt-clean dbt-run-staging dbt-run-marts \
        dbt-docs-generate dbt-docs-serve dbt-docs

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m
DIM   := \033[2m

# ── Directories ────────────────────────────────────────────────────────────
PIPELINES_DIR  := apps/pipelines
DEPLOY_DIR     := $(PIPELINES_DIR)/deploy
COMPOSE_BASE   := docker compose -f $(DEPLOY_DIR)/docker-compose.yml
COMPOSE_DEV    := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.dev.yml
COMPOSE_PROD   := $(COMPOSE_BASE) -f $(DEPLOY_DIR)/docker-compose.prod.yml


# ── Help ────────────────────────────────────────────────────────────────────
help: ## Show this help
	@echo ""
	@echo "  $(BOLD)unique-stocks monorepo$(RESET)"
	@echo ""
	@echo "  $(DIM)── Pipelines ─────────────────────────────────────────────────$(RESET)"
	@awk 'BEGIN {FS = ":.*##"} /^pipelines-[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-28s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "  $(DIM)── Infrastructure (Docker) ────────────────────────────────────$(RESET)"
	@awk 'BEGIN {FS = ":.*##"} /^infra-[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-28s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "  $(DIM)── dbt ─────────────────────────────────────────────────────────$(RESET)"
	@awk 'BEGIN {FS = ":.*##"} /^dbt-[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-28s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "  Run $(CYAN)make -C apps/pipelines help$(RESET) for the full pipelines command list."
	@echo ""

# ── Pipelines ──────────────────────────────────────────────────────────────
pipelines-install: ## Install pipeline deps
	$(MAKE) -C $(PIPELINES_DIR) install

pipelines-test: ## Run all pipeline tests
	$(MAKE) -C $(PIPELINES_DIR) test

pipelines-check: ## Run full pipeline quality gate (format + lint + typecheck + test)
	$(MAKE) -C $(PIPELINES_DIR) check

pipelines-lint: ## Lint pipeline code
	$(MAKE) -C $(PIPELINES_DIR) lint

pipelines-typecheck: ## Type-check pipeline code
	$(MAKE) -C $(PIPELINES_DIR) typecheck

pipelines-worker: ## Start a Prefect worker (reads PREFECT_API_URL from env)
	$(MAKE) -C $(PIPELINES_DIR) prefect-worker

pipelines-dashboard: ## Start the Streamlit pipeline audit dashboard
	$(MAKE) -C $(PIPELINES_DIR) dashboard

pipelines-setup: ## Full one-time setup: init lake, save blocks, create pool, deploy
	$(MAKE) -C $(PIPELINES_DIR) setup

pipelines-deploy: ## Register all Prefect deployments
	$(MAKE) -C $(PIPELINES_DIR) deploy

# ── Infrastructure ─────────────────────────────────────────────────────────
infra-up: ## Start dev stack (Docker) — Prefect server + Postgres + worker + dashboard
	$(COMPOSE_DEV) up -d

infra-up-prod: ## Start production stack
	$(COMPOSE_PROD) up -d

infra-down: ## Stop all services
	$(COMPOSE_DEV) down

infra-down-volumes: ## Stop all services AND delete persistent data (⚠ destructive)
	$(COMPOSE_DEV) down -v

infra-logs: ## Tail logs for all services (Ctrl-C to stop)
	$(COMPOSE_DEV) logs -f

infra-logs-pipelines: ## Tail pipeline worker logs only
	$(COMPOSE_DEV) logs -f pipelines-worker

infra-logs-dashboard: ## Tail pipeline dashboard logs only
	$(COMPOSE_DEV) logs -f pipelines-dashboard

infra-ps: ## Show running service status
	$(COMPOSE_DEV) ps

# ── dbt ────────────────────────────────────────────────────────────────────
dbt-install: ## Install dbt deps
	$(MAKE) -C $(PIPELINES_DIR) dbt-install

dbt-debug: ## Validate dbt project config and lake connection
	$(MAKE) -C $(PIPELINES_DIR) dbt-debug

dbt-compile: ## Compile dbt models (no DB writes)
	$(MAKE) -C $(PIPELINES_DIR) dbt-compile

dbt-run: ## Run all dbt models (Bronze → Silver → Gold)
	$(MAKE) -C $(PIPELINES_DIR) dbt-run

dbt-test: ## Run dbt tests
	$(MAKE) -C $(PIPELINES_DIR) dbt-test

dbt-build: ## Run dbt models and data tests
	$(MAKE) -C $(PIPELINES_DIR) dbt-build

dbt-clean: ## Remove dbt build artifacts
	$(MAKE) -C $(PIPELINES_DIR) dbt-clean

dbt-run-staging: ## Run staging (Silver) models only
	$(MAKE) -C $(PIPELINES_DIR) dbt-run-staging

dbt-run-marts: ## Run mart (Gold) models only
	$(MAKE) -C $(PIPELINES_DIR) dbt-run-marts

dbt-docs-generate: ## Generate dbt docs and lineage artifacts
	$(MAKE) -C $(PIPELINES_DIR) dbt-docs-generate

dbt-docs-serve: ## Serve generated dbt docs and lineage graph
	$(MAKE) -C $(PIPELINES_DIR) dbt-docs-serve

dbt-docs: ## Generate and serve dbt docs
	$(MAKE) -C $(PIPELINES_DIR) dbt-docs
