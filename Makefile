.DEFAULT_GOAL := help
.PHONY: help \
        pipelines-install pipelines-test pipelines-check pipelines-lint pipelines-typecheck \
        pipelines-worker pipelines-deploy \
        infra-up infra-down infra-logs infra-ps \
        dbt-run dbt-test dbt-compile

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
GREEN := \033[32m
CYAN  := \033[36m
DIM   := \033[2m

PIPELINES_DIR := apps/pipelines
INFRA_DIR     := infra

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
	$(MAKE) -C $(PIPELINES_DIR) worker

pipelines-setup: ## Create work pool + register all deployments (run once after first infra-up)
	$(MAKE) -C $(PIPELINES_DIR) prefect-setup

pipelines-deploy: ## Register all Prefect deployments
	$(MAKE) -C $(PIPELINES_DIR) deploy

# ── Infrastructure ─────────────────────────────────────────────────────────
infra-up: ## Start all services (Prefect server + Postgres + pipelines worker)
	docker compose -f $(INFRA_DIR)/docker-compose.yml up -d

infra-down: ## Stop all services
	docker compose -f $(INFRA_DIR)/docker-compose.yml down

infra-down-volumes: ## Stop all services AND delete persistent data (⚠ destructive)
	docker compose -f $(INFRA_DIR)/docker-compose.yml down -v

infra-logs: ## Tail logs for all services (Ctrl-C to stop)
	docker compose -f $(INFRA_DIR)/docker-compose.yml logs -f

infra-logs-pipelines: ## Tail pipeline worker logs only
	docker compose -f $(INFRA_DIR)/docker-compose.yml logs -f pipelines

infra-ps: ## Show running service status
	docker compose -f $(INFRA_DIR)/docker-compose.yml ps

# ── dbt ────────────────────────────────────────────────────────────────────
dbt-compile: ## Compile dbt models (no DB writes)
	cd dbt_project && dbt compile

dbt-run: ## Run all dbt models (Bronze → Silver → Gold)
	cd dbt_project && dbt run

dbt-test: ## Run dbt tests
	cd dbt_project && dbt test

dbt-run-staging: ## Run staging (Silver) models only
	cd dbt_project && dbt run --select staging

dbt-run-marts: ## Run mart (Gold) models only
	cd dbt_project && dbt run --select marts
