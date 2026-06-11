.DEFAULT_GOAL := help
.PHONY: help \
        pipelines-install pipelines-test pipelines-check pipelines-lint pipelines-typecheck pipelines-dashboard \
        pipelines-worker pipelines-setup pipelines-deploy pipelines-ci \
        prod-guard infra-up infra-up-prod infra-down infra-down-prod infra-down-volumes \
        infra-logs infra-logs-prod infra-logs-pipelines infra-logs-dashboard infra-ps infra-ps-prod \
        dbt-install dbt-debug dbt-compile dbt-run dbt-test dbt-build dbt-clean dbt-run-staging dbt-run-marts \
        dbt-docs-generate dbt-docs-serve dbt-docs

# ── Colours ────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m
CYAN  := \033[36m

# ── Directories ────────────────────────────────────────────────────────────
PIPELINES_DIR  := apps/pipelines
DEPLOY_DIR     := $(PIPELINES_DIR)/deploy
PROD_COMPOSE_PROJECT ?= unique-stocks-prod
COMPOSE_PROD   := docker compose -p $(PROD_COMPOSE_PROJECT) -f $(DEPLOY_DIR)/docker-compose.yml -f $(DEPLOY_DIR)/docker-compose.prod.yml


# ── Help ────────────────────────────────────────────────────────────────────
help: ## Show this help
	@echo ""
	@echo "  $(BOLD)unique-stocks monorepo$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^##@ / { printf "\n  $(BOLD)%s$(RESET)\n", substr($$0, 5); next } /^[a-zA-Z0-9_-]+:.*?##/ { printf "  $(CYAN)%-22s$(RESET) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "  Run $(CYAN)make -C apps/pipelines help$(RESET) for the full pipelines command list."
	@echo ""

##@ Pipelines
# ── Pipelines ──────────────────────────────────────────────────────────────
pipelines-install: ## Install pipeline deps
	$(MAKE) -C $(PIPELINES_DIR) install

pipelines-test: ## Run all pipeline tests
	$(MAKE) -C $(PIPELINES_DIR) test

pipelines-check: ## Run full local pipeline quality gate
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

pipelines-ci: ## Run the same pipeline checks as GitHub CI
	$(MAKE) -C $(PIPELINES_DIR) ci-local

##@ Infrastructure (Docker)
# ── Infrastructure ─────────────────────────────────────────────────────────
infra-up: ## Start dev stack (Docker) — Prefect server + Postgres + worker + dashboard
	$(MAKE) -C $(PIPELINES_DIR) docker-up

prod-guard:
	@if [ "$(CONFIRM_PROD)" != "1" ]; then \
		echo "Production infrastructure targets require CONFIRM_PROD=1."; \
		echo "Example: CONFIRM_PROD=1 make infra-up-prod"; \
		exit 2; \
	fi

infra-up-prod: prod-guard ## Start production stack (requires CONFIRM_PROD=1)
	$(COMPOSE_PROD) up -d --build --remove-orphans

infra-down: ## Stop all services
	$(MAKE) -C $(PIPELINES_DIR) docker-down

infra-down-prod: prod-guard ## Stop production stack (requires CONFIRM_PROD=1)
	$(COMPOSE_PROD) down

infra-down-volumes: ## Stop dev stack AND delete persistent data (requires CONFIRM=1)
	$(MAKE) -C $(PIPELINES_DIR) docker-down-volumes

infra-logs: ## Tail logs for all services (Ctrl-C to stop)
	$(MAKE) -C $(PIPELINES_DIR) docker-logs

infra-logs-prod: ## Tail production stack logs (Ctrl-C to stop)
	$(COMPOSE_PROD) logs -f

infra-logs-pipelines: ## Tail pipeline worker logs only
	$(MAKE) -C $(PIPELINES_DIR) docker-logs-worker

infra-logs-dashboard: ## Tail pipeline dashboard logs only
	$(MAKE) -C $(PIPELINES_DIR) docker-logs-dashboard

infra-ps: ## Show running service status
	$(MAKE) -C $(PIPELINES_DIR) docker-ps

infra-ps-prod: ## Show production stack service status
	$(COMPOSE_PROD) ps

##@ dbt
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
