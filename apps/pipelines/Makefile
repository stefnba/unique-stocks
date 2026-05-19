.DEFAULT_GOAL := help
.PHONY: help install test test-unit test-integration lint lint-fix format format-check typecheck check \
        worker deploy deploy-dry prefect-server prefect-setup

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

# ── Prefect — deployments ──────────────────────────────────────────────────
prefect-setup: ## Create work pool + register all deployments (run once after first infra-up)
	@echo "Waiting for Prefect server to be ready..."
	@until curl -sf "$(PREFECT_API_URL)/health" > /dev/null 2>&1; do \
		printf "."; sleep 2; \
	done; echo " ready."
	uv run prefect work-pool create default-process-pool --type process --overwrite
	uv run prefect --no-prompt deploy --all

deploy: ## Register all deployments with the Prefect server
	uv run prefect --no-prompt deploy --all

deploy-dry: ## List deployments defined in prefect.yaml without registering
	@uv run python -c "\
import yaml; \
d = yaml.safe_load(open('prefect.yaml')); \
print('Deployments in prefect.yaml:'); \
[print(f'  {x[\"name\"]:20s}  {x[\"entrypoint\"]}') for x in d['deployments']]"
