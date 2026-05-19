# Music Streaming Pipeline — unified task runner.
# All Python tooling executes inside uv-managed venv via `uv run`.
# All Terraform targets run from the corresponding environment directory.

SHELL := /bin/bash

# Default target — show help when `make` is invoked without arguments.
.DEFAULT_GOAL := help

.PHONY: help install lint fmt typecheck test test-all tf-validate tf-validate-prod \
        tf-plan-dev tf-apply-dev tf-plan-prod upload-data query-kpis zip-utils \
        pre-commit-install pre-commit-run clean

help: ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------------------------------------------------------------------------
# Python environment + quality gates
# ---------------------------------------------------------------------------
install: ## Sync dependencies (runtime + dev group) via uv
	uv sync --all-extras

lint: ## Run ruff lint checks (no fixes)
	uv run ruff check src/ tests/

fmt: ## Auto-format with ruff (writes changes)
	uv run ruff format src/ tests/

typecheck: ## Run mypy on src/ (strict mode)
	uv run mypy src/

test: ## Run unit tests with coverage (terminal report)
	uv run pytest tests/unit/ --cov=src --cov-report=term-missing

test-all: ## Run all tests (unit + integration) and write coverage XML
	uv run pytest tests/ --cov=src --cov-report=xml --cov-report=term-missing

pre-commit-install: ## Install pre-commit git hooks
	uv run pre-commit install

pre-commit-run: ## Run all pre-commit hooks against every tracked file
	uv run pre-commit run --all-files

# ---------------------------------------------------------------------------
# Terraform targets
# ---------------------------------------------------------------------------
tf-validate: ## terraform init -backend=false && validate (dev environment)
	cd terraform/environments/dev && terraform init -backend=false && terraform validate

tf-validate-prod: ## terraform init -backend=false && validate (prod environment)
	cd terraform/environments/prod && terraform init -backend=false && terraform validate

tf-plan-dev: ## Plan dev environment
	cd terraform/environments/dev && terraform plan

tf-apply-dev: ## Apply dev environment (auto-approve)
	cd terraform/environments/dev && terraform apply -auto-approve

tf-plan-prod: ## Plan prod environment (no auto-apply — manual approval required)
	cd terraform/environments/prod && terraform plan

# ---------------------------------------------------------------------------
# Operational scripts
# ---------------------------------------------------------------------------
upload-data: ## Upload sample streams CSV to S3 raw/streams/ prefix
	bash scripts/upload_sample_data.sh

query-kpis: ## Run DynamoDB query examples against the deployed KPI table
	bash scripts/query_dynamodb.sh

zip-utils: ## Package src/utils/ as utils.zip for ad-hoc Glue --extra-py-files inspection
	cd src && zip -r ../utils.zip utils/

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------
clean: ## Remove caches, build artifacts, and coverage data
	find . -type d \( -name __pycache__ -o -name .mypy_cache -o -name .pytest_cache -o -name .ruff_cache -o -name .coverage -o -name htmlcov \) -prune -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name '*.egg-info' -prune -exec rm -rf {} + 2>/dev/null || true
	rm -f utils.zip coverage.xml .coverage
