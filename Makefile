SHELL := /bin/bash
# =============================================================================
# Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
SHELLFLAGS := -e -o pipefail -c

# Silence output if VERBOSE is not set
ifndef VERBOSE
.SILENT:
endif

# Define colors and formatting
BLUE := $(shell printf "\033[1;34m")
GREEN := $(shell printf "\033[1;32m")
RED := $(shell printf "\033[1;31m")
YELLOW := $(shell printf "\033[1;33m")
NC := $(shell printf "\033[0m")
INFO := $(shell printf "$(BLUE)ℹ$(NC)")
OK := $(shell printf "$(GREEN)✓$(NC)")
WARN := $(shell printf "$(YELLOW)⚠$(NC)")
ERROR := $(shell printf "$(RED)✖$(NC)")
RUST_ENV := $(HOME)/.cargo/env

.PHONY: help
help:                                               ## Display this help text for Makefile
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


# =============================================================================
# Developer Utils
# =============================================================================
.PHONY: install-uv
install-uv:                                         ## Install latest version of uv
	@echo "${INFO} Installing uv..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
	@echo "${OK} UV installed successfully"

.PHONY: install-rust
install-rust:                                       ## Install Rust toolchain via rustup
	@echo "${INFO} Installing Rust toolchain..."
	@if command -v cargo >/dev/null 2>&1; then \
		echo "${OK} Rust toolchain already installed"; \
		exit 0; \
	fi
	@if command -v rustup >/dev/null 2>&1; then \
		rustup toolchain install stable; \
	else \
		curl -fsSL https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1; \
	fi
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		echo "${OK} Rust toolchain installed"; \
	else \
		echo "${ERROR} Rust toolchain install failed; ensure rustup is available"; \
		exit 1; \
	fi

.PHONY: install
install: clean                                      ## Install project dependencies for local development
	@echo "${INFO} Starting installation..."
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@uv python pin 3.12 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@$(MAKE) py-install
	@if ! command -v cargo >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
	fi
	@$(MAKE) rs-build
	@cd $(CURDIR) && echo "${OK} Installation complete!"

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies to the latest stable versions
	@echo "${INFO} Updating all dependencies..."
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@uv lock --upgrade
	@echo "${OK} Python dependencies updated"
	@$(MAKE) py-install
	@if ! command -v cargo >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
	fi
	@$(MAKE) rs-upgrade

.PHONY: clean
clean:                                              ## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory..."
	@rm -rf .pytest_cache .ruff_cache .hypothesis build/ dist/ .eggs/ .coverage coverage.xml coverage.json htmlcov/ src/py/tests/.pytest_cache src/py/tests/**/.pytest_cache .mypy_cache .cache/ coverage/ >/dev/null 2>&1
	@find . -name '*.egg-info' -exec rm -rf {} + >/dev/null 2>&1
	@find . -type f -name '*.egg' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyc' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyo' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*~' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '__pycache__' -exec rm -rf {} + >/dev/null 2>&1
	@find . -name '.ipynb_checkpoints' -exec rm -rf {} + >/dev/null 2>&1
	@find . -name '*.so' -path '*/vertebra/*' -exec rm -f {} + >/dev/null 2>&1
	@echo "${OK} Working directory cleaned"

.PHONY: destroy
destroy:                                            ## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment..."
	@rm -rf .venv
	@echo "${OK} Virtual environment destroyed"


# =============================================================================
# Rust Development
# =============================================================================
.PHONY: rs-build
rs-build:                                           ## Build Rust workspace
	@echo "${INFO} Building Rust components..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cargo build --manifest-path $(CURDIR)/src/rs/Cargo.toml
	@echo "${OK} Rust build complete"

.PHONY: rs-test
rs-test:                                            ## Run Rust tests
	@echo "${INFO} Cleaning shared memory..."
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running Rust tests..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo test
	@echo "${OK} Rust tests passed"

.PHONY: rs-miri
rs-miri:                                            ## Run Miri on Rust code
	@echo "${INFO} Running Miri UB detection..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@cd src/rs && cargo +nightly miri test
	@echo "${OK} Miri checks passed"

.PHONY: rs-bench
rs-bench:                                           ## Run Rust benchmarks
	@echo "${INFO} Running Rust benchmarks..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@cd src/rs && cargo bench
	@echo "${OK} Benchmarks complete"

.PHONY: rs-fmt
rs-fmt:                                             ## Format Rust code
	@echo "${INFO} Formatting Rust code..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@cd src/rs && cargo fmt
	@echo "${OK} Rust formatting complete"

.PHONY: rs-lint
rs-lint:                                            ## Run Rust linting
	@echo "${INFO} Running Rust linting..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@cd src/rs && cargo clippy --all-targets --all-features -- -D warnings
	@echo "${OK} Rust linting complete"

.PHONY: rs-clean
rs-clean:                                           ## Clean Rust build artifacts
	@echo "${INFO} Cleaning Rust artifacts..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		cd src/rs && cargo clean; \
	else \
		echo "${WARN} cargo not found; skipping Rust clean"; \
	fi
	@echo "${OK} Rust artifacts cleaned"

.PHONY: rs-upgrade
rs-upgrade:                                         ## Update Rust dependencies (Cargo.lock)
	@echo "${INFO} Updating Rust dependencies..."
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		cd src/rs && cargo update; \
		echo "${OK} Rust dependencies updated"; \
	else \
		echo "${WARN} cargo not found; skipping Rust dependency update"; \
	fi


# =============================================================================
# Python Development
# =============================================================================
.PHONY: py-install
py-install:                                         ## Install Python dependencies
	@echo "${INFO} Installing Python dependencies..."
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@uv sync --all-extras --dev
	@echo "${OK} Python dependencies installed"

.PHONY: ensure-native
ensure-native:                                      ## Ensure native extension is built
	@if ! uv run python -c "import vertebra._vertebra" 2>/dev/null; then \
		echo "${INFO} Building native extension..."; \
		uv run maturin develop --uv >/dev/null 2>&1; \
		echo "${OK} Native extension built"; \
	fi

.PHONY: py-build
py-build:                                           ## Build Python package
	@echo "${INFO} Building Python package..."
	@uv build
	@echo "${OK} Python package build complete"

.PHONY: py-lint
py-lint:                                            ## Run Python linting and formatting
	@echo "${INFO} Running Python linting and formatting..."
	@uv run ruff check --fix src/py/vertebra src/py/tests
	@uv run ruff format src/py/vertebra src/py/tests
	@echo "${OK} Python linting complete"

.PHONY: mypy
mypy:                                               ## Run mypy
	@echo "${INFO} Running mypy..."
	@uv run mypy src/py/vertebra
	@echo "${OK} Mypy checks passed"

.PHONY: type-check
type-check: mypy                                    ## Run all type checking

.PHONY: pre-commit
pre-commit:                                         ## Runs pre-commit hooks
	@echo "${INFO} Running pre-commit checks..."
	@uv run pre-commit run --color=always --all-files
	@echo "${OK} Pre-commit checks passed"

.PHONY: fix
fix:                                                ## Run formatting scripts
	@echo "${INFO} Running code formatters..."
	@uv run ruff check --fix src/py/vertebra src/py/tests
	@uv run ruff format src/py/vertebra src/py/tests
	@cd $(CURDIR) && echo "${OK} Code formatting complete"

.PHONY: fmt
fmt: fix                                            ## Alias for fix

.PHONY: lint
lint: py-lint rs-fmt                                ## Format Python/Rust with fixes

.PHONY: lint-all
lint-all: py-lint rs-lint pre-commit type-check     ## Run all linting

.PHONY: coverage
coverage:                                           ## Run the tests and generate coverage report
	@echo "${INFO} Running tests with coverage..."
	@uv run pytest src/py/tests --cov --quiet
	@uv run coverage html >/dev/null 2>&1
	@uv run coverage xml >/dev/null 2>&1
	@echo "${OK} Coverage report generated"

.PHONY: test
test:                                               ## Run unit tests (excludes integration)
	@echo "${INFO} Cleaning shared memory..."
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running unit tests..."
	@uv run pytest src/py/tests -v
	@echo "${OK} Unit tests passed"

.PHONY: integration-test
integration-test:                                   ## Run integration tests only
	@echo "${INFO} Cleaning shared memory..."
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running integration tests..."
	@uv run pytest src/py/tests -m integration
	@echo "${OK} Integration tests passed"

.PHONY: test-all
test-all: rs-test test                              ## Run all tests (Rust -> Python)
	@echo "${OK} All tests passed"

.PHONY: check-all
check-all: lint test-all coverage rs-lint           ## Run all linting, tests, and coverage checks

.PHONY: build
build: py-build rs-build                            ## Build Python and Rust artifacts
