SHELL := /bin/bash
# =============================================================================
# Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
SHELLFLAGS := -e -o pipefail -c

ifndef VERBOSE
.SILENT:
endif

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
# Setup
# =============================================================================
.PHONY: install
install: clean ts-install dev                       ## Install everything (Python + Rust + TS + native extension)
	@echo "${INFO} Starting installation..."
	@if ! command -v uv >/dev/null 2>&1; then \
		curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1; \
	fi
	@uv python pin 3.12 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@uv sync --all-extras --dev
	@if ! command -v cargo >/dev/null 2>&1; then \
		curl -fsSL https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1; \
	fi
	@echo "${OK} Installation complete"

.PHONY: clean
clean:                                              ## Remove build artifacts and caches
	@echo "${INFO} Cleaning..."
	@# Python caches and artifacts
	@rm -rf .pytest_cache .ruff_cache .mypy_cache .cache .hypothesis .coverage coverage.xml coverage.json htmlcov .sqlspec >/dev/null 2>&1
	@find . -name '*.egg-info' -o -name '*.pyc' -o -name '*.pyo' -o -name '__pycache__' | xargs rm -rf 2>/dev/null || true
	@find . -name '*.so' -path '*/vertebra/*' -exec rm -f {} + >/dev/null 2>&1
	@rm -rf build/ dist/ >/dev/null 2>&1
	@# Rust artifacts
	@rm -rf src/rs/target src/rs/target-ft >/dev/null 2>&1
	@# TypeScript/JS artifacts
	@rm -rf src/ts/vertebra-client/dist .biome src/py/vertebra/server/_js/bundle/ >/dev/null 2>&1
	@echo "${OK} Clean"

.PHONY: destroy
destroy:                                            ## Destroy virtual environment
	@rm -rf .venv && echo "${OK} Virtual environment destroyed"

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies (Python, Rust, TS, CI)
	@echo "${INFO} Upgrading Python dependencies (uv)..."
	@uv lock --upgrade && uv sync --all-extras --dev
	@echo "${INFO} Upgrading Rust dependencies (cargo)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo update)
	@echo "${INFO} Upgrading TS dependencies (bun)..."
	@(cd src/ts/vertebra-client && bun update)
	@echo "${INFO} Upgrading CI hooks (pre-commit)..."
	@uv run pre-commit autoupdate
	@echo "${OK} All dependencies upgraded"


.PHONY: ts-install
ts-install:                                         ## Install TS dependencies
	@echo "${INFO} Installing TS dependencies (Bun)..."
	@cd src/ts/vertebra-client && bun install
	@echo "${OK} TS installation complete"

.PHONY: ts-build
ts-build:                                           ## Build TS client package
	@echo "${INFO} Building @vertebra/client..."
	@cd src/ts/vertebra-client && bun run build
	@echo "${OK} TS build complete"

.PHONY: ts-test
ts-test:                                            ## Run TS unit tests
	@echo "${INFO} Running TS tests (Vitest)..."
	@cd src/ts/vertebra-client && bun run test
	@echo "${OK} TS tests passed"

.PHONY: ts-lint
ts-lint:                                            ## Lint TS package with auto-fixes
	@echo "${INFO} Linting TS package (Biome)..."
	@cd src/ts/vertebra-client && bun run lint:fix
	@echo "${OK} TS lint complete"

.PHONY: dev
dev:                                                ## Install native extension in debug mode for development
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo build --workspace --exclude vertebra-py)
	@uv run maturin develop --uv 2>&1 | grep -E "Compiling|Finished|Built wheel|Installed|error\[" | tail -3

# =============================================================================
# Build
# =============================================================================
.PHONY: build
build: ts-build                                     ## Build optimized release binaries + native extension + TS client
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo build --workspace --exclude vertebra-py --release)
	@uv run maturin develop --uv -r 2>&1 | grep -E "Compiling|Finished|Built wheel|Installed|error\[" | tail -3
	@uv build
	@echo "${OK} Build complete"


# =============================================================================
# Test
# =============================================================================
.PHONY: test
test: dev                                           ## Run Python tests
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@uv run pytest src/py/tests -v
	@echo "${OK} Tests passed"

.PHONY: rs-test
rs-test:                                            ## Run Rust tests
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cd src/rs && cargo test
	@echo "${OK} Rust tests passed"

.PHONY: test-all
test-all: rs-test test                              ## Run all tests (Rust then Python)

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@uv run pytest src/py/tests --cov --quiet
	@uv run coverage html >/dev/null 2>&1
	@echo "${OK} Coverage report: htmlcov/index.html"


# =============================================================================
# Lint & Format
# =============================================================================
.PHONY: lint
lint: ts-lint                                       ## Format and lint everything with auto-fixes (Python, Rust, TS, Markdown)
	@echo "${INFO} Linting and fixing Python (Ruff)..."
	@uv run ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests
	@uv run ruff format src/py/vertebra src/py/tests
	@echo "${INFO} Linting and fixing Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt)
	@(cd src/rs && cargo clippy --fix --allow-dirty --allow-staged --workspace --exclude vertebra-py --all-targets -- -D warnings >/dev/null 2>&1)
	@echo "${OK} Lint complete"

.PHONY: lint-all
lint-all: lint                                      ## Full lint + type checks
	@echo "${INFO} Running type checks (Mypy)..."
	@uv run mypy src/py/vertebra
	@echo "${INFO} Running pre-commit hooks..."
	@uv run pre-commit run --color=always --all-files
	@echo "${OK} All checks passed"

.PHONY: check-all
check-all: lint-all test-all coverage               ## Everything: lint, type check, all tests, coverage


# =============================================================================
# Benchmarks
# =============================================================================
.PHONY: bench
bench: build                                        ## Run benchmarks (use TARGETS= and SCENARIOS= to customize)
	@echo "${INFO} Running benchmarks..."
	@uv run python -m tools.benchmarks run \
		-t $${TARGETS:-vtb} \
		-s $${SCENARIOS:-default} \
		$${BENCH_ARGS:-}
	@echo "${OK} Benchmarks complete"

.PHONY: bench-all
bench-all: build                                    ## Run ALL benchmarks (all targets, all scenarios)
	@echo "${INFO} Running full benchmark suite..."
	@uv run python -m tools.benchmarks run \
		-t all \
		-s full \
		$${BENCH_ARGS:-}
	@echo "${OK} Full benchmarks complete"

.PHONY: bench-ft
bench-ft:                                           ## Benchmark with Python 3.14t free-threaded (auto-installs + builds)
	@echo "${INFO} Installing Python 3.14t..."
	@uv python install 3.14t 2>/dev/null || true
	@echo "${INFO} Preparing .venv-ft with 3.14t dependencies..."
	@UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python 3.14t --no-dev --extra framework 2>&1 | tail -3
	@echo "${INFO} Building server binary linked against libpython3.14t..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYO3_PYTHON=$$(uv python find 3.14t) cargo build \
		--manifest-path src/rs/Cargo.toml \
		--release --target-dir src/rs/target-ft 2>&1 | tail -3
	@echo "${INFO} Running free-threaded benchmarks..."
	@uv run python -m tools.benchmarks run -t freethreaded -s $${SCENARIOS:-multiworker} $${BENCH_ARGS:-}
	@echo "${OK} Free-threaded benchmarks complete"

.PHONY: bench-report
bench-report:                                       ## Show latest benchmark report
	@uv run python -m tools.benchmarks report

.PHONY: bench-diff
bench-diff:                                         ## Compare latest benchmark run vs previous
	@uv run python -m tools.benchmarks compare
