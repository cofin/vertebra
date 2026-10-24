SHELL := /bin/bash
# =============================================================================
# Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
.SHELLFLAGS := -eu -o pipefail -c

ifndef VERBOSE
.SILENT:
endif

# Rodete (Internal Linux) detection and overrides
ifneq ($(shell grep -s -q "rodete" /etc/os-release && echo "yes"),)
export NPM_CONFIG_REGISTRY=https://registry.npmjs.org
export PIP_INDEX_URL=https://pypi.org/simple
export UV_INDEX_URL=https://pypi.org/simple
export UV_DEFAULT_INDEX=https://pypi.org/simple
export CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
export CARGO_NET_GIT_FETCH_WITH_CLI=true
endif

export CARGO_TARGET_DIR ?= /tmp/vtb-cargo-target

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
.PHONY: setup-env
setup-env:                                          ## Configure environment (detects Rodete vs standard Linux/macOS)
	@bash tools/scripts/setup-env.sh

.PHONY: install-uv
install-uv:                                         ## Install uv package manager
	@echo "${INFO} Installing uv..."
	@curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null 2>&1
	@echo "${OK} UV installed successfully"

.PHONY: install-bun
install-bun:                                        ## Install Bun JavaScript runtime
	@echo "${INFO} Installing bun..."
	@curl -fsSL https://bun.sh/install | bash >/dev/null 2>&1
	@echo "${OK} Bun installed successfully"

.PHONY: install-rust
install-rust:                                       ## Install Rust toolchain via rustup
	@echo "${INFO} Installing Rust toolchain..."
	@curl -fsSL https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1
	@echo "${OK} Rust installed successfully"

.PHONY: install
install: destroy clean setup-env                    ## Install everything (Python + Rust + TS + native extension)
	@echo "${INFO} Starting fresh installation..."
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@if ! command -v bun >/dev/null 2>&1; then \
		$(MAKE) install-bun; \
	fi
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
		if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi; \
	fi
	@uv python pin 3.12 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@uv sync --all-extras --dev
	@$(MAKE) ts-install
	@$(MAKE) ts-build
	@$(MAKE) dev
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
upgrade: setup-env                                  ## Upgrade all dependencies (Python, Rust, TS, CI)
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
	@cd src/ts/vertebra-client && \
		NODE_OPTIONS="$${NODE_OPTIONS:+$${NODE_OPTIONS} }--localstorage-file=$${TMPDIR:-/tmp}/vertebra-vitest-localstorage" \
		bun run test
	@echo "${OK} TS tests passed"

.PHONY: ts-lint
ts-lint:                                            ## Lint TS package with auto-fixes
	@echo "${INFO} Linting TS package (Biome)..."
	@cd src/ts/vertebra-client && bunx biome check --write --unsafe .
	@echo "${OK} TS lint complete"

.PHONY: dev
dev:                                                ## Install native extensions in debug mode for development
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage)
	@uv run maturin develop --uv
	@uv run maturin develop --uv -m src/rs/vertebra-bundler/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-storage/Cargo.toml

# =============================================================================
# Build
# =============================================================================
.PHONY: build
build: ts-build                                     ## Build optimized release binaries + native extension + TS client
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage --release)
	@uv run maturin develop --uv -r
	@uv run maturin develop --uv -r -m src/rs/vertebra-bundler/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-storage/Cargo.toml
	@uv build
	@echo "${OK} Build complete"

.PHONY: build-pgo
build-pgo: ts-build                                 ## Build PGO-optimized release server and PyO3 extensions (uses PROFDATA or baseline)
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@uv run python -c "from pathlib import Path; from tools.pgo.rust_pgo import RustPgoConfig, build_optimized, build_optimized_extension; cfg = RustPgoConfig(); profdata = Path('$${PROFDATA:-.profiles/merged.profdata}'); build_optimized(cfg, profdata); build_optimized_extension(cfg, profdata)"
	@echo "${OK} PGO build complete"


# =============================================================================
# Test
# =============================================================================
.PHONY: rs-test
rs-test:                                            ## Run Rust tests (all features)
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cd src/rs && cargo test --workspace --features tls,compression
	@echo "${OK} Rust tests passed"

.PHONY: py-test
py-test: dev                                        ## Run Python tests
	@uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage
	@uv run pytest src/py/tests -v
	@echo "${OK} Python tests passed"

.PHONY: test
test: rs-test py-test ts-test                       ## Run all tests (Rust, Python, TypeScript)
	@echo "${OK} All tests passed"

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@uv run pytest src/py/tests --cov --quiet
	@uv run coverage xml >/dev/null 2>&1
	@uv run coverage html >/dev/null 2>&1
	@echo "${OK} Coverage report: htmlcov/index.html"


# =============================================================================
# Lint & Format
# =============================================================================
.PHONY: lint
lint: ts-lint                                       ## Format and lint everything with auto-fixes (Python, Rust, TS)
	@echo "${INFO} Linting and fixing Python (Ruff)..."
	@uv run ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests tools/scripts
	@uv run ruff format src/py/vertebra src/py/tests tools/scripts
	@echo "${INFO} Linting and fixing Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt --all)
	@(cd src/rs && cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets --features tls,compression -- -D warnings)
	@echo "${INFO} Running type checks (Mypy)..."
	@uv run mypy src/py/vertebra tools/scripts
	@if uv run python -c "import vertebra._vertebra, vertebra._bundler, vertebra._storage" >/dev/null 2>&1; then \
		uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage; \
	fi
	@$(MAKE) review-check
	@echo "${OK} Lint complete"

.PHONY: lint-all
lint-all:                                           ## Check formatting, linting, and types without mutating files (CI)
	@echo "${INFO} Checking TS package (Biome)..."
	@(cd src/ts/vertebra-client && bunx biome check .)
	@echo "${INFO} Checking Python (Ruff & Mypy)..."
	@uv run ruff check src/py/vertebra src/py/tests tools/scripts
	@uv run ruff format --check src/py/vertebra src/py/tests tools/scripts
	@uv run mypy src/py/vertebra tools/scripts
	@if uv run python -c "import vertebra._vertebra, vertebra._bundler, vertebra._storage" >/dev/null 2>&1; then \
		uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage; \
	fi
	@echo "${INFO} Checking Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt --all -- --check)
	@(cd src/rs && cargo clippy --workspace --all-targets --features tls,compression -- -D warnings)
	@$(MAKE) review-check
	@echo "${OK} All lint checks passed"

.PHONY: review-check
review-check:                                       ## Validate agentic review setup, skills, and manifests
	@uv run python -m tools.scripts.review_setup validate

.PHONY: review-route
review-route:                                       ## Route changed files to owning review areas and shards
	@BASE=$${BASE:-origin/main}; uv run python -m tools.scripts.review_setup route --base "$$BASE"

.PHONY: check
check: lint test coverage                           ## Everything: lint, type check, all tests, coverage


# =============================================================================
# Benchmarks
# =============================================================================
.PHONY: bench
bench: build                                        ## Run benchmarks (use TARGETS= and SCENARIOS= to customize)
	@echo "${INFO} Running benchmarks..."
	@uv run --group bench python -m tools.benchmarks run \
		-t $${TARGETS:-vtb} \
		-s $${SCENARIOS:-default} \
		$${BENCH_ARGS:-}
	@echo "${OK} Benchmarks complete"

.PHONY: bench-all
bench-all: build                                    ## Run ALL benchmarks (all targets, all scenarios)
	@echo "${INFO} Running full benchmark suite..."
	@uv run --group bench python -m tools.benchmarks run \
		-t all \
		-s full \
		$${BENCH_ARGS:-}
	@echo "${OK} Full benchmarks complete"

.PHONY: bench-ft
bench-ft:                                           ## Benchmark with free-threaded Python (FT_PYTHON=3.15t fallback 3.14t)
	@FT_PY="$${FT_PYTHON:-}"; \
	if [ -z "$$FT_PY" ]; then \
		if uv python find 3.15t >/dev/null 2>&1 || uv python install 3.15t 2>/dev/null; then \
			FT_PY="3.15t"; \
		else \
			uv python install 3.14t 2>/dev/null || true; \
			FT_PY="3.14t"; \
		fi; \
	else \
		uv python install "$$FT_PY" 2>/dev/null || true; \
	fi; \
	echo "${INFO} Preparing .venv-ft with $$FT_PY dependencies..."; \
	UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python "$$FT_PY" --no-dev --extra framework 2>&1 | tail -3; \
	echo "${INFO} Building server binary linked against $$FT_PY..."; \
	if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi; \
	PYO3_PYTHON=$$(uv python find "$$FT_PY") cargo build \
		--manifest-path src/rs/Cargo.toml \
		--release --target-dir src/rs/target-ft 2>&1 | tail -3; \
	echo "${INFO} Running free-threaded benchmarks ($$FT_PY)..."; \
	FT_PYTHON="$$FT_PY" uv run --group bench python -m tools.benchmarks run -t freethreaded -s $${SCENARIOS:-multiworker} $${BENCH_ARGS:-}
	@echo "${OK} Free-threaded benchmarks complete"

.PHONY: bench-report
bench-report:                                       ## Show latest benchmark report
	@uv run --group bench python -m tools.benchmarks report

.PHONY: bench-diff
bench-diff:                                         ## Compare latest benchmark run vs previous
	@uv run --group bench python -m tools.benchmarks compare

.PHONY: rs-bench
rs-bench:                                           ## Run Rust Criterion benchmarks
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cd src/rs && cargo bench -p vertebra-core --features compression && cargo bench -p vertebra-http --bench server_throughput

.PHONY: bench-micro
bench-micro:                                        ## Run Python micro-benchmarks
	@uv run --group bench python -m tools.benchmarks.micro

.PHONY: bench-baseline
bench-baseline:                                     ## Record benchmark baseline (NAME=pre-refactor by default)
	@$(MAKE) rs-bench
	@$(MAKE) bench-micro
	@uv run --group bench python -m tools.benchmarks.baseline record --name $${NAME:-pre-refactor}

.PHONY: bench-check
bench-check:                                        ## Compare current benchmarks against baseline (BASELINE=pre-refactor)
	@uv run --group bench python -m tools.benchmarks.baseline check --baseline $${BASELINE:-pre-refactor}

