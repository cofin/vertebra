SHELL := /bin/bash
# =============================================================================
# Variables
# =============================================================================

.DEFAULT_GOAL:=help
.ONESHELL:
.EXPORT_ALL_VARIABLES:
MAKEFLAGS += --no-print-directory
SHELLFLAGS := -e -o pipefail -c

# Configure build system
GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
CHARSET_NORMALIZER_USE_MYPYC=1

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

.PHONY: install-bun
install-bun:                                        ## Install latest version of bun
	@echo "${INFO} Installing bun..."
	@curl -fsSL https://bun.sh/install | bash >/dev/null 2>&1
	@echo "${OK} Bun installed successfully"

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
install: destroy clean                              ## Install project dependencies for local development
	@echo "${INFO} Starting fresh installation..."
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@uv python pin 3.12 >/dev/null 2>&1
	@uv venv >/dev/null 2>&1
	@$(MAKE) py-install
	@if ! command -v bun >/dev/null 2>&1; then \
		$(MAKE) install-bun; \
	fi
	@$(MAKE) js-install
	@if ! command -v cargo >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
	fi
	@$(MAKE) rs-build
	@cd $(CURDIR) && echo "${OK} Installation complete! 🎉"

.PHONY: upgrade
upgrade:                                            ## Upgrade all dependencies to the latest stable versions
	@echo "${INFO} Updating all dependencies... 🔄"
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@uv lock --upgrade
	@echo "${OK} Python dependencies updated 🔄"
	@$(MAKE) py-install
	@if ! command -v bun >/dev/null 2>&1; then \
		$(MAKE) install-bun; \
	fi
	@$(MAKE) js-upgrade
	@if ! command -v cargo >/dev/null 2>&1; then \
		$(MAKE) install-rust; \
	fi
	@$(MAKE) rs-upgrade
	@cd $(CURDIR) && NODE_OPTIONS="--no-deprecation --disable-warning=ExperimentalWarning" uv run pre-commit autoupdate
	@echo "${OK} Updated Pre-commit hooks 🔄"

.PHONY: clean
clean:                                              ## Cleanup temporary build artifacts
	@echo "${INFO} Cleaning working directory..."
	@rm -rf .pytest_cache .ruff_cache .hypothesis build/ dist/ .eggs/ .coverage coverage.xml coverage.json htmlcov/ src/py/tests/.pytest_cache src/py/tests/**/.pytest_cache .mypy_cache .unasyncd_cache/ .auto_pytabs_cache src/ts/node_modules src/ts/.bun .biome/ .cache/ coverage/ >/dev/null 2>&1
	@find . -name '*.egg-info' -exec rm -rf {} + >/dev/null 2>&1
	@find . -type f -name '*.egg' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyc' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.pyo' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*~' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '__pycache__' -exec rm -rf {} + >/dev/null 2>&1
	@find . -name '.ipynb_checkpoints' -exec rm -rf {} + >/dev/null 2>&1
	@find . -name '*.so' -path '*/vertebra/*' -exec rm -f {} + >/dev/null 2>&1
	@find . -name '*.sqlite*' -not -path './.venv/*' -not -path './.cargo/*' -exec rm -f {} + >/dev/null 2>&1
	@echo "${OK} Working directory cleaned"
	$(MAKE) docs-clean
	$(MAKE) rs-clean

.PHONY: destroy
destroy:                                            ## Destroy the virtual environment
	@echo "${INFO} Destroying virtual environment... 🗑️"
	@if command -v uv >/dev/null 2>&1; then \
		uv run pre-commit clean >/dev/null 2>&1; \
	fi
	@rm -rf .venv
	@echo "${OK} Virtual environment destroyed 🗑️"


# =============================================================================
# Rust Development
# =============================================================================
.PHONY: rs-build
rs-build:                                           ## Build Rust workspace
	@echo "${INFO} Building Rust components... 🦀"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo build
	@echo "${OK} Rust build complete"

.PHONY: rs-test
rs-test:                                            ## Run Rust tests
	@echo "${INFO} Cleaning shared memory... 🧹"
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running Rust tests... 🧪"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo test
	@echo "${OK} Rust tests passed ✨"

.PHONY: rs-miri
rs-miri:                                            ## Run Miri on Rust code
	@echo "${INFO} Running Miri UB detection... 🔍"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo +nightly miri test
	@echo "${OK} Miri checks passed ✨"

.PHONY: rs-bench
rs-bench:                                           ## Run Rust benchmarks
	@echo "${INFO} Running Rust benchmarks... 📊"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo bench
	@echo "${OK} Benchmarks complete"

.PHONY: rs-fmt
rs-fmt:                                             ## Format Rust code
	@echo "${INFO} Formatting Rust code... 🧹"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo fmt
	@echo "${OK} Rust formatting complete"

.PHONY: rs-lint
rs-lint:                                            ## Run Rust linting
	@echo "${INFO} Running Rust linting... 🔍"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "${ERROR} cargo not found; run 'make install-rust'"; \
		exit 1; \
	fi
	@cd src/rs && cargo clippy --all-targets --all-features -- -D warnings
	@echo "${OK} Rust linting complete"

.PHONY: rs-tsan
rs-tsan:                                            ## Run ThreadSanitizer on Rust tests
	@echo "${INFO} Running ThreadSanitizer... 🧪"
	@./tools/rs_tsan.sh
	@echo "${OK} ThreadSanitizer complete"

.PHONY: rs-valgrind
rs-valgrind:                                        ## Run valgrind against Rust tests
	@echo "${INFO} Running valgrind... 🧪"
	@./tools/rs_valgrind.sh
	@echo "${OK} Valgrind checks complete"

.PHONY: rs-coverage
rs-coverage:                                        ## Run Rust coverage (cargo-llvm-cov)
	@echo "${INFO} Running Rust coverage... 📊"
	@./tools/rs_coverage.sh
	@echo "${OK} Rust coverage complete"

.PHONY: rs-clean
rs-clean:                                           ## Clean Rust build artifacts
	@echo "${INFO} Cleaning Rust artifacts... 🧹"
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
	@echo "${INFO} Updating Rust dependencies... 🔄"
	@if [ -f "$(RUST_ENV)" ]; then \
		. "$(RUST_ENV)"; \
	fi
	@if command -v cargo >/dev/null 2>&1; then \
		cd src/rs && cargo update; \
		echo "${OK} Rust dependencies updated 🔄"; \
	else \
		echo "${WARN} cargo not found; skipping Rust dependency update"; \
	fi


# =============================================================================
# TypeScript/Bun Development
# =============================================================================
.PHONY: js-install
js-install:                                         ## Install TypeScript dependencies
	@echo "${INFO} Installing TypeScript dependencies... 📦"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && if [ -f bun.lockb ]; then \
		bun install --frozen-lockfile >/dev/null 2>&1; \
	else \
		bun install >/dev/null 2>&1; \
	fi
	@echo "${OK} TypeScript dependencies installed ✨"

.PHONY: js-upgrade
js-upgrade:                                         ## Upgrade TypeScript dependencies
	@echo "${INFO} Updating TypeScript dependencies... 🔄"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bun update >/dev/null 2>&1
	@echo "${OK} TypeScript dependencies updated 🔄"
.PHONY: js-dev
js-dev:                                            ## Start TypeScript development server
	@echo "${INFO} Starting TypeScript development server... 🚀"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bun run dev

.PHONY: js-build
js-build:                                          ## Build TypeScript for production
	@echo "${INFO} Building TypeScript for production... 📦"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bun run build
	@cd $(CURDIR) && echo "${OK} TypeScript build complete"

.PHONY: js-test
js-test:                                           ## Run TypeScript tests
	@echo "${INFO} Running TypeScript tests... 🧪"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bun test
	@cd $(CURDIR) && echo "${OK} TypeScript tests passed ✨"

.PHONY: js-type-check
js-type-check:                                     ## Run TypeScript type checking
	@echo "${INFO} Running TypeScript type checking... 🔍"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bunx tsc --noEmit
	@cd $(CURDIR) && echo "${OK} TypeScript type checking complete ✨"

.PHONY: js-lint
js-lint:                                           ## Run TypeScript linting and formatting
	@echo "${INFO} Running TypeScript linting and formatting... 🔍"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bunx @biomejs/biome check --write --unsafe src/ --files-ignore-unknown=true --no-errors-on-unmatched
	@cd $(CURDIR) && echo "${OK} TypeScript linting complete ✨"

.PHONY: js-format
js-format:                                         ## Format TypeScript with Biome
	@echo "${INFO} Formatting TypeScript with Biome... 🎨"
	@if ! command -v bun >/dev/null 2>&1; then \
		echo "${ERROR} bun not found; run 'make install-bun'"; \
		exit 1; \
	fi
	@cd src/ts && bunx @biomejs/biome format --write src
	@cd $(CURDIR) && echo "${OK} TypeScript formatting complete ✨"


# =============================================================================
# Python Development
# =============================================================================
.PHONY: py-install
py-install:                                         ## Install Python dependencies
	@echo "${INFO} Installing Python dependencies... 🐍"
	@if ! command -v uv >/dev/null 2>&1; then \
		$(MAKE) install-uv; \
	fi
	@set -o pipefail
	@UV_SYNC_LOG=$$(mktemp); \
		if ! uv sync --all-extras --dev 2>&1 | tee "$$UV_SYNC_LOG"; then \
			echo "${ERROR} Python dependency install failed"; \
			rm -f "$$UV_SYNC_LOG"; \
			exit 1; \
		fi; \
		if grep -Eq "Failed to build|maturin failed|returned non-zero exit status" "$$UV_SYNC_LOG"; then \
			echo "${ERROR} Python dependency install failed"; \
			rm -f "$$UV_SYNC_LOG"; \
			exit 1; \
		fi; \
		rm -f "$$UV_SYNC_LOG"
	@echo "${INFO} Building Rust extension... 🦀"
	@uv run maturin develop --uv >/dev/null 2>&1
	@echo "${OK} Python dependencies installed ✨"

.PHONY: py-build
py-build:                                           ## Build Python package
	@echo "${INFO} Building Python package... 📦"
	@uv build
	@echo "${OK} Python package build complete ✨"
.PHONY: py-lint
py-lint:                                           ## Run Python linting and formatting with unsafe fixes
	@echo "${INFO} Running Python linting and formatting with unsafe fixes... 🔍"
	@uv run ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests tools/
	@uv run ruff format src/py/vertebra src/py/tests tools/
	@echo "${OK} Python linting complete ✨"

.PHONY: mypy
mypy:                                              ## Run mypy
	@echo "${INFO} Running mypy... 🔍"
	@uv run mypy src/py/vertebra src/py/tests tools/
	@echo "${OK} Mypy checks passed ✨"

.PHONY: pyright
pyright:                                           ## Run pyright
	@echo "${INFO} Running pyright... 🔍"
	@uv run pyright src/py/vertebra src/py/tests tools/
	@echo "${OK} Pyright checks passed ✨"

.PHONY: type-check
type-check: mypy pyright                           ## Run all type checking

.PHONY: pre-commit
pre-commit:                                        ## Runs pre-commit hooks; includes ruff formatting and linting, codespell
	@echo "${INFO} Running pre-commit checks... 🔎"
	@uv run pre-commit run --color=always --all-files
	@echo "${OK} Pre-commit checks passed ✨"

.PHONY: slotscheck
slotscheck:                                        ## Run slotscheck
	@echo "${INFO} Running slots check... 🔍"
	@uv run slotscheck -m vertebra
	@echo "${OK} Slots check passed ✨"

.PHONY: fix
fix:                                               ## Run formatting scripts
	@echo "${INFO} Running code formatters... 🔧"
	@uv run ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests tools/
	@uv run ruff format src/py/vertebra src/py/tests tools/
	@$(MAKE) js-format
	@cd $(CURDIR) && echo "${OK} Code formatting complete ✨"

.PHONY: fmt
fmt:                                               ## Run formatting scripts
	@echo "${INFO} Running code formatters... 🔧"
	@uv run ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests tools/
	@uv run ruff format src/py/vertebra src/py/tests tools/
	@$(MAKE) js-format
	@cd $(CURDIR) && echo "${OK} Code formatting complete ✨"

.PHONY: lint
lint: py-lint js-format rs-fmt                     ## Format Python/TS/Rust with fixes

.PHONY: lint-all
lint-all: py-lint js-lint rs-lint pre-commit type-check slotscheck ## Run all linting

.PHONY: coverage
coverage:                                          ## Run the tests and generate coverage report
	@echo "${INFO} Running tests with coverage... 📊"
	@uv run pytest src/py/tests --cov --quiet
	@uv run coverage html >/dev/null 2>&1
	@uv run coverage xml >/dev/null 2>&1
	@echo "${OK} Coverage report generated ✨"

.PHONY: test
test:                                              ## Run unit tests (excludes integration)
	@echo "${INFO} Cleaning shared memory... 🧹"
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running unit tests... 🧪"
	@uv run pytest src/py/tests -m "not integration"
	@echo "${OK} Unit tests passed ✨"

.PHONY: integration-test
integration-test:                                 ## Run integration tests only
	@echo "${INFO} Cleaning shared memory... 🧹"
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running integration tests... 🧪"
	@uv run pytest src/py/tests -m integration
	@echo "${OK} Integration tests passed ✨"

.PHONY: py-test-all
py-test-all:                                      ## Run all Python tests (unit + integration)
	@echo "${INFO} Cleaning shared memory... 🧹"
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	@echo "${INFO} Running all Python tests... 🧪"
	@uv run pytest src/py/tests
	@echo "${OK} Python tests passed ✨"

.PHONY: test-all
test-all: rs-test js-test py-test-all              ## Run all tests (Rust -> TypeScript -> Python)
	@echo "${OK} All tests passed ✨"

.PHONY: check-all
check-all: lint test-all coverage js-type-check js-test js-build rs-test rs-lint ## Run all linting, tests, and coverage checks

.PHONY: build
build: py-build js-build rs-build                   ## Build Python, TypeScript, and Rust artifacts


# =============================================================================
# Docs
# =============================================================================
.PHONY: docs-clean
docs-clean:                                        ## Dump the existing built docs
	@echo "${INFO} Cleaning documentation build assets... 🧹"
	@rm -rf docs/_build >/dev/null 2>&1
	@echo "${OK} Documentation assets cleaned"

.PHONY: docs-serve
docs-serve: docs-clean                             ## Serve the docs locally
	@echo "${INFO} Starting documentation server... 📚"
	@uv run sphinx-autobuild docs docs/_build/ -j auto --watch src --watch docs --watch src/py/tests --watch CONTRIBUTING.rst --port 8002

.PHONY: docs
docs: docs-clean                                   ## Dump the existing built docs and rebuild them
	@echo "${INFO} Building documentation... 📝"
	@uv run sphinx-build -M html docs docs/_build/ -E -a -j auto -W --keep-going >/dev/null 2>&1
	@echo "${OK} Documentation built successfully"

.PHONY: docs-linkcheck
docs-linkcheck:                                    ## Run the link check on the docs
	@echo "${INFO} Checking documentation links... 🔗"
	@uv run sphinx-build -b linkcheck ./docs ./docs/_build -D linkcheck_ignore='http://.*','https://.*' >/dev/null 2>&1
	@echo "${OK} Link check complete"

.PHONY: docs-linkcheck-full
docs-linkcheck-full:                               ## Run the full link check on the docs
	@echo "${INFO} Running full link check... 🔗"
	@uv run sphinx-build -b linkcheck ./docs ./docs/_build -D linkcheck_anchors=0 >/dev/null 2>&1
	@echo "${OK} Full link check complete"
