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

export CARGO_TARGET_DIR ?= $(CURDIR)/src/rs/target
export PATH := $(CURDIR)/.venv/bin:$(CURDIR)/.venv/Scripts:$(HOME)/.bun/bin:$(HOME)/.local/bin:$(PATH)
ifneq ($(wildcard $(CURDIR)/.venv/bin/python),)
export PYO3_PYTHON ?= $(CURDIR)/.venv/bin/python
else ifneq ($(wildcard $(CURDIR)/.venv/Scripts/python.exe),)
export PYO3_PYTHON ?= $(CURDIR)/.venv/Scripts/python.exe
else ifneq ($(origin PYO3_PYTHON),undefined)
export PYO3_PYTHON
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
	@uv venv --clear >/dev/null 2>&1
	@uv sync --all-extras --dev --group doc
	@$(MAKE) ts-install
	@$(MAKE) ts-build
	@$(MAKE) dev
	@echo "${OK} Installation complete"

.PHONY: clean
clean:                                              ## Remove build artifacts and caches
	@echo "${INFO} Cleaning..."
	@# Python caches and artifacts
	@rm -rf .pytest_cache .ruff_cache .mypy_cache .cache site .hypothesis .coverage coverage.xml coverage.json htmlcov .sqlspec >/dev/null 2>&1
	@find . -name '*.egg-info' -o -name '*.pyc' -o -name '*.pyo' -o -name '__pycache__' | xargs rm -rf 2>/dev/null || true
	@find src/py -depth -type d -empty -delete 2>/dev/null || true
	@find . -name '*.so' -path '*/vertebra/*' -exec rm -f {} + >/dev/null 2>&1
	@rm -rf build/ dist/ >/dev/null 2>&1
	@# Rust artifacts
	@rm -rf src/rs/target src/rs/target-ft .profiles tools/pgo/.profiles >/dev/null 2>&1
	@# TypeScript/JS artifacts
	@rm -rf src/ts/vertebra-client/dist .biome src/py/vertebra/server/_js/bundle/ >/dev/null 2>&1
	@echo "${OK} Clean"

.PHONY: destroy
destroy:                                            ## Destroy virtual environment
	@rm -rf .venv && echo "${OK} Virtual environment destroyed"

.PHONY: upgrade
upgrade: setup-env                                  ## Upgrade all dependencies (Python, Rust, TS, CI)
	@echo "${INFO} Upgrading Python dependencies (uv)..."
	@uv lock --upgrade && uv sync --all-extras --dev --group doc
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
	@rm -f "$(CARGO_TARGET_DIR)/release/vertebra-server"
	@(cd src/rs && $(if $(PYO3_PYTHON),PYO3_PYTHON="$$(realpath "$(PYO3_PYTHON)")",) cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage --exclude vertebra-mysql --exclude vertebra-bigquery --exclude vertebra-mssql --exclude vertebra-spanner --exclude vertebra-duckdb --exclude vertebra-oracle --exclude vertebra-postgres --exclude vertebra-sqlite --exclude vertebra-redis --exclude vertebra-valkey)
	@uv run maturin develop --uv
	@uv run maturin develop --uv -m src/rs/vertebra-bundler/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-storage/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-mysql/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-bigquery/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-mssql/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-spanner/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-oracle/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-postgres/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-sqlite/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-redis/Cargo.toml
	@uv run maturin develop --uv -m src/rs/vertebra-valkey/Cargo.toml

# =============================================================================
# Build
# =============================================================================
.PHONY: build
build: ts-build                                     ## Build optimized release binaries + native extension + TS client
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && $(if $(PYO3_PYTHON),PYO3_PYTHON="$(PYO3_PYTHON)",) cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage --exclude vertebra-mysql --exclude vertebra-bigquery --exclude vertebra-mssql --exclude vertebra-spanner --exclude vertebra-duckdb --exclude vertebra-oracle --exclude vertebra-postgres --exclude vertebra-sqlite --exclude vertebra-redis --exclude vertebra-valkey --release --features vertebra-http/python,vertebra-http/tls,vertebra-http/compression,vertebra-http/http3)
	@uv run maturin develop --uv -r
	@uv run maturin develop --uv -r -m src/rs/vertebra-bundler/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-storage/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-mysql/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-bigquery/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-mssql/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-spanner/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-duckdb/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-oracle/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-postgres/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-sqlite/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-redis/Cargo.toml
	@uv run maturin develop --uv -r -m src/rs/vertebra-valkey/Cargo.toml
	@uv build
	@echo "${OK} Build complete"

.PHONY: build-pgo
build-pgo: ts-build                                 ## Build PGO-optimized release server and PyO3 extensions (two-phase or PROFDATA=<path>)
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@if [ -n "$${PROFDATA:-}" ]; then \
		uv run python -m tools.pgo.rust_pgo --profdata "$$PROFDATA" --use-existing; \
	else \
		uv run python -m tools.pgo.rust_pgo; \
	fi
	@echo "${OK} PGO build complete"


# =============================================================================
# Test
# =============================================================================
.PHONY: rs-test
rs-test:                                            ## Run Rust tests (all features + non-http3 fail-loud check)
	@rm -rf /dev/shm/test-* /dev/shm/*vertebra* 2>/dev/null || true
	if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	py_extra_paths="$(CURDIR)/src/py"
	if [ -n "$${PYO3_PYTHON:-}" ] && [ -x "$${PYO3_PYTHON}" ]; then
		export PYTHONHOME="$$("$${PYO3_PYTHON}" -c 'import sys; print(sys.base_prefix)')"
		py_site="$$("$${PYO3_PYTHON}" -c 'import os, site; print(os.pathsep.join(site.getsitepackages()))')"
		if [ -n "$${py_site}" ]; then
			py_extra_paths="$${py_extra_paths}:$${py_site}"
		fi
		py_libdir="$$("$${PYO3_PYTHON}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
		if [ -n "$${py_libdir}" ]; then
			export LD_LIBRARY_PATH="$${py_libdir}$${LD_LIBRARY_PATH:+:$${LD_LIBRARY_PATH}}"
			export DYLD_FALLBACK_LIBRARY_PATH="$${py_libdir}$${DYLD_FALLBACK_LIBRARY_PATH:+:$${DYLD_FALLBACK_LIBRARY_PATH}}"
		fi
		export PYO3_PYTHON="$$(realpath "$${PYO3_PYTHON}")"
	fi
	export PYTHONPATH="$${py_extra_paths}$${PYTHONPATH:+:$${PYTHONPATH}}"
	cd src/rs
	cargo test --workspace --features tls,http3,compression,gcp
	cargo test -p vertebra-http --features tls,compression --lib config::tests
	cargo test -p vertebra-http --features tls,compression --test it test_run_with_listener_fails_loud_without_http3_feature
	echo "${OK} Rust tests passed"

.PHONY: py-test
py-test: dev                                        ## Run Python tests
	@uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._mysql vertebra._bigquery vertebra._mssql vertebra._spanner vertebra._duckdb vertebra._oracle vertebra._postgres vertebra._sqlite vertebra._redis vertebra._valkey
	@uv run --no-sync pytest $(PYTEST_ARGS) src/py/tests -v
	@echo "${OK} Python tests passed"

.PHONY: stubtest
stubtest:                                           ## Check native extension .pyi stubs against already-built extensions
	@uv run --no-sync python -c 'import importlib.util, sys; missing = [m for m in ("vertebra._vertebra", "vertebra._bundler", "vertebra._storage", "vertebra._mysql", "vertebra._bigquery", "vertebra._mssql", "vertebra._spanner", "vertebra._duckdb", "vertebra._oracle", "vertebra._postgres", "vertebra._sqlite", "vertebra._redis", "vertebra._valkey") if importlib.util.find_spec(m) is None]; sys.exit(f"Missing native extensions: {missing}") if missing else None'
	@uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._mysql vertebra._bigquery vertebra._mssql vertebra._spanner vertebra._duckdb vertebra._oracle vertebra._postgres vertebra._sqlite vertebra._redis vertebra._valkey

.PHONY: dev-ext
dev-ext:                                            ## Build one native extension in debug mode (EXT=<bundler|storage|mysql|...>)
ifeq ($(strip $(EXT)),)
	@echo "EXT is required: core, bundler, storage, mysql, bigquery, mssql, spanner, duckdb, oracle, postgres, sqlite, redis, valkey" >&2; exit 2
else ifeq ($(strip $(EXT)),core)
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@uv run --no-sync maturin develop --uv
else
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@uv run --no-sync maturin develop --uv -m src/rs/vertebra-$(strip $(EXT))/Cargo.toml
endif

.PHONY: db-conformance
db-conformance:                                     ## Run live zero-skip conformance lane (ENGINE=duckdb|mysql|mariadb|mssql|oracle|redis|sqlite|valkey [VERSION=<v>])
ifeq ($(strip $(ENGINE)),)
	@echo "ENGINE is required: duckdb, mysql, mariadb, mssql, oracle, redis, sqlite, valkey" >&2; exit 2
else
	@export VERTEBRA_CONFORMANCE_DOCKER=1 \
		VERTEBRA_CONFORMANCE_ENGINES=$(strip $(ENGINE))$(if $(strip $(VERSION)), \
		VERTEBRA_CONFORMANCE_$(shell printf '%s' '$(strip $(ENGINE))' | tr '[:lower:]' '[:upper:]')_VERSION=$(strip $(VERSION)))$(if $(filter duckdb,$(strip $(ENGINE))), \
		VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1); \
	uv run --no-sync pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs
endif

.PHONY: sqlite-test
sqlite-test:                                        ## Run vertebra-sqlite Rust + Python unit and conformance tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo test -p vertebra-sqlite --locked)
	@PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._sqlite
	@uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/integration/db/test_driver_conformance.py -k sqlite -v
	@echo "${OK} SQLite tests passed"

.PHONY: sqlite-wheel-check
sqlite-wheel-check:                                 ## Build vertebra-sqlite wheel and verify payload isolation
	@mkdir -p /tmp/vtb-wheels-sqlite && rm -f /tmp/vtb-wheels-sqlite/*.whl
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-sqlite/Cargo.toml --out /tmp/vtb-wheels-sqlite
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-sqlite --wheel-glob "vertebra_sqlite-*.whl" --extension-prefix "vertebra/_sqlite"
	@echo "${OK} vertebra-sqlite wheel isolation verified"

.PHONY: duckdb-test
duckdb-test:                                        ## Run vertebra-duckdb Rust + Python unit and conformance tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo test -p vertebra-duckdb --locked)
	@uv run python -m mypy.stubtest vertebra._duckdb
	@uv run pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/integration/db/test_duckdb_native.py src/py/tests/integration/db/test_driver_conformance.py -k duckdb -v
	@echo "${OK} DuckDB tests passed"

.PHONY: duckdb-wheel-check
duckdb-wheel-check:                                 ## Build vertebra-duckdb wheel and verify payload isolation
	@mkdir -p /tmp/vtb-wheels-duckdb && rm -f /tmp/vtb-wheels-duckdb/*.whl
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-duckdb/Cargo.toml --out /tmp/vtb-wheels-duckdb
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-duckdb --wheel-glob "vertebra_duckdb-*.whl" --extension-prefix "vertebra/_duckdb"
	@echo "${OK} vertebra-duckdb wheel isolation verified"

.PHONY: mysql-test
mysql-test:                                         ## Run MySQL Rust crate tests, stubtest, and Python MySQL test suites
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo test -p vertebra-mysql -p vertebra-db --features gcp)
	@uv run python -m mypy.stubtest vertebra._mysql
	@uv run pytest src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/integration/db/test_mysql_native.py src/py/tests/integration/db/test_driver_conformance.py -k "mysql" -v
	@echo "${OK} MySQL tests passed"

.PHONY: mysql-wheel-check
mysql-wheel-check:                                  ## Build vertebra-mysql wheel and verify PEP 420 namespace isolation
	@rm -rf /tmp/vtb-wheels-mysql && mkdir -p /tmp/vtb-wheels-mysql
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-mysql/Cargo.toml --out /tmp/vtb-wheels-mysql
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-mysql --wheel-glob "vertebra_mysql-*.whl" --extension-prefix "vertebra/_mysql" --forbid-python-files
	@echo "${OK} vertebra-mysql wheel isolation verified"

.PHONY: mssql-test
mssql-test:                                         ## Run Rust and Python SQL Server (vertebra-mssql) tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo test -p vertebra-mssql --locked)
	@uv run python -m mypy.stubtest vertebra._mssql
	@uv run pytest src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/integration/db/test_mssql_native.py src/py/tests/integration/db/test_driver_conformance.py -k "mssql" -v
	@echo "${OK} MSSQL tests passed"

.PHONY: mssql-wheel-check
mssql-wheel-check:                                  ## Build vertebra-mssql wheel and verify isolated archive layout
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@rm -rf /tmp/vtb-wheels-mssql && mkdir -p /tmp/vtb-wheels-mssql
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-mssql/Cargo.toml --out /tmp/vtb-wheels-mssql
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-mssql --wheel-glob "vertebra_mssql-*.whl" --extension-prefix "vertebra/_mssql" --forbid-python-files
	@echo "${OK} vertebra-mssql wheel layout verified"

.PHONY: oracle-test
oracle-test:                                        ## Run Oracle Rust unit tests, stubtest, and Python unit tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
	@PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._oracle
	@PYTHONPATH="src/py:." uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_oracle_context_notify_and_queues.py src/py/tests/integration/db/test_oracle_native.py -v
	@echo "${OK} Oracle tests passed"

.PHONY: oracle-wheel-check
oracle-wheel-check:                                 ## Build vertebra-core and vertebra-oracle wheels and verify isolated fail-loud contract
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_oracle_wheel.py
	@echo "${OK} Oracle wheel isolation verified"

.PHONY: postgres-test
postgres-test:                                      ## Run vertebra-postgres Rust tests and _postgres stubtest
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="$(CURDIR)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
	@uv run --no-sync python -m mypy.stubtest vertebra._postgres
	@echo "${OK} PostgreSQL extension tests passed"

.PHONY: postgres-wheel-check
postgres-wheel-check:                               ## Build and verify standalone vertebra-postgres wheel, sdist, and 14-class isolation
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_postgres_wheel.py
	@echo "${OK} PostgreSQL wheel and sdist verified"

.PHONY: redis-test
redis-test:                                         ## Run vertebra-redis Rust unit tests, stubtest, and Python tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis --locked
	@PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._redis
	@uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/framework/test_queue_backends_and_channels.py src/py/tests/integration/db/test_driver_conformance.py -k "redis and not docker" -v
	@echo "${OK} Redis tests passed"

.PHONY: redis-wheel-check
redis-wheel-check:                                  ## Build vertebra-redis wheel and verify isolated archive layout
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@rm -rf /tmp/vtb-wheels-redis && mkdir -p /tmp/vtb-wheels-redis
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-redis/Cargo.toml --out /tmp/vtb-wheels-redis
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-redis --wheel-glob "vertebra_redis-*.whl" --extension-prefix "vertebra/_redis" --forbid-python-files
	@echo "${OK} vertebra-redis wheel layout verified"

.PHONY: valkey-test
valkey-test:                                         ## Run vertebra-valkey Rust unit tests, stubtest, and Python tests
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey --locked
	@PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._valkey
	@uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/framework/test_queue_backends_and_channels.py src/py/tests/integration/db/test_driver_conformance.py -k "valkey and not docker" -v
	@echo "${OK} Valkey tests passed"

.PHONY: valkey-wheel-check
valkey-wheel-check:                                  ## Build vertebra-valkey wheel and verify isolated archive layout
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@rm -rf /tmp/vtb-wheels-valkey && mkdir -p /tmp/vtb-wheels-valkey
	@uv run --no-sync maturin build --manifest-path src/rs/vertebra-valkey/Cargo.toml --out /tmp/vtb-wheels-valkey
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/_wheel_verify.py --wheel-dir /tmp/vtb-wheels-valkey --wheel-glob "vertebra_valkey-*.whl" --extension-prefix "vertebra/_valkey" --forbid-python-files
	@echo "${OK} vertebra-valkey wheel layout verified"

.PHONY: core-wheel-check
core-wheel-check:                                   ## Build relocatable vertebra-server + vertebra-core wheel and smoke test vtb run
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@rm -rf /tmp/vtb-core-wheel /tmp/vtb-core-smoke-venv /tmp/vtb-core-smoke-app
	@uv run --no-sync python tools/scripts/build_server_binary.py build
	@uv build --wheel --out-dir /tmp/vtb-core-wheel
	@uv venv --python 3.12 /tmp/vtb-core-smoke-venv
	@uv pip install --python /tmp/vtb-core-smoke-venv/bin/python /tmp/vtb-core-wheel/vertebra_core-*.whl
	@uv run --no-sync python tools/scripts/smoke_vtb_run.py --vtb /tmp/vtb-core-smoke-venv/bin/vtb --wheel-dir /tmp/vtb-core-wheel --workdir /tmp/vtb-core-smoke-app
	@echo "${OK} Core wheel and vtb run smoke verified"

.PHONY: storage-wheel-check
storage-wheel-check:                                ## Build vertebra-core and vertebra-storage wheels and verify isolated fail-loud contract
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_storage_wheel.py --package storage
	@echo "${OK} Storage wheel isolation verified"

.PHONY: bigquery-wheel-check
bigquery-wheel-check:                               ## Build vertebra-core and vertebra-bigquery wheels and verify isolated fail-loud contract
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_storage_wheel.py --package bigquery
	@echo "${OK} BigQuery wheel isolation verified"

.PHONY: spanner-wheel-check
spanner-wheel-check:                                ## Build vertebra-core and vertebra-spanner wheels and verify isolated fail-loud contract
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_storage_wheel.py --package spanner
	@echo "${OK} Spanner wheel isolation verified"

.PHONY: bundler-wheel-check
bundler-wheel-check:                                ## Build vertebra-core and vertebra-bundler wheels and verify isolated fail-loud contract
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="src/py:." uv run --no-sync python tools/scripts/verify_storage_wheel.py --package bundler
	@echo "${OK} Bundler wheel isolation verified"

.PHONY: test
test: rs-test py-test ts-test                       ## Run all tests (Rust, Python, TypeScript)
	@echo "${OK} All tests passed"

.PHONY: coverage
coverage:                                           ## Run tests with coverage report
	@uv run --no-sync pytest $(PYTEST_ARGS) src/py/tests --cov --quiet
	@uv run --no-sync coverage xml -i >/dev/null 2>&1
	@uv run --no-sync coverage html -i >/dev/null 2>&1
	@echo "${OK} Coverage report: htmlcov/index.html"


# =============================================================================
# Lint & Format
# =============================================================================
.PHONY: lint
lint: ts-lint                                       ## Format and lint everything with auto-fixes (Python, Rust, TS)
	@echo "${INFO} Type-checking TS package (tsc)..."
	@(cd src/ts/vertebra-client && bunx tsc --noEmit)
	@echo "${INFO} Linting and fixing Python (Ruff)..."
	@uv run --no-sync ruff check --fix --unsafe-fixes src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync ruff format src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@echo "${INFO} Linting and fixing Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt --all)
	@(cd src/rs && cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings)
	@echo "${INFO} Running type checks (Mypy & Pyright)..."
	@uv run --no-sync mypy src/py/vertebra src/py/tests/unit src/py/tests/integration tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync pyright src/py/vertebra
	@echo "${INFO} Checking optional extras parity and package metadata..."
	@uv run --no-sync python tools/scripts/check_extras.py
	@uv run --no-sync python tools/scripts/check_package_metadata.py
	@echo "${INFO} Checking native extension stubs (stubtest)..."
	@uv run --no-sync python -c 'import importlib.util, sys; missing = [m for m in ("vertebra._vertebra", "vertebra._bundler", "vertebra._storage", "vertebra._mysql", "vertebra._bigquery", "vertebra._mssql", "vertebra._spanner", "vertebra._duckdb", "vertebra._oracle", "vertebra._postgres", "vertebra._sqlite", "vertebra._redis", "vertebra._valkey") if importlib.util.find_spec(m) is None]; sys.exit(f"Missing native extensions: {missing}") if missing else None'
	@uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._mysql vertebra._bigquery vertebra._mssql vertebra._spanner vertebra._duckdb vertebra._oracle vertebra._postgres vertebra._sqlite vertebra._redis vertebra._valkey
	@echo "${OK} Lint complete"

.PHONY: lint-ts
lint-ts:                                            ## Check TypeScript formatting, lint, and types (Biome & tsc)
	@echo "${INFO} Checking TS package (Biome & tsc)..."
	@(cd src/ts/vertebra-client && bunx biome check .)
	@(cd src/ts/vertebra-client && bunx tsc --noEmit)

.PHONY: lint-py
lint-py:                                            ## Check Python lint, format, types, extras, and package metadata without mutating files
	@echo "${INFO} Checking Python (Ruff, Mypy, Pyright, extras & metadata)..."
	@uv run --no-sync ruff check src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync ruff format --check src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync mypy src/py/vertebra src/py/tests/unit src/py/tests/integration tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync pyright src/py/vertebra
	@uv run --no-sync python tools/scripts/check_extras.py
	@uv run --no-sync python tools/scripts/check_package_metadata.py

.PHONY: lint-rs
lint-rs:                                            ## Check Rust formatting and Clippy warnings without mutating files
	@echo "${INFO} Checking Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt --all -- --check)
	@(cd src/rs && cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings)

.PHONY: lint-all
lint-all:                                           ## Check formatting, linting, and types without mutating files (CI)
	@echo "${INFO} Checking TS package (Biome & tsc)..."
	@(cd src/ts/vertebra-client && bunx biome check .)
	@(cd src/ts/vertebra-client && bunx tsc --noEmit)
	@echo "${INFO} Checking Python (Ruff, Mypy, Pyright, extras & metadata)..."
	@uv run --no-sync ruff check src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync ruff format --check src/py/vertebra src/py/tests tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync mypy src/py/vertebra src/py/tests/unit src/py/tests/integration tools/scripts tools/pgo tools/benchmarks $(wildcard tools/docs docs/examples)
	@uv run --no-sync pyright src/py/vertebra
	@uv run --no-sync python tools/scripts/check_extras.py
	@uv run --no-sync python tools/scripts/check_package_metadata.py
	@uv run --no-sync python -c 'import importlib.util, sys; missing = [m for m in ("vertebra._vertebra", "vertebra._bundler", "vertebra._storage", "vertebra._mysql", "vertebra._bigquery", "vertebra._mssql", "vertebra._spanner", "vertebra._duckdb", "vertebra._oracle", "vertebra._postgres", "vertebra._sqlite", "vertebra._redis", "vertebra._valkey") if importlib.util.find_spec(m) is None]; sys.exit(f"Missing native extensions: {missing}") if missing else None'
	@uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._mysql vertebra._bigquery vertebra._mssql vertebra._spanner vertebra._duckdb vertebra._oracle vertebra._postgres vertebra._sqlite vertebra._redis vertebra._valkey
	@echo "${INFO} Checking Rust (Fmt & Clippy)..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@(cd src/rs && cargo fmt --all -- --check)
	@(cd src/rs && cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings)
	@echo "${OK} All lint checks passed"

.PHONY: review-check
review-check:                                       ## Validate agentic review setup, skills, and manifests
	@uv run --no-sync python -m tools.scripts.review_setup validate

.PHONY: review-route
review-route:                                       ## Route changed files to owning review areas and shards
	@BASE=$${BASE:-origin/main}; uv run --no-sync python -m tools.scripts.review_setup route --base "$$BASE"

.PHONY: check
check: lint test coverage                           ## Everything: lint, type check, all tests, coverage


# =============================================================================
# Documentation
# =============================================================================
.PHONY: docs-clean
docs-clean:                                         ## Remove built documentation and Zensical cache
	@rm -rf site .cache
	@echo "${OK} Docs clean"

.PHONY: docs
docs: docs-clean                                    ## Build and verify Zensical documentation
	@echo "${INFO} Building documentation (Zensical)..."
	@PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts --stage-inventory-cache /tmp/vertebra-mkdocs-cache src/py/vertebra
	@PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.generate_cli_docs --check
	@XDG_CACHE_HOME="/tmp/vertebra-mkdocs-cache" PYTHONPATH="src/py:." uv run --no-sync zensical build --clean
	@PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.postbuild --site-dir site --docs-dir docs --zensical-config zensical.toml --rust-doc-dir "$(CARGO_TARGET_DIR)/doc"
	@PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.verify_docs_artifacts --site-dir site
	@echo "${OK} Documentation built and verified in site/"

.PHONY: docs-serve
docs-serve: docs                                    ## Build and serve documentation locally
	@XDG_CACHE_HOME="/tmp/vertebra-mkdocs-cache" PYTHONPATH="src/py:." uv run --no-sync zensical serve --dev-addr 127.0.0.1:8000

.PHONY: docs-check
docs-check: docs                                    ## Build docs and verify live zensical serve HTTP 200
	@echo "${INFO} Verifying zensical serve..."
	@set -euo pipefail; \
	server_log="$$(mktemp)"; \
	server_pid=""; \
	cleanup() { \
		if [ -n "$$server_pid" ] && kill -0 "$$server_pid" 2>/dev/null; then \
			kill "$$server_pid" 2>/dev/null || true; \
			wait "$$server_pid" 2>/dev/null || true; \
		fi; \
		rm -f "$$server_log"; \
	}; \
	trap cleanup EXIT INT TERM; \
	PYTHONPATH="src/py:." uv run --no-sync zensical serve --dev-addr 127.0.0.1:8000 >"$$server_log" 2>&1 & \
	server_pid="$$!"; \
	for _ in $$(seq 1 50); do \
		if curl --silent --show-error --fail http://127.0.0.1:8000/ >/dev/null 2>&1 || curl --silent --show-error --fail http://127.0.0.1:8000/vertebra/ >/dev/null 2>&1; then \
			echo "${OK} Docs server responded with HTTP 200"; \
			exit 0; \
		fi; \
		if ! kill -0 "$$server_pid" 2>/dev/null; then \
			cat "$$server_log" >&2; \
			exit 1; \
		fi; \
		sleep 0.2; \
	done; \
	cat "$$server_log" >&2; \
	exit 1

.PHONY: docs-rust
docs-rust:                                          ## Build Rust workspace rustdoc and copy into site/rustdoc/ if site/ exists
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cargo doc --manifest-path src/rs/Cargo.toml --workspace --no-deps
	@if [ -d "site" ] && [ -d "$(CARGO_TARGET_DIR)/doc" ]; then \
		rm -rf site/rustdoc && cp -R "$(CARGO_TARGET_DIR)/doc" site/rustdoc; \
	elif [ -d "site" ] && [ -d "src/rs/target/doc" ]; then \
		rm -rf site/rustdoc && cp -R src/rs/target/doc site/rustdoc; \
	fi
	@echo "${OK} Rust documentation built"


# =============================================================================
# Benchmarks
# =============================================================================
.PHONY: bench-tools
bench-tools:                                        ## Check and install missing benchmark load drivers (oha, h2load, ghz, k6) on demand
	@echo "${INFO} Checking benchmark load-driver binaries..."
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi; \
	missing=0; \
	if command -v oha >/dev/null 2>&1; then \
		echo "${OK} oha already installed ($$(command -v oha))"; \
	elif command -v cargo >/dev/null 2>&1; then \
		echo "${INFO} Installing oha via cargo..."; \
		cargo install --locked oha; \
	else \
		echo "${WARN} oha missing: install Rust/cargo and run 'cargo install --locked oha' or download from https://github.com/hatoo/oha/releases"; \
		missing=1; \
	fi; \
	if command -v h2load >/dev/null 2>&1; then \
		echo "${OK} h2load already installed ($$(command -v h2load))"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "${INFO} Installing nghttp2 (h2load) via Homebrew..."; \
		brew install nghttp2; \
	else \
		echo "${WARN} h2load missing: install with 'sudo apt-get install -y nghttp2-client' (Debian/Ubuntu) or 'brew install nghttp2' (macOS)"; \
		missing=1; \
	fi; \
	if command -v ghz >/dev/null 2>&1; then \
		echo "${OK} ghz already installed ($$(command -v ghz))"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "${INFO} Installing ghz via Homebrew..."; \
		brew install ghz; \
	elif command -v go >/dev/null 2>&1; then \
		echo "${INFO} Installing ghz via go install..."; \
		go install github.com/bojand/ghz/cmd/ghz@latest; \
	else \
		echo "${WARN} ghz missing: download a release binary from https://github.com/bojand/ghz/releases or run 'go install github.com/bojand/ghz/cmd/ghz@latest'"; \
		missing=1; \
	fi; \
	if command -v k6 >/dev/null 2>&1; then \
		echo "${OK} k6 already installed ($$(command -v k6))"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "${INFO} Installing k6 via Homebrew..."; \
		brew install k6; \
	else \
		echo "${WARN} k6 missing: install from https://grafana.com/docs/k6/latest/set-up/install-k6/ or run via Docker (grafana/k6:latest)"; \
		missing=1; \
	fi; \
	if [ "$$missing" -eq 0 ]; then \
		echo "${OK} All benchmark load drivers are available"; \
	else \
		echo "${WARN} Some benchmark load drivers require manual package installation or Docker fallback"; \
	fi

.PHONY: bench-run
bench-run:                                          ## Run benchmarks without rebuilding release artifacts
	@echo "${INFO} Running benchmarks..."
	@uv run --no-sync --group bench python -m tools.benchmarks run \
		-t $${TARGETS:-vtb} \
		-s $${SCENARIOS:-default} \
		$${BENCH_ARGS:-}
	@echo "${OK} Benchmarks complete"

.PHONY: bench-all-run
bench-all-run:                                      ## Run ALL benchmarks without rebuilding release artifacts
	@echo "${INFO} Running full benchmark suite..."
	@uv run --no-sync --group bench python -m tools.benchmarks run \
		-t all \
		-s full \
		$${BENCH_ARGS:-}
	@echo "${OK} Full benchmarks complete"

.PHONY: bench
bench: build bench-run                              ## Run benchmarks (use TARGETS= and SCENARIOS= to customize)

.PHONY: bench-all
bench-all: build bench-all-run                      ## Run ALL benchmarks (all targets, all scenarios)

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
	UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python "$$FT_PY" --no-dev 2>&1 | tail -3; \
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

.PHONY: postgres-bench
postgres-bench:                                     ## Run comparative PostgreSQL benchmarks and Criterion benches
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@cd src/rs && cargo bench -p vertebra-postgres --bench postgres_native --locked
	@uv run --no-sync --group postgres-bench pytest -m postgres_benchmark src/py/tests/integration/db/test_postgres_benchmarks.py -v

.PHONY: postgres-bench-check
postgres-bench-check:                               ## Verify Criterion postgres_native benches execute in test mode and comparator suite passes offline
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@PYTHONPATH="$(CURDIR)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --locked
	@uv run --no-sync ruff check tools/benchmarks/postgres.py tools/scripts/_wheel_verify.py tools/scripts/verify_postgres_wheel.py src/py/tests/unit/tools/test_postgres_benchmarks.py src/py/tests/integration/db/test_postgres_benchmarks.py
	@uv run --no-sync ruff format --check tools/benchmarks/postgres.py tools/scripts/_wheel_verify.py tools/scripts/verify_postgres_wheel.py src/py/tests/unit/tools/test_postgres_benchmarks.py src/py/tests/integration/db/test_postgres_benchmarks.py
	@uv run --no-sync mypy tools/benchmarks/postgres.py tools/scripts/_wheel_verify.py tools/scripts/verify_postgres_wheel.py
	@uv run --no-sync pytest src/py/tests/unit/tools/test_postgres_benchmarks.py -q
	@echo "${OK} PostgreSQL benchmark checks passed"

# =============================================================================
# Release
# =============================================================================
.PHONY: release
release:                                             ## Bump a stable release (bump=patch|minor|major) or finalize the current pre-release (no bump)
	@$(MAKE) --no-print-directory _release-preflight
	@current="$$(uv run --no-sync bump-my-version show current_version)"
	@case "$(bump)" in
		patch|minor|major)
			echo "${INFO} Bumping $$current ($(bump))..."
			uv run --no-sync bump-my-version bump "$(bump)" ;;
		"")
			case "$$current" in
				*-*)
					echo "${INFO} Finalizing pre-release $$current as $${current%%-*}..."
					uv run --no-sync bump-my-version bump --new-version "$${current%%-*}" pre ;;
				*)
					echo "${ERROR} $$current is already stable. Usage: make release bump=patch|minor|major"
					exit 1 ;;
			esac ;;
		*)
			echo "${ERROR} Unknown bump '$(bump)'. Usage: make release bump=patch|minor|major"
			exit 1 ;;
	esac
	@$(MAKE) --no-print-directory _release-finish

.PHONY: pre-release
pre-release:                                         ## Start or advance a pre-release: make pre-release version=X.Y.Z-alpha.N
	@if ! [[ "$(version)" =~ ^[0-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$$ ]]; then
		echo "${ERROR} Usage: make pre-release version=X.Y.Z-(alpha|beta|rc).N"
		echo ""
		echo "Pre-release workflow:"
		echo "  1. Start alpha:     make pre-release version=0.4.0-alpha.1"
		echo "  2. Next alpha:      make pre-release version=0.4.0-alpha.2"
		echo "  3. Move to beta:    make pre-release version=0.4.0-beta.1"
		echo "  4. Move to rc:      make pre-release version=0.4.0-rc.1"
		echo "  5. Final release:   make release"
		exit 1
	fi
	@$(MAKE) --no-print-directory _release-preflight
	@echo "${INFO} Preparing pre-release $(version)..."
	@uv run --no-sync bump-my-version bump --new-version "$(version)" pre
	@$(MAKE) --no-print-directory _release-finish

.PHONY: _release-preflight
_release-preflight:
	@if [ "$$(git rev-parse --abbrev-ref HEAD)" = "main" ]; then
		echo "${ERROR} Releases are prepared on a branch and merged by pull request. Run: git switch -c release/next"
		exit 1
	fi
	@uv run --no-sync python tools/scripts/check_package_metadata.py

.PHONY: _release-finish
_release-finish:
	@if [ -f "$(RUST_ENV)" ]; then . "$(RUST_ENV)"; fi
	@uv lock --offline
	@cargo update -w --manifest-path src/rs/Cargo.toml --offline
	@$(MAKE) --no-print-directory docs
	@version="$$(uv run --no-sync bump-my-version show current_version)"
	@prerelease=""
	@case "$$version" in *-*) prerelease=" --prerelease" ;; esac
	@echo "${OK} Version bumped to $$version"
	@echo ""
	@echo "${INFO} Next steps:"
	@echo "  1. Add a '## $$version' section to CHANGELOG.md"
	@echo "  2. Commit: git commit -S -am 'chore(release): bump to v$$version'"
	@echo "  3. Push the branch and open a pull request"
	@echo "  4. After it merges, publish: gh release create v$$version --target main --generate-notes$$prerelease --title v$$version"
	@echo "     The Release workflow builds every wheel and publishes all 13 packages to PyPI"
