---
type: Guide
title: "Benchmark Suite & Free-Threaded Python 3.14t/3.15t"
description: "Criterion microbenchmarks, the integrated tools.benchmarks matrix harness (HTTP/1.1, HTTP/2, HTTP/3, gRPC, standalone adapters, RustFS/Floci/Azurite fixtures), PGO tooling, dependency-group isolation, and the free-threaded Python 3.14t/3.15t gate."
status: stable
tags:
  - testing
  - benchmarks
  - free-threaded
  - python
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:bb-executor
    at: "2026-11-19T23:05:00Z"
    commit: a2da3e9f
  - by: agent:flow-executor
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-executor
    at: "2026-11-14T18:05:00Z"
    commit: 1b191388
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 1b191388
  - by: agent:flow-setup
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
sources:
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-tools-benchmarks-cli-py
    resource: tools/benchmarks/cli.py
    title: "Repository tooling module (tools/benchmarks/cli.py)"
  - id: src-tools-benchmarks-drivers-py
    resource: tools/benchmarks/drivers.py
    title: "Unified closed-loop and open-loop load-generator drivers (oha, h2load, ghz, k6)"
  - id: src-tools-benchmarks-fixtures-py
    resource: tools/benchmarks/fixtures.py
    title: "Container fixture specs (RustFS, Floci, Azurite, SQL, cache) and storage config builders"
  - id: src-tools-benchmarks-runner-py
    resource: tools/benchmarks/runner.py
    title: "Repository tooling module (tools/benchmarks/runner.py)"
  - id: src-tools-benchmarks-scenarios-py
    resource: tools/benchmarks/scenarios.py
    title: "Repository tooling module (tools/benchmarks/scenarios.py)"
  - id: src-tools-benchmarks-targets-py
    resource: tools/benchmarks/targets.py
    title: "Repository tooling module (tools/benchmarks/targets.py)"
  - id: src-tools-benchmarks-store-py
    resource: tools/benchmarks/store.py
    title: "Repository tooling module (tools/benchmarks/store.py)"
  - id: src-tools-benchmarks-postgres-py
    resource: tools/benchmarks/postgres.py
    title: "PostgreSQL comparative benchmark capture and comparator CLI"
  - id: src-tools-benchmarks-adapters-base-py
    resource: tools/benchmarks/adapters/base.py
    title: "Shared standalone adapter comparator contract, schema_version=1, and driver_mode stratification"
  - id: src-tools-benchmarks-adapters-sql-py
    resource: tools/benchmarks/adapters/sql.py
    title: "Standalone SQL and ORM adapter comparators (PostgreSQL, SQLite, DuckDB, MySQL, Oracle, MSSQL, SQLAlchemy, SQLSpec)"
  - id: src-tools-benchmarks-adapters-cache-py
    resource: tools/benchmarks/adapters/cache.py
    title: "Standalone Redis and Valkey cache comparators"
  - id: src-tools-benchmarks-adapters-storage-py
    resource: tools/benchmarks/adapters/storage.py
    title: "Standalone ObjectStore storage comparators across Memory, Local, RustFS, Floci, and Azurite"
  - id: src-tools-benchmarks-data-backend-py
    resource: tools/benchmarks/apps/_data_backend.py
    title: "Shared full-stack benchmark data backend, pool budget, and MinIO ban enforcement"
  - id: src-tools-pgo-init-py
    resource: tools/pgo/__init__.py
    title: "Repository tooling module (tools/pgo/__init__.py)"
  - id: src-tools-pgo-rust-pgo-py
    resource: tools/pgo/rust_pgo.py
    title: "Repository tooling module (tools/pgo/rust_pgo.py)"
  - id: src-tools-pgo-python-pgo-py
    resource: tools/pgo/python_pgo.py
    title: "Repository tooling module (tools/pgo/python_pgo.py)"
  - id: src-benchmark-yml
    resource: .github/workflows/benchmark.yml
    title: "GitHub Actions workflow (benchmark.yml)"
  - id: src-tests-unit-test-freethreaded-gil-py
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Python implementation module (src/py/tests/unit/test_freethreaded_gil.py)"
  - id: src-tests-unit-tools-test-e2e-benchmarks-py
    resource: src/py/tests/unit/tools/test_e2e_benchmarks.py
    title: "Consolidated offline end-to-end benchmark test suite"
  - id: src-vertebra-core-cargo-toml
    resource: src/rs/vertebra-core/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-core/Cargo.toml)"
  - id: src-vertebra-http-cargo-toml
    resource: src/rs/vertebra-http/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-http/Cargo.toml)"
  - id: src-vertebra-postgres-cargo-toml
    resource: src/rs/vertebra-postgres/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-postgres/Cargo.toml)"
  - id: src-postgres-native-bench-rs
    resource: src/rs/vertebra-postgres/benches/postgres_native.rs
    title: "PostgreSQL native codec, wire bind, PGCOPY, and pgoutput Criterion benches"
---

# Benchmark Suite & Free-Threaded Python 3.14t/3.15t

<!-- truth: start -->
- Third-party `duckdb>=1.0; python_version < '3.14'` lives in `[dependency-groups].duckdb` and `bench` solely for `tools.benchmarks.store`, third-party comparative SQL/cache/storage/framework drivers live in `[dependency-groups].bench-comparators` (and `psycopg[binary]`/`asyncpg` in `postgres-bench`), and `uvicorn`/`granian`/`spikard` live in `bench`; `make install`, `make upgrade`, `make py-test`, and `make coverage` never sync `--group duckdb` or `bench-comparators`.
- `tools.benchmarks` stores 8 matrix columns (`adapter`, `protocol`, `transport`, `broker_mode`, `load_mode`, `target_rps`, `pool_size`, `gil_enabled`) in DuckDB with idempotent legacy migration, dispatches closed-loop and open-loop load via `oha`, `h2load`, `ghz`, and `k6` (failing loud with install hints when absent), supervises SHM `BrokerHostHook` lifecycles, and forbids MinIO in favor of `rustfs`, `floci_*`, `azurite`, `local`, and `memory`.
- Run `make bench`, `make bench-ft`, `make rs-bench`, `make bench-micro`, `make bench-check`, and `make postgres-bench` against release/PGO builds; run `make postgres-bench-check` and `src/py/tests/unit/tools/test_e2e_benchmarks.py` offline for Criterion test execution, lint/type validation, standalone SQLite/Memory comparators, and full matrix contract checks.
<!-- truth: end -->

Vertebra has Rust Criterion microbenchmarks (`src/rs/vertebra-core/benches/`, `src/rs/vertebra-http/benches/`, `src/rs/vertebra-postgres/benches/`), standalone adapter comparators in [`tools/benchmarks/adapters/`](../../../../tools/benchmarks/adapters/base.py)[^src-tools-benchmarks-adapters-base-py] and [`tools/benchmarks/postgres.py`](../../../../tools/benchmarks/postgres.py)[^src-tools-benchmarks-postgres-py], and a full-stack HTTP/1.1, HTTP/2, HTTP/3, and gRPC harness in [`tools/benchmarks/`](../../../../tools/benchmarks/cli.py)[^src-tools-benchmarks-cli-py]. No throughput figures are recorded in the repository outside `tools/benchmarks/results/`.

## 1. Benchmark Targets (`Makefile` & `tools.benchmarks`)

| Target | Command | Scope |
|---|---|---|
| `make bench-tools` | Checks `oha`, `h2load`, `ghz`, `k6` on `PATH` and installs missing binaries on demand | Prepares local load-generator binaries for closed-loop and open-loop runs. |
| `make bench` | `make build`, then `uv run --group bench python -m tools.benchmarks run -t ${TARGETS:-vtb} -s ${SCENARIOS:-default} ${BENCH_ARGS}` | `default` scenarios (`hello`, `plaintext`, `body_1k`, `body_10k`, `json`, `echo`, `io_10ms`) against Vertebra targets (embedded and SHM topologies across native, RSGI, ASGI, and WSGI). |
| `make bench-all` | `make build`, then `... run -t all -s full ${BENCH_ARGS}` | Every scenario (`full` adds `body_100k`, data, storage, and gRPC scenarios) against all targets: `vtb-embedded-*`, `vtb-shm-*`, `vtb-ft-*`, data/gRPC targets, and baseline servers/frameworks. |
| `make bench-ft` | Resolves `FT_PYTHON` (`3.15t` if installed, else `3.14t`), syncs `.venv-ft`, builds `src/rs/target-ft`, then `... run -t freethreaded -s ${SCENARIOS:-multiworker}` | Free-threaded Python `3.15t`/`3.14t` (`Py_GIL_DISABLED`). `multiworker` = `hello`, `json`, `echo`, `io_10ms`. |
| `make postgres-bench` | `cd src/rs && cargo bench -p vertebra-postgres --bench postgres_native --locked`, then `uv run --no-sync --group postgres-bench pytest -m postgres_benchmark` | Native codecs/wire/`PGCOPY`/`pgoutput` Criterion benches plus live `vertebra` vs `psycopg`/`asyncpg` throughput and latency (`schema_version=1`). |
| `make postgres-bench-check` | `PYTHONPATH="$(CURDIR)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --bench postgres_native --locked` + Ruff/Mypy + `pytest src/py/tests/unit/tools/test_postgres_benchmarks.py` | Offline Criterion test execution, script/benchmark lint and type check, and comparator threshold unit gate. |
| `make bench-report` | `uv run --group bench python -m tools.benchmarks report` | Latest run summary (`make bench-diff` compares against previous). |
| `cargo bench` | `cd src/rs && cargo bench` | Criterion: `spsc`, `guards`, `ring_throughput`, `codec`, `shm_transport` (`vertebra-core`), `put_checksum` (`vertebra-storage`), `ssr_render` (`vertebra-bundler`), `server_throughput` (`vertebra-http`), and `postgres_native` (`vertebra-postgres`)[^src-vertebra-postgres-cargo-toml] [^src-postgres-native-bench-rs]. |

`FREETHREADED_TARGETS` and `DATA_FREETHREADED_TARGETS` require `src/rs/target-ft/release/vertebra-server` and raise `FileNotFoundError` immediately if `make bench-ft` has not built the free-threaded binary.

## 2. Harness Layout, Load Drivers & Matrix CLI (`tools/benchmarks/`)

- [`cli.py`](../../../../tools/benchmarks/cli.py)[^src-tools-benchmarks-cli-py]: `run` supports target presets (`all`, `vtb`, `baselines`, `freethreaded`, `data`, `data-baselines`, `data-freethreaded`, `frameworks`, `grpc`, or comma-separated names), scenario presets (`quick`, `default`, `full`, `multiworker`, `data`, `storage`, `grpc`, `grpc-native`), and matrix filters `--protocol`, `--topology`, `--adapter`, `--broker-mode`, `--load-mode` (`closed_loop` | `open_loop`), and `--rate` (required when `--load-mode open_loop`). `list-targets` accepts the same `--protocol`, `--topology`, `--adapter`, and `--broker-mode` filters alongside `list-scenarios`, `ingest`, `compare`, `report`, and `runs`.
- [`drivers.py`](../../../../tools/benchmarks/drivers.py)[^src-tools-benchmarks-drivers-py]: Unified `LoadDriver` protocol and `LoadConfig` implemented by `OhaDriver` (`oha -q` for open-loop HTTP/1.1), `H2LoadDriver` (`h2load` for HTTP/2 and HTTP/3), `GhzDriver` (`ghz` for gRPC), and `K6Driver` (`k6` scripted workloads), plus `build_docker_driver_command` fallback helpers. Missing binaries raise `DriverBinaryNotFoundError` (`FileNotFoundError`) with actionable install hints.
- [`runner.py`](../../../../tools/benchmarks/runner.py)[^src-tools-benchmarks-runner-py]: `ServerRunner` executes `ServerLifecycleHook` implementations (`BrokerHostHook` with `BrokerHookConfig`) in order on `pre_start()` before spawning the server and tears them down in reverse order on `post_stop()`, including on startup failure.
- [`scenarios.py`](../../../../tools/benchmarks/scenarios.py)[^src-tools-benchmarks-scenarios-py]: `SCENARIOS` covers static/echo/I/O, TechEmpower-style database/cache workloads (`db`, `queries_1..20`, `updates_5..20`, `cached_queries_10..100`), object storage (`object_put_4k`, `object_get_4k`, `object_get_64k`), and gRPC (`grpc_db`, `grpc_queries_10`, `grpc_native_db`, `grpc_native_queries_10`).
- [`targets.py`](../../../../tools/benchmarks/targets.py)[^src-tools-benchmarks-targets-py]: `Target` carries `protocol`, `transport`, `adapter`, `broker_mode`, `pool_size`, and `workers` across `VERTEBRA_TARGETS`, `BASELINE_TARGETS`, `FREETHREADED_TARGETS`, `DATA_TARGETS`, `DATA_BASELINE_TARGETS`, `DATA_FREETHREADED_TARGETS`, `FRAMEWORK_TARGETS`, and `GRPC_TARGETS`. `tools/benchmarks/apps/` hosts the native, ASGI, RSGI, WSGI, gRPC (`data_grpc.py`), and framework (`data_frameworks.py`) apps backed by [`_data_backend.py`](../../../../tools/benchmarks/apps/_data_backend.py)[^src-tools-benchmarks-data-backend-py].
- [`store.py`](../../../../tools/benchmarks/store.py)[^src-tools-benchmarks-store-py] and `sql/`: JSONL results (`tools/benchmarks/results/benchmarks.jsonl`) ingest into DuckDB (`tools/benchmarks/results/benchmarks.duckdb`). `_migrate_result_columns()` idempotently upgrades legacy 2-table schemas with `topology` and the 8 matrix columns (`adapter`, `protocol`, `transport`, `broker_mode`, `load_mode`, `target_rps`, `pool_size`, `gil_enabled`); `get_run_results()` filters by matrix dimensions and `reports.py` renders matrix summaries.
- [`src/py/tests/unit/test_bench_infrastructure.py`](../../../../src/py/tests/unit/test_bench_infrastructure.py) and [`src/py/tests/unit/tools/test_e2e_benchmarks.py`](../../../../src/py/tests/unit/tools/test_e2e_benchmarks.py)[^src-tests-unit-tools-test-e2e-benchmarks-py] verify the full suite offline without Docker or external load-driver binaries.

## 3. Standalone Adapter Comparators & Container Fixtures

- [`adapters/base.py`](../../../../tools/benchmarks/adapters/base.py)[^src-tools-benchmarks-adapters-base-py]: Defines `AdapterBenchmarkCase`, `AdapterBenchmarkReport` (`ADAPTER_SCHEMA_VERSION = 1`), `from_postgres_report()` for backward-compatible `postgres.py` conversion, `detect_gil_enabled()`, and `compare_adapter_reports()` with `driver_mode` (`async_native` vs `sync_blocking`) stratification so sync and async drivers are never compared across modes by accident.
- [`adapters/sql.py`](../../../../tools/benchmarks/adapters/sql.py)[^src-tools-benchmarks-adapters-sql-py]: Measures Vertebra SQL drivers against external comparators (`psycopg`, `asyncpg`, `sqlite3`/`aiosqlite`, `duckdb`, `pymysql`/`aiomysql`, `oracledb`, `pymssql`, SQLAlchemy, and upstream SQLSpec); SQLite (`:memory:`) runs completely offline.
- [`adapters/cache.py`](../../../../tools/benchmarks/adapters/cache.py)[^src-tools-benchmarks-adapters-cache-py]: Measures Vertebra Redis/Valkey sync and async drivers (point ops, pipelines, PubSub) against `redis-py` / `hiredis`.
- [`adapters/storage.py`](../../../../tools/benchmarks/adapters/storage.py)[^src-tools-benchmarks-adapters-storage-py]: Measures `SyncObjectStore` and `AsyncObjectStore` PUT/GET/range reads against `obstore`, `boto3`, `aiobotocore`, and `azure-storage-blob`; `memory` and `local` backends run completely offline.
- [`fixtures.py`](../../../../tools/benchmarks/fixtures.py)[^src-tools-benchmarks-fixtures-py]: Defines `tmpfs`-backed `ContainerFixtureSpec` entries (`postgres`, `mysql`, `mariadb`, `oracle`, `mssql`, `redis`, `valkey`, `rustfs`, `floci_aws`, `floci_gcp`, `floci_azure`, `floci_oci`, `azurite`), `STORAGE_LOCAL_BACKENDS = ("local", "memory")`, `build_docker_run_command()`, `build_storage_config()`, and `FixtureLauncher`. MinIO is explicitly forbidden across `FIXTURE_SPECS`, `build_storage_config()`, and `load_backend_config()`.

## 4. Scheduled Benchmarks (`.github/workflows/benchmark.yml`)

The workflow runs on `workflow_dispatch` (input `full_benchmark`) and published `release` events. It installs `oha` with `taiki-e/install-action@v2`, runs `uv sync --all-extras --dev`, `make ts-install`, and `make build`, then executes `make bench` (or `make bench-all` when `full_benchmark` is `"true"`) and uploads `tools/benchmarks/results/` via `actions/upload-artifact@v7` with 90-day retention.

## 5. Profile-Guided Optimization (`tools/pgo/`)

- [`rust_pgo.py`](../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py]: `RustPGOConfig` (`PGOMode` `generate | use | baseline`), `EXTENSION_MANIFESTS` (`discover_extension_manifests`), `extension_module_name`, `PGOPipelineResult`, `resolve_cargo_release_dir` (`CARGO_TARGET_DIR`), `build_instrumented`, `build_instrumented_extension`, `build_instrumented_extensions`, `run_default_pgo_workload`, `merge_profiles`, `weighted_merge_profiles`, `find_llvm_profdata`, `validate_profdata`, `check_profdata_size`, `build_optimized`, `build_optimized_extension`, `build_optimized_extensions`, and `run_pgo_pipeline` (`python -m tools.pgo.rust_pgo`).
- [`python_pgo.py`](../../../../tools/pgo/python_pgo.py)[^src-tools-pgo-python-pgo-py]: `PythonBuildInfo`, `PlatformSupport`, `PLATFORM_MATRIX`, `detect_python_build`, `check_pgo_availability`, and `get_platform_support`.

`make build-pgo` invokes `uv run python -m tools.pgo.rust_pgo` for two-phase instrumented and profile-use release builds across `vertebra-http` and every discovered PyO3 extension (root-configured `_vertebra` plus each `src/rs/vertebra-*` crate with its own `pyproject.toml`), or `PROFDATA=<path> make build-pgo` to reuse an existing profile. The PGO tooling ships only in the repository; `vertebra.build.packaging` exposes no PGO API. `src/py/tests/unit/build/test_packaging_and_pgo.py` exercises both modules, and `vtb build [--server-binary <path>]` bundles a pre-built `bin/vertebra-server` into standalone and onefile artifacts.

## 6. Isolating `--group bench`, `--group bench-comparators`, `--group duckdb`, and `--group postgres-bench` (`pyproject.toml`)

Keep benchmark-only dependencies strictly in `[dependency-groups]` in [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml], never in `[project.dependencies]`, `[project.optional-dependencies]`, or `dev`:

- `duckdb` and `bench`: `duckdb>=1.0; python_version < '3.14'` solely for `tools.benchmarks.store` result persistence, plus `uvicorn`/`httptools`/`uvloop`/`granian`/`spikard` in `bench`.
- `postgres-bench`: `asyncpg>=0.30` and `psycopg[binary]>=3.2,<3.4` (`python_version < '3.14'`).
- `bench-comparators`: standalone third-party SQL, cache, storage, and framework comparators (`asyncpg`, `psycopg[binary]`, `pymysql`, `aiomysql`, `aiosqlite`, `oracledb`, `pymssql`, `redis[hiredis]`, `boto3`, `aiobotocore`, `obstore`, `azure-storage-blob`, `duckdb`, `django-bolt`, `litestar`, `fastapi`, `django`, `flask`).
- `pyproject.toml` sets no `[tool.uv] default-groups`; `make install`, `make upgrade`, `make py-test`, and `make coverage` build and exercise all 13 native PyO3 extensions (`vertebra._duckdb`, `_sqlite`, `_redis`, and `_valkey` included) without syncing `--group duckdb` or `--group bench-comparators`.
- `make bench*` passes `--group bench`, `make postgres-bench` passes `--group postgres-bench`, and `test_bench_infrastructure.py` plus `test_optional_driver_wheels.py` guard group isolation offline.

## 7. Free-Threaded Build (`make bench-ft`)

PyO3 links different CPython ABI symbols when `Py_GIL_DISABLED` is set, so free-threaded artifacts use their own environment and target directory:

1. `UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python "$FT_PYTHON" --no-dev` builds `.venv-ft` (selecting `3.15t` when installed, else `3.14t`).
2. `PYO3_PYTHON=$(uv python find "$FT_PYTHON") cargo build --manifest-path src/rs/Cargo.toml --release --target-dir src/rs/target-ft` builds the workspace against `libpython3.15t` / `libpython3.14t` without touching `src/rs/target`.
3. The harness runs from the default `.venv` (`--group bench`) and launches `src/rs/target-ft/release/vertebra-server` with `.venv-ft` site-packages injected ahead of the repo root through `PYTHONPATH` and `LD_LIBRARY_PATH` (`_ft_env` in `targets.py`).

`make clean` removes both `src/rs/target` and `src/rs/target-ft`.

## 8. Free-Threaded & Static Type Test Gate

- The CI `test-python` matrix includes `3.14t` and `3.15t` and runs `make py-test` on both; `test-db-conformance` also exercises `duckdb`, `sqlite`, `redis`, `valkey`, and `mysql` (`8.4`) on `3.14t`.
- All 13 PyO3 extension modules (`_vertebra`, `_bundler`, `_storage`, `_mysql`, `_bigquery`, `_mssql`, `_spanner`, `_duckdb`, `_oracle`, `_postgres`, `_sqlite`, `_redis`, `_valkey`) declare `#[pymodule(gil_used = false)]`.
- [`test_freethreaded_gil.py`](../../../../src/py/tests/unit/test_freethreaded_gil.py)[^src-tests-unit-test-freethreaded-gil-py] asserts `GIL_USED` is `False` across all 13 modules, enforces `Py_TPFLAGS_IMMUTABLETYPE` across native classes (including 15 `_postgres` classes), runs 8-thread concurrent construction proofs for Oracle and PostgreSQL, and verifies isolated child `sys._is_gil_enabled() is False` after `import vertebra._postgres`.
- `unit/server/test_hotpath_dispatch_and_freethreaded.py` and `unit/server/test_runtime_tuning.py` cover the free-threaded dispatch, loop backend selection, and `EventLoopPool` paths; `GilAuditReport` (`server/_gil_audit.py`) reports native extensions that re-enable the GIL.
- `make lint` and `make lint-all` also enforce `bunx tsc --noEmit`, `pyright src/py/vertebra`, `tools/scripts/check_extras.py`, and fail-loud 13-extension `mypy.stubtest` verification ahead of the test matrices.

Provenance sources: [^src-makefile], [^src-pyproject-toml], [^src-tools-benchmarks-cli-py], [^src-tools-benchmarks-drivers-py], [^src-tools-benchmarks-fixtures-py], [^src-tools-benchmarks-runner-py], [^src-tools-benchmarks-postgres-py], [^src-tools-benchmarks-scenarios-py], [^src-tools-benchmarks-targets-py], [^src-tools-benchmarks-store-py], [^src-tools-benchmarks-adapters-base-py], [^src-tools-benchmarks-adapters-sql-py], [^src-tools-benchmarks-adapters-cache-py], [^src-tools-benchmarks-adapters-storage-py], [^src-tools-benchmarks-data-backend-py], [^src-tools-pgo-init-py], [^src-benchmark-yml], [^src-tests-unit-tools-test-e2e-benchmarks-py], [^src-vertebra-core-cargo-toml], [^src-vertebra-http-cargo-toml], [^src-vertebra-postgres-cargo-toml], [^src-postgres-native-bench-rs].

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-tools-benchmarks-cli-py]: `tools/benchmarks/cli.py` — Repository tooling module (tools/benchmarks/cli.py).
[^src-tools-benchmarks-drivers-py]: `tools/benchmarks/drivers.py` — Unified closed-loop and open-loop load-generator drivers (oha, h2load, ghz, k6).
[^src-tools-benchmarks-fixtures-py]: `tools/benchmarks/fixtures.py` — Container fixture specs (RustFS, Floci, Azurite, SQL, cache) and storage config builders.
[^src-tools-benchmarks-runner-py]: `tools/benchmarks/runner.py` — Repository tooling module (tools/benchmarks/runner.py).
[^src-tools-benchmarks-postgres-py]: `tools/benchmarks/postgres.py` — PostgreSQL comparative benchmark capture and comparator CLI.
[^src-tools-benchmarks-scenarios-py]: `tools/benchmarks/scenarios.py` — Repository tooling module (tools/benchmarks/scenarios.py).
[^src-tools-benchmarks-targets-py]: `tools/benchmarks/targets.py` — Repository tooling module (tools/benchmarks/targets.py).
[^src-tools-benchmarks-store-py]: `tools/benchmarks/store.py` — Repository tooling module (tools/benchmarks/store.py).
[^src-tools-benchmarks-adapters-base-py]: `tools/benchmarks/adapters/base.py` — Shared standalone adapter comparator contract, schema_version=1, and driver_mode stratification.
[^src-tools-benchmarks-adapters-sql-py]: `tools/benchmarks/adapters/sql.py` — Standalone SQL and ORM adapter comparators (PostgreSQL, SQLite, DuckDB, MySQL, Oracle, MSSQL, SQLAlchemy, SQLSpec).
[^src-tools-benchmarks-adapters-cache-py]: `tools/benchmarks/adapters/cache.py` — Standalone Redis and Valkey cache comparators.
[^src-tools-benchmarks-adapters-storage-py]: `tools/benchmarks/adapters/storage.py` — Standalone ObjectStore storage comparators across Memory, Local, RustFS, Floci, and Azurite.
[^src-tools-benchmarks-data-backend-py]: `tools/benchmarks/apps/_data_backend.py` — Shared full-stack benchmark data backend, pool budget, and MinIO ban enforcement.
[^src-tools-pgo-init-py]: `tools/pgo/__init__.py` — Repository tooling module (tools/pgo/__init__.py).
[^src-tools-pgo-rust-pgo-py]: `tools/pgo/rust_pgo.py` — Repository tooling module (tools/pgo/rust_pgo.py).
[^src-tools-pgo-python-pgo-py]: `tools/pgo/python_pgo.py` — Repository tooling module (tools/pgo/python_pgo.py).
[^src-benchmark-yml]: `.github/workflows/benchmark.yml` — GitHub Actions workflow (benchmark.yml).
[^src-tests-unit-test-freethreaded-gil-py]: `src/py/tests/unit/test_freethreaded_gil.py` — Python implementation module (src/py/tests/unit/test_freethreaded_gil.py).
[^src-tests-unit-tools-test-e2e-benchmarks-py]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Consolidated offline end-to-end benchmark test suite.
[^src-vertebra-core-cargo-toml]: `src/rs/vertebra-core/Cargo.toml` — Rust crate manifest (src/rs/vertebra-core/Cargo.toml).
[^src-vertebra-http-cargo-toml]: `src/rs/vertebra-http/Cargo.toml` — Rust crate manifest (src/rs/vertebra-http/Cargo.toml).
[^src-vertebra-postgres-cargo-toml]: `src/rs/vertebra-postgres/Cargo.toml` — Rust crate manifest (src/rs/vertebra-postgres/Cargo.toml).
[^src-postgres-native-bench-rs]: `src/rs/vertebra-postgres/benches/postgres_native.rs` — PostgreSQL native codec, wire bind, PGCOPY, and pgoutput Criterion benches.
