---
type: Guide
title: Polyglot Test Matrix & Environment Setup
description: "Rust, Python, and TypeScript test suites, Makefile and CI entry points, registry auto-setup, extension rebuilds, and /dev/shm cleanup."
status: stable
tags:
  - testing
  - verification
  - python
  - rust
  - typescript
generated:
  by: flow/0.29.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-14T18:05:00Z"
    commit: 8290822e
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 334dc98d
sources:
  - id: src-makefile
    resource: Makefile
    title: Canonical polyglot build, test, lint, docs, and benchmark targets
  - id: src-tools-scripts-setup-env-sh
    resource: tools/scripts/setup-env.sh
    title: "Repository tooling module (tools/scripts/setup-env.sh)"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: Python package metadata, dependencies, and tool configuration
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-tests-conftest-py
    resource: src/py/tests/conftest.py
    title: "Python implementation module (src/py/tests/conftest.py)"
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
  - id: src-ci-yml
    resource: .github/workflows/ci.yml
    title: "GitHub Actions workflow (ci.yml)"
  - id: src-security-yml
    resource: .github/workflows/security.yml
    title: "GitHub Actions security analysis workflow (security.yml)"
  - id: src-db-conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Parameterized database contract and environment readiness gates
  - id: src-storage-public-codec-tests
    resource: src/py/tests/unit/storage/test_storage_store.py
    title: Native storage codec demand, completion and cancellation tests
  - id: src-storage-native-tests
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
    title: Native streaming and normalized S3/B2 multipart tests
  - id: src-storage-shm-tests
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: Named Arrow allocation rejection tests
  - id: src-storage-cursor-view-tests
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: Retained native cursor views and DuckDB driver tests
  - id: src-tests-unit-conftest-py
    resource: "src/py/tests/unit/conftest.py::resp3_server"
    title: Loopback RESP3 server fixture for Redis and Valkey unit tests
  - id: src-tests-unit-server-stage1-hotpath-py
    resource: src/py/tests/unit/server/test_stage1_hotpath_and_314t.py
    title: Server hot-path batch dispatch and free-threaded 3.14t/3.15t unit tests
  - id: src-ts-tests-agent-test-ts
    resource: src/ts/vertebra-client/tests/agent.test.ts
    title: TypeScript Vitest suite for agent, router, and WebMCP client modules
  - id: src-tests-unit-agent-mcp-conformance-py
    resource: src/py/tests/unit/agent/test_mcp_conformance.py
    title: MCP 2026-07-28 protocol, ToolRegistry provider, WebMCP, and HTTP handler conformance suite
  - id: src-agent-mcp-registry-py
    resource: src/py/vertebra/agent/mcp/registry.py
    title: Unified ToolRegistry and MCPRegistry implementation
  - id: src-stack-regressions
    resource: src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
    title: Native pipeline and fallback statement/result/bind regressions
  - id: src-bigquery-stack-regressions
    resource: src/py/tests/unit/extensions/test_sqlspec_bigquery.py
    title: Actual BigQuery driver stack execution over loopback REST
  - id: src-spanner-stack-regressions
    resource: src/py/tests/unit/extensions/test_sqlspec_spanner.py
    title: Actual Spanner driver stack execution over external client ports
  - id: src-verify-postgres-wheel
    resource: tools/scripts/verify_postgres_wheel.py
    title: Standalone vertebra-postgres wheel, sdist, and 14-class isolation verifier
  - id: src-pg-dataplane-tests
    resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
    title: PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM unit tests
  - id: src-postgres-bench-tests
    resource: src/py/tests/unit/tools/test_postgres_benchmarks.py
    title: Offline PostgreSQL benchmark schema and comparator CLI unit tests
  - id: src-provider-regressions
    resource: src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
    title: Native provider ownership, lazy resolution, policy and driver error regressions
  - id: src-provider-schema-regressions
    resource: src/py/tests/unit/agent/test_core_and_observability.py
    title: Neutral provider schema and validation boundaries
  - id: src-provider-snapshot-regressions
    resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
    title: Approval binding and nested invocation ownership across controlled suspension
  - id: src-native-driver-tests
    resource: src/py/tests/unit/db/test_native_db_drivers.py
    title: Cancellation and cloud DBAPI regression coverage
  - id: src-sqlite-returning-tests
    resource: src/py/tests/unit/db/test_sqlite_driver.py
    title: SQLite statement completion and transaction regressions
  - id: src-postgres-transaction-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: PostgreSQL transaction and affected-count regressions
---

# Polyglot Test Matrix & Environment Setup

<!-- truth: start -->
- Verify polyglot changes with `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, and `make review-check`, running `make dev` after any Rust edit before invoking `pytest` or `stubtest`.
- Exercise real local SHM/SQLite/in-memory/loopback backends with unique allocation names and ownership cleanup; isolate full-suite `/dev/shm` cleanup from concurrent workloads.
<!-- truth: end -->

Vertebra runs verification gates across Rust (`cargo test`), Python (`pytest` + AnyIO), and TypeScript (`vitest`), orchestrated through [`Makefile`](../../../../Makefile)[^src-makefile], [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml], and [`src/rs/Cargo.toml`](../../../../src/rs/Cargo.toml)[^src-cargo-toml].

## 1. Registry Setup (`make setup-env`)

[`tools/scripts/setup-env.sh`](../../../../tools/scripts/setup-env.sh)[^src-tools-scripts-setup-env-sh] runs from `make setup-env`, `make install`, and `make upgrade`. It removes stale `uv.toml` and `src/rs/.cargo/config.toml` files, configures public PyPI/npm/crates.io registries on workstations whose mirrors replace them, and is a no-op on external hosts and GitHub Actions.

## 2. Test & Lint Targets (`Makefile`)

| Target | Command | What It Verifies |
|---|---|---|
| `make dev` | `cargo build --workspace` (non-extension crates) + `maturin develop --uv` across all 10 extensions (`_vertebra`, `_bundler`, `_storage`, `_mysql`, `_bigquery`, `_mssql`, `_spanner`, `_duckdb`, `_oracle`, `_postgres`) | Debug native build required after Rust edits before `pytest` (`make build` runs the `--release` counterpart). |
| `make rs-test` | `rm -rf /dev/shm/test-* /dev/shm/*vertebra*` + `cargo test --workspace --features tls,compression,gcp` | Unit and integration tests across all 13 workspace crates. |
| `make py-test` | `make dev` + `mypy.stubtest` across all 10 extensions + `uv run pytest src/py/tests -v` | `.pyi` stub parity plus Python suites under [`src/py/tests/`](../../../../src/py/tests/conftest.py)[^src-tests-conftest-py] (zero third-party `duckdb` dependency outside `--group bench`). |
| `make {duckdb,mysql,mssql,oracle,postgres}-test` & `make db-conformance` | Per-crate `cargo test`, `mypy.stubtest vertebra._<engine>`, engine unit + live conformance/native suites (`VERTEBRA_CONFORMANCE_ENGINES=sqlite,duckdb` by default in `db-conformance`; `oracle-test` runs `test_oracle_driver.py` + `test_driver_conformance.py -k oracle` + `test_oracle_native.py`), `postgres-wheel-check` ([`verify_postgres_wheel.py`](../../../../tools/scripts/verify_postgres_wheel.py)[^src-verify-postgres-wheel]), and `postgres-bench-check` ([`test_postgres_benchmarks.py`](../../../../src/py/tests/unit/tools/test_postgres_benchmarks.py)[^src-postgres-bench-tests]) | Scoped native database crate, stub, live conformance, benchmark comparator, and PEP 420 wheel isolation gates. |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=..." bun run test` | Vitest suites under [`package.json`](../../../../src/ts/vertebra-client/package.json)[^src-package-json]. |
| `make lint-all` | `biome check .` + `ruff check` & `ruff format --check` (`src/py`, `tools`, `docs/examples`) + `mypy` (`tools/benchmarks` included) + `mypy.stubtest` + `cargo fmt --check` + `cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings` + `make review-check` | Read-only CI lint, type, stub, and review-manifest gate (`make lint` applies auto-fixes). |
| `make docs` | `scan_public_docstrings` + `generate_cli_docs --check` + `zensical build --clean` + `postbuild` + `verify_docs_artifacts` | Docstring/CLI parity, Zensical build, `llms.txt`, and `<= 120 KB` / `<= 14` TOC HTML bounds. |
| `make check` | `lint` + `test` (`rs-test` + `py-test` + `ts-test`) + `coverage` | Full release verification gate. |

## 3. CI Matrix (`.github/workflows/ci.yml` & `security.yml`)

[`.github/workflows/ci.yml`](../../../../.github/workflows/ci.yml)[^src-ci-yml] runs on `main` pushes, pull requests, and `workflow_dispatch`. `changes` (`dorny/paths-filter`) classifies diffs into `agents` (`.agents/**`, `.codex/**`, `AGENTS.md`, `.github/workflows/ci.yml`), `python` (`src/py/**`, `tools/**`, `pyproject.toml`, `uv.lock`), `rust` (`src/rs/**`), `typescript` (`src/ts/**`), `docs`, and `ci` (`Makefile`, `.pre-commit-config.yaml`, `biome.json`). [`.github/workflows/security.yml`](../../../../.github/workflows/security.yml)[^src-security-yml] scopes `zizmor` push/PR runs to `.github/**` (plus weekly schedule and `workflow_dispatch`).

| Job | Runs When | Command | Matrix |
|---|---|---|---|
| `review-check` | `agents`, `python`, `ci`, or `workflow_dispatch` | `uv run --no-project --python 3.12 --with "msgspec>=0.18" python -m tools.scripts.review_setup validate` (`fetch-depth: 0`) | `ubuntu-latest` |
| `lint` | `python`, `rust`, `typescript`, `ci`, or `workflow_dispatch` | `make ts-install` + `make lint-all` | `ubuntu-latest` |
| `test-python` | `python`, `rust`, `ci`, or `workflow_dispatch` | `make py-test`; on `3.12` also `make coverage` (`coverage.xml`) | Python `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t` |
| `test-rust` | `rust`, `ci`, or `workflow_dispatch` | Windows `cargo check` (`x86_64-pc-windows-msvc`) + `make rs-test` (`tls,compression,gcp`) | `ubuntu-latest` |
| `test-rust-macos` | `workflow_dispatch` | `cargo test --workspace --features tls,compression,gcp,macos-ulock` | `macos-latest` |
| `test-typescript` | `typescript`, `ci`, or `workflow_dispatch` | `make ts-install` + `make ts-build` + `make ts-test` | `ubuntu-latest` |
| `build-docs` | `docs`, `ci`, or `workflow_dispatch` | `uv sync --frozen --no-install-project --only-group doc` + `make docs` | `ubuntu-latest` |
| `core-wheel-smoke` | `python`, `rust`, `typescript`, `ci`, or `workflow_dispatch` | `uv sync --frozen --group build && make ts-install && make ts-build && make core-wheel-check` | `ubuntu-latest` |
| `postgres-wheel-check` | `python`, `rust`, `ci`, or `workflow_dispatch` | `uv sync --frozen --group build && make ts-install && make ts-build && make postgres-wheel-check` | `ubuntu-latest` |
| `test-db-conformance` | `python`, `rust`, `ci`, or `workflow_dispatch` | `test_driver_conformance.py` + `test_{duckdb,mysql,mssql}_native.py` (and PG `18` replication/feature/deadline suites) gated by `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`, `VERTEBRA_CONFORMANCE_<ENGINE>_REQUIRE_ZERO_SKIPS=1`) and `ServiceVersionMatrix` (`VERTEBRA_CONFORMANCE_{POSTGRES,MYSQL,MARIADB,MSSQL}_VERSION`) | 10-row matrix: PG `14`–`18` (`pgvector/pgvector:pg<ver>`), MySQL `8.4`/`9.1`, MariaDB `11.4`, MSSQL `2022-CU16`, `sqlite,duckdb` (`ubuntu-latest`) |
| `conformance-oracle` | `python`, `rust`, `ci`, or `workflow_dispatch` | `test_driver_conformance.py -k oracle` + `test_oracle_native.py` with `VERTEBRA_CONFORMANCE_ENGINES=oracle`, `VERTEBRA_CONFORMANCE_ORACLE_VERSION=23`, and `VERTEBRA_CONFORMANCE_ORACLE_REQUIRE_ZERO_SKIPS=1` | `gvenzl/oracle-free:23-slim-faststart` (`continue-on-error: true`, `ubuntu-latest`) |

## 4. Suite Inventory, Shared-Memory Hygiene & Boundary Evidence

- **Python**: 1:1 module-mirrored `src/py/tests/unit/` packages with `__init__.py` and top-level imports (`PLC0415`) across server hotpaths[^src-tests-unit-server-stage1-hotpath-py], storage codecs and worker cancellation barriers[^src-storage-public-codec-tests], native drivers[^src-native-driver-tests], SQLite `RETURNING`[^src-sqlite-returning-tests], PostgreSQL transactions/RLS/CDC/benchmarks[^src-postgres-transaction-tests][^src-pg-dataplane-tests][^src-postgres-bench-tests], DuckDB/Arrow cursor retention[^src-storage-cursor-view-tests], loopback `resp3_server` Redis/Valkey tests[^src-tests-unit-conftest-py], SQLSpec/SQLAlchemy stacks[^src-stack-regressions][^src-bigquery-stack-regressions][^src-spanner-stack-regressions], and dual-mode MCP / SSE / skills / UI / ES256 IAP / stdio-bridge CLI / `litestar-mcp` parity suites[^src-tests-unit-agent-mcp-conformance-py][^src-agent-mcp-registry-py][^src-provider-regressions][^src-provider-schema-regressions][^src-provider-snapshot-regressions].
- **Rust & TypeScript**: Integration harnesses in `vertebra-core`, `vertebra-http`, `vertebra-storage`[^src-storage-native-tests], and `vertebra-db`[^src-storage-shm-tests]; Vitest suites in [`tests/agent.test.ts`](../../../../src/ts/vertebra-client/tests/agent.test.ts)[^src-ts-tests-agent-test-ts] and sibling client modules; and cross-engine SQL/Arrow conformance in [`test_driver_conformance.py`](../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-db-conformance].
- **SHM Hygiene**: [`conftest.py`](../../../../src/py/tests/conftest.py)[^src-tests-conftest-py] pins AnyIO to `asyncio`, generates `test-<uuid8>` SHM names, and sweeps `/dev/shm/{test-*,vtb-*,*vertebra*}` post-test; use targeted `cargo test -p <crate>` during parallel runs to avoid `make rs-test` SHM sweeps.

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-tools-scripts-setup-env-sh]: `tools/scripts/setup-env.sh` — Repository tooling module (tools/scripts/setup-env.sh).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-tests-conftest-py]: `src/py/tests/conftest.py` — Python implementation module (src/py/tests/conftest.py).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-ci-yml]: `.github/workflows/ci.yml` — GitHub Actions workflow (ci.yml).
[^src-security-yml]: `.github/workflows/security.yml` — GitHub Actions security analysis workflow (security.yml).
[^src-db-conformance]: `src/py/tests/integration/db/test_driver_conformance.py` — Parameterized database contract and environment readiness gates.
[^src-storage-public-codec-tests]: `src/py/tests/unit/storage/test_storage_store.py` — Native storage codec demand, completion and cancellation tests.
[^src-storage-native-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Native streaming and normalized S3/B2 multipart tests.
[^src-storage-shm-tests]: `src/rs/vertebra-db/src/arrow_shm.rs` — Named Arrow allocation rejection tests.
[^src-storage-cursor-view-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Retained native cursor views and DuckDB driver tests.
[^src-tests-unit-conftest-py]: `src/py/tests/unit/conftest.py::resp3_server` — Loopback RESP3 server fixture for Redis and Valkey unit tests.
[^src-tests-unit-server-stage1-hotpath-py]: `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py` — Server hot-path batch dispatch and free-threaded 3.14t/3.15t unit tests.
[^src-ts-tests-agent-test-ts]: `src/ts/vertebra-client/tests/agent.test.ts` — TypeScript Vitest suite for agent, router, and WebMCP client modules.
[^src-tests-unit-agent-mcp-conformance-py]: `src/py/tests/unit/agent/test_mcp_conformance.py` — MCP 2026-07-28 protocol, ToolRegistry provider, WebMCP, and HTTP handler conformance suite.
[^src-agent-mcp-registry-py]: `src/py/vertebra/agent/mcp/registry.py` — Unified ToolRegistry and MCPRegistry implementation.
[^src-stack-regressions]: `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Native pipeline and fallback statement/result/bind regressions.
[^src-bigquery-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_bigquery.py` — Actual BigQuery driver stack execution over loopback REST.
[^src-spanner-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_spanner.py` — Actual Spanner driver stack execution over external client ports.
[^src-verify-postgres-wheel]: `tools/scripts/verify_postgres_wheel.py` — Standalone vertebra-postgres wheel, sdist, and 14-class isolation verifier.
[^src-pg-dataplane-tests]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM unit tests.
[^src-postgres-bench-tests]: `src/py/tests/unit/tools/test_postgres_benchmarks.py` — Offline PostgreSQL benchmark schema and comparator CLI unit tests.
[^src-provider-regressions]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Native provider ownership, lazy resolution, policy and driver error regressions.
[^src-provider-schema-regressions]: `src/py/tests/unit/agent/test_core_and_observability.py` — Neutral provider schema and validation boundaries.
[^src-provider-snapshot-regressions]: `src/py/tests/unit/agent/test_runtime_and_workflows.py` — Approval binding and nested invocation ownership across controlled suspension.
[^src-native-driver-tests]: `src/py/tests/unit/db/test_native_db_drivers.py` — Cancellation and cloud DBAPI regression coverage.
[^src-sqlite-returning-tests]: `src/py/tests/unit/db/test_sqlite_driver.py` — SQLite statement completion and transaction regressions.
[^src-postgres-transaction-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — PostgreSQL transaction and affected-count regressions.
