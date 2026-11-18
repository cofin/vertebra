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
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-archive
    at: "2026-11-16T15:16:00Z"
    commit: 11020d84
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
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
  - id: src-storage-core-tests
    resource: src/py/tests/unit/storage/test_storage_core_and_ranges.py
    title: Native storage core CRUD, ranges, conditions, and atomic rename tests
  - id: src-storage-arrow-tests
    resource: src/py/tests/unit/storage/test_storage_arrow_and_streaming.py
    title: Native storage Arrow, Parquet, IPC streaming, and buffered reader/writer tests
  - id: src-storage-bulk-tests
    resource: src/py/tests/unit/storage/test_storage_bulk_checksums_and_backends.py
    title: Native storage bulk transfers, multipart uploads, checksums, and backend tests
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
  - id: src-tests-unit-server-hotpath-py
    resource: src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
    title: Server hot-path batch dispatch, subdomain consolidation, and free-threaded sharding tests
  - id: src-ts-tests-agent-test-ts
    resource: src/ts/vertebra-client/tests/agent.test.ts
    title: TypeScript Vitest suite for agent, router, and WebMCP client modules
  - id: src-ts-tests-react-vue-svelte-ts
    resource: src/ts/vertebra-client/tests/unit/react_vue_svelte.test.ts
    title: Behavioral Vitest suite for React, Vue, and Svelte @vertebra/client adapters
  - id: src-ts-tests-angular-htmx-vite-ts
    resource: src/ts/vertebra-client/tests/unit/angular_htmx_vite.test.ts
    title: Behavioral Vitest suite for Angular, HTMX, and Vite @vertebra/client adapters
  - id: src-tests-unit-agent-mcp-core-py
    resource: src/py/tests/unit/agent/test_mcp_core_and_plugin.py
    title: MCP core dispatch, ToolRegistry provider, confirmations, and plugin routing conformance suite
  - id: src-tests-unit-agent-mcp-sessions-py
    resource: src/py/tests/unit/agent/test_mcp_protocol_sessions_and_sse.py
    title: MCP protocol negotiation, session store, batching, and SSE conformance suite
  - id: src-tests-unit-agent-mcp-skills-py
    resource: src/py/tests/unit/agent/test_mcp_skills_and_ui_extensions.py
    title: MCP notifications, resource subscriptions, skills, UI extensions, and WebMCP suite
  - id: src-agent-mcp-registry-py
    resource: src/py/vertebra/agent/mcp/registry.py
    title: Unified ToolRegistry and MCPRegistry implementation
  - id: src-sqlalchemy-dialect-tests
    resource: src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py
    title: SQLAlchemy 2.1 +vtb dialects, types, Alembic, and PEP 249 DBAPI tests
  - id: src-sqlspec-stack-tests
    resource: src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py
    title: SQLSpec StatementStack execution and PostgreSQL/Oracle adapter stack tests
  - id: src-broker-proxy-tests
    resource: src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py
    title: BrokerPool, SQLSpec provider, and Proxy*Driver isolation and cancellation tests
  - id: src-ext-cache-adk-tests
    resource: src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py
    title: Alchemy cache regions, SQLSpec ADK stores, CDC/Events backends, and reconcile tests
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
  - id: src-db-channels-dialers-tests
    resource: src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py
    title: Native database drivers, Cloud SQL and AlloyDB dialers, ChannelBus, and ArrowCursor tests
  - id: src-db-unified-contracts-tests
    resource: src/py/tests/unit/db/test_db_unified_driver_contracts.py
    title: Unified SQL database driver contracts and cloud DB-API semantics tests
  - id: src-db-capabilities-purity-tests
    resource: src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py
    title: Driver capability protocols, pool_key isolation, provide_driver, and AST purity gate tests
  - id: src-oracle-adapter-tests
    resource: src/py/tests/unit/db/test_oracle_adapter.py
    title: Oracle adapter configuration, binds, and query pipeline unit tests
  - id: src-oracle-context-notify-tests
    resource: src/py/tests/unit/db/test_oracle_context_notify_and_queues.py
    title: Oracle session context, AQ notifications, and queue store unit tests
  - id: src-queue-worker-scheduler-tests
    resource: src/py/tests/unit/framework/test_queue_worker_and_scheduler.py
    title: Distributed queue workers, retry/DLQ lifecycle, cron scheduler, and Django tasks tests
  - id: src-queue-backends-channels-tests
    resource: src/py/tests/unit/framework/test_queue_backends_and_channels.py
    title: Queue backend configuration, stores, Lua promotion, and channel wiring tests
  - id: src-sqlite-returning-tests
    resource: src/py/tests/unit/db/test_sqlite_driver.py
    title: SQLite statement completion and transaction regressions
  - id: src-postgres-transaction-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: PostgreSQL transaction and affected-count regressions
  - id: src-postgres-copy-tests
    resource: src/py/tests/unit/db/test_postgres_native_copy.py
    title: PostgreSQL native COPY protocol and binary stream unit tests
  - id: src-postgres-copy-cancel-tests
    resource: src/py/tests/unit/db/test_postgres_native_copy_cancel_and_buffers.py
    title: PostgreSQL native COPY cancellation and buffer lifecycle unit tests
  - id: src-postgres-arrow-streams-tests
    resource: src/py/tests/unit/db/test_postgres_native_arrow_streams.py
    title: PostgreSQL native Arrow stream unit tests
  - id: src-postgres-listener-tests
    resource: src/py/tests/unit/db/test_postgres_native_listener.py
    title: PostgreSQL native async/sync LISTEN and NOTIFY unit tests
  - id: src-postgres-notify-source-tests
    resource: src/py/tests/unit/db/test_postgres_notification_source.py
    title: PostgreSQL notification source and OAuth token refresh unit tests
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
| `make dev` | `cargo build --workspace` (non-extension crates) + `maturin develop --uv` across all 13 extensions (`_vertebra`, `_bundler`, `_storage`, `_mysql`, `_bigquery`, `_mssql`, `_spanner`, `_duckdb`, `_oracle`, `_postgres`, `_sqlite`, `_redis`, `_valkey`) | Debug native build required after Rust edits before `pytest` (`make build` runs the `--release` counterpart). |
| `make rs-test` | `rm -rf /dev/shm/test-* /dev/shm/*vertebra*` + `cargo test --workspace --features tls,http3,compression,gcp` + non-`http3` `config::tests` & `server` fail-loud check | Unit and integration tests across all 16 workspace crates plus `--no-default-features` / `--features tls,compression` gate coverage. |
| `make py-test` | `make dev` + `mypy.stubtest` across all 13 extensions + `uv run pytest src/py/tests -v` | `.pyi` stub parity plus Python suites under [`src/py/tests/`](../../../../src/py/tests/conftest.py)[^src-tests-conftest-py] (zero third-party `duckdb` dependency outside `--group bench`). |
| `make {sqlite,duckdb,mysql,mssql,oracle,postgres,redis,valkey}-test`, `make db-conformance`, & wheel checks | Per-crate `cargo test`, `mypy.stubtest vertebra._<engine>`, engine unit + live conformance suites (`make db-conformance ENGINE=<duckdb\|mysql\|mariadb\|mssql\|oracle\|redis\|sqlite\|valkey> [VERSION=<v>]` under zero-skip `ConformanceGate`), all 13 `*-wheel-check` targets (`core`, `sqlite`, `duckdb`, `mysql`, `mssql`, `oracle`, `postgres` via [`verify_postgres_wheel.py`](../../../../tools/scripts/verify_postgres_wheel.py)[^src-verify-postgres-wheel], `redis`, `valkey`, `storage`, `bigquery`, `spanner`, `bundler`), and `postgres-bench-check` ([`test_postgres_benchmarks.py`](../../../../src/py/tests/unit/tools/test_postgres_benchmarks.py)[^src-postgres-bench-tests]) | Scoped native database/cache crate, stub, live conformance (`oracle-test` runs `test_oracle_adapter.py` + `test_oracle_context_notify_and_queues.py`; `redis-test`/`valkey-test` run `test_queue_backends_and_channels.py`), benchmark comparator, and PEP 420 wheel isolation gates. |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=..." bun run test` | Vitest suites under [`package.json`](../../../../src/ts/vertebra-client/package.json)[^src-package-json], including behavioral React/Vue/Svelte and Angular/HTMX/Vite adapter suites. |
| `make lint-all` | `biome check .` + `tsc --noEmit` + `ruff check` & `ruff format --check` + `mypy` (`src/py/vertebra`, `src/py/tests/{unit,integration}`, `tools/{scripts,pgo,benchmarks,docs}`, `docs/examples`) + `pyright src/py/vertebra` + `tools/scripts/check_extras.py` + fail-loud 13-extension `find_spec` & `mypy.stubtest` + `cargo fmt --check` + `cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings` | Read-only CI lint, strict TS + Python type-check, extras parity, and fail-loud stub gate (`uv run --no-sync` on all `uv` steps; `make lint` applies auto-fixes). |
| `make docs` | `scan_public_docstrings` + `generate_cli_docs --check` + `zensical build --clean` + `postbuild` + `verify_docs_artifacts` | Docstring/CLI parity, Zensical build, `llms.txt`, and `<= 120 KB` / `<= 14` TOC HTML bounds. |
| `make check` | `lint` + `test` (`rs-test` + `py-test` + `ts-test`) + `coverage` | Full release verification gate. |

## 3. CI Matrix (`.github/workflows/ci.yml` & `security.yml`)

[`.github/workflows/ci.yml`](../../../../.github/workflows/ci.yml)[^src-ci-yml] runs on `main` pushes, pull requests, and `workflow_dispatch`. `changes` (`dorny/paths-filter`) classifies diffs into `python` (`src/py/**`, `tools/**`, `pyproject.toml`, `uv.lock`), `rust` (`src/rs/**`), `rust_macos` (`src/rs/**`, `.github/workflows/ci.yml`), `typescript` (`src/ts/**`), `packaging` (`src/rs/vertebra-*/**`, `src/py/vertebra/**`, `tools/scripts/*wheel*.py`, `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`), `docs`, and `ci` (`Makefile`, `.pre-commit-config.yaml`, `biome.json`). [`.github/workflows/security.yml`](../../../../.github/workflows/security.yml)[^src-security-yml] scopes `zizmor` push/PR runs to `.github/**` (plus weekly schedule and `workflow_dispatch`).

| Job | Runs When | Command | Matrix |
|---|---|---|---|
| `lint` | `python`, `rust`, `typescript`, `ci`, or `workflow_dispatch` | `uv sync --frozen --all-extras --group test --group lint --group build`, `make ts-install && make ts-build`, `make dev`, then `make lint-all` | `ubuntu-latest` |
| `test-python` | `python`, `rust`, `ci`, or `workflow_dispatch` | `make py-test`; on `3.12` also `make coverage` (`coverage.xml`) | Python `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t` |
| `test-rust` | `rust`, `ci`, or `workflow_dispatch` | Windows `cargo check` (`x86_64-pc-windows-msvc`) + `make rs-test` (`tls,http3,compression,gcp`) | `ubuntu-latest` |
| `test-rust-macos` | `rust_macos`, `ci`, or `workflow_dispatch` | `cargo test --workspace --features tls,http3,compression,gcp` | `macos-latest` |
| `test-typescript` | `typescript`, `ci`, or `workflow_dispatch` | `make ts-install` + `make ts-build` + `make ts-test` | `ubuntu-latest` |
| `build-docs` | `docs`, `ci`, or `workflow_dispatch` | `uv sync --frozen --no-install-project --only-group doc` + `make docs` | `ubuntu-latest` |
| `core-wheel-smoke`, `{sqlite,postgres,redis,valkey}-wheel-check` | `python`, `rust`, (`typescript` for core), `ci`, or `workflow_dispatch` | `uv sync --frozen --group build && make ts-install && make ts-build && make {core,sqlite-test && sqlite,postgres,redis,valkey}-wheel-check` | `ubuntu-latest` |
| `optional-wheel-checks` | `packaging` or `workflow_dispatch` | `uv sync --frozen --group build && make ts-install && make ts-build && make ${{ matrix.package }}-wheel-check` | 8-row matrix: `duckdb`, `mysql`, `mssql`, `oracle`, `storage`, `bigquery`, `spanner`, `bundler` (`ubuntu-latest`) |
| `postgres-conformance` | `python`, `rust`, `ci`, or `workflow_dispatch` | `test_driver_conformance.py -v` filtered by `VERTEBRA_CONFORMANCE_ENGINES=postgres`, `VERTEBRA_CONFORMANCE_POSTGRES_VERSION=<pg_version>`, and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, plus live replication/feature/deadline suites on PG `18` | PG `14`–`18` (`pgvector/pgvector:pg<ver>`, `ubuntu-latest`) |
| `test-db-conformance` | `python`, `rust`, `ci`, or `workflow_dispatch` | `make db-conformance ENGINE=${{ matrix.engine }} VERSION=${{ matrix.version }}` (`test_driver_conformance.py` + `test_{duckdb,mysql,mssql}_native.py`) gated by zero-skip `ConformanceGate` (`VERTEBRA_CONFORMANCE_ENGINES`) and `ServiceVersionMatrix` | 16-row matrix: `sqlite` (`3.12`, `3.14t`), `duckdb` (`3.12`, `3.14t`), `redis` (`3.12`, `3.14t`), `valkey` (`3.12`, `3.14t`), `mysql` (`8.0`/`8.4`/`9.7` on `3.12`; `8.4` on `3.14t`), `mariadb` (`11.4`, `11.8`), `mssql` (`2022`, `2025`) (`ubuntu-latest`) |
| `conformance-oracle` | `python`, `rust`, `ci`, or `workflow_dispatch` | `make db-conformance ENGINE=oracle VERSION=23ai` (`test_driver_conformance.py` + `test_oracle_native.py` with `VERTEBRA_CONFORMANCE_ENGINES=oracle` and `VERTEBRA_CONFORMANCE_ORACLE_VERSION=23ai`) | `gvenzl/oracle-free:23-slim-faststart` (blocking, `ubuntu-latest`) |

## 4. Suite Inventory, Shared-Memory Hygiene & Boundary Evidence

- **Python**: 1:1 module-mirrored `src/py/tests/unit/` packages with `__init__.py` and top-level imports (`PLC0415`) across server hotpaths and free-threaded sharding[^src-tests-unit-server-hotpath-py], split storage core/ranges, Arrow/streaming, and bulk/checksums/backends suites[^src-storage-core-tests][^src-storage-arrow-tests][^src-storage-bulk-tests], split DB driver/channel/dialer, unified SQL contract, and capability/purity gate suites[^src-db-channels-dialers-tests][^src-db-unified-contracts-tests][^src-db-capabilities-purity-tests], split Oracle adapter and context/notify/queue suites[^src-oracle-adapter-tests][^src-oracle-context-notify-tests], SQLite `RETURNING`[^src-sqlite-returning-tests], PostgreSQL transactions, split COPY/Arrow, split listener/notify-source, RLS/CDC, and benchmark suites[^src-postgres-transaction-tests][^src-postgres-copy-tests][^src-postgres-copy-cancel-tests][^src-postgres-arrow-streams-tests][^src-postgres-listener-tests][^src-postgres-notify-source-tests][^src-pg-dataplane-tests][^src-postgres-bench-tests], DuckDB/Arrow cursor retention[^src-storage-cursor-view-tests], loopback `resp3_server` Redis/Valkey tests[^src-tests-unit-conftest-py], split queue worker/scheduler and backend/channel suites[^src-queue-worker-scheduler-tests][^src-queue-backends-channels-tests], split SQLAlchemy `+vtb` dialect/DBAPI, SQLSpec stack, `BrokerPool`/proxy, and cache/ADK/events suites[^src-sqlalchemy-dialect-tests][^src-sqlspec-stack-tests][^src-broker-proxy-tests][^src-ext-cache-adk-tests][^src-bigquery-stack-regressions][^src-spanner-stack-regressions], and split MCP core/plugin, protocol/sessions/SSE, skills/UI, and agent suites[^src-tests-unit-agent-mcp-core-py][^src-tests-unit-agent-mcp-sessions-py][^src-tests-unit-agent-mcp-skills-py][^src-agent-mcp-registry-py][^src-provider-regressions][^src-provider-schema-regressions][^src-provider-snapshot-regressions].
- **Rust & TypeScript**: Integration harnesses in `vertebra-core`, `vertebra-http`, `vertebra-storage`[^src-storage-native-tests], and `vertebra-db`[^src-storage-shm-tests]; Vitest suites in [`tests/agent.test.ts`](../../../../src/ts/vertebra-client/tests/agent.test.ts)[^src-ts-tests-agent-test-ts], [`tests/unit/react_vue_svelte.test.ts`](../../../../src/ts/vertebra-client/tests/unit/react_vue_svelte.test.ts)[^src-ts-tests-react-vue-svelte-ts], [`tests/unit/angular_htmx_vite.test.ts`](../../../../src/ts/vertebra-client/tests/unit/angular_htmx_vite.test.ts)[^src-ts-tests-angular-htmx-vite-ts], and sibling client modules; and zero-skip cross-engine SQL/Arrow conformance in [`test_driver_conformance.py`](../../../../src/py/tests/integration/db/test_driver_conformance.py)[^src-db-conformance].
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
[^src-storage-core-tests]: `src/py/tests/unit/storage/test_storage_core_and_ranges.py` — Native storage core CRUD, ranges, conditions, and atomic rename tests.
[^src-storage-arrow-tests]: `src/py/tests/unit/storage/test_storage_arrow_and_streaming.py` — Native storage Arrow, Parquet, IPC streaming, and buffered reader/writer tests.
[^src-storage-bulk-tests]: `src/py/tests/unit/storage/test_storage_bulk_checksums_and_backends.py` — Native storage bulk transfers, multipart uploads, checksums, and backend tests.
[^src-storage-native-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Native streaming and normalized S3/B2 multipart tests.
[^src-storage-shm-tests]: `src/rs/vertebra-db/src/arrow_shm.rs` — Named Arrow allocation rejection tests.
[^src-storage-cursor-view-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Retained native cursor views and DuckDB driver tests.
[^src-tests-unit-conftest-py]: `src/py/tests/unit/conftest.py::resp3_server` — Loopback RESP3 server fixture for Redis and Valkey unit tests.
[^src-tests-unit-server-hotpath-py]: `src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py` — Server hot-path batch dispatch, subdomain consolidation, and free-threaded sharding tests.
[^src-ts-tests-agent-test-ts]: `src/ts/vertebra-client/tests/agent.test.ts` — TypeScript Vitest suite for agent, router, and WebMCP client modules.
[^src-ts-tests-react-vue-svelte-ts]: `src/ts/vertebra-client/tests/unit/react_vue_svelte.test.ts` — Behavioral Vitest suite for React, Vue, and Svelte adapters.
[^src-ts-tests-angular-htmx-vite-ts]: `src/ts/vertebra-client/tests/unit/angular_htmx_vite.test.ts` — Behavioral Vitest suite for Angular, HTMX, and Vite adapters.
[^src-tests-unit-agent-mcp-core-py]: `src/py/tests/unit/agent/test_mcp_core_and_plugin.py` — MCP core dispatch, ToolRegistry provider, confirmations, and plugin routing conformance suite.
[^src-tests-unit-agent-mcp-sessions-py]: `src/py/tests/unit/agent/test_mcp_protocol_sessions_and_sse.py` — MCP protocol negotiation, session store, batching, and SSE conformance suite.
[^src-tests-unit-agent-mcp-skills-py]: `src/py/tests/unit/agent/test_mcp_skills_and_ui_extensions.py` — MCP notifications, resource subscriptions, skills, UI extensions, and WebMCP suite.
[^src-agent-mcp-registry-py]: `src/py/vertebra/agent/mcp/registry.py` — Unified ToolRegistry and MCPRegistry implementation.
[^src-sqlalchemy-dialect-tests]: `src/py/tests/unit/extensions/test_sqlalchemy_vtb_dialects_and_dbapi.py` — SQLAlchemy 2.1 +vtb dialects, types, Alembic, and PEP 249 DBAPI tests.
[^src-sqlspec-stack-tests]: `src/py/tests/unit/extensions/test_sqlspec_vtb_stacks.py` — SQLSpec StatementStack execution and PostgreSQL/Oracle adapter stack tests.
[^src-broker-proxy-tests]: `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py` — BrokerPool, SQLSpec provider, and Proxy*Driver isolation and cancellation tests.
[^src-ext-cache-adk-tests]: `src/py/tests/unit/extensions/test_extensions_cache_adk_and_events.py` — Alchemy cache regions, SQLSpec ADK stores, CDC/Events backends, and reconcile tests.
[^src-bigquery-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_bigquery.py` — Actual BigQuery driver stack execution over loopback REST.
[^src-spanner-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_spanner.py` — Actual Spanner driver stack execution over external client ports.
[^src-verify-postgres-wheel]: `tools/scripts/verify_postgres_wheel.py` — Standalone vertebra-postgres wheel, sdist, and 14-class isolation verifier.
[^src-pg-dataplane-tests]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — PostgreSQL RLS tenant predicates, CDC spill bridge, DataFusion, and memfd SHM unit tests.
[^src-postgres-bench-tests]: `src/py/tests/unit/tools/test_postgres_benchmarks.py` — Offline PostgreSQL benchmark schema and comparator CLI unit tests.
[^src-provider-regressions]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Native provider ownership, lazy resolution, policy and driver error regressions.
[^src-provider-schema-regressions]: `src/py/tests/unit/agent/test_core_and_observability.py` — Neutral provider schema and validation boundaries.
[^src-provider-snapshot-regressions]: `src/py/tests/unit/agent/test_runtime_and_workflows.py` — Approval binding and nested invocation ownership across controlled suspension.
[^src-db-channels-dialers-tests]: `src/py/tests/unit/db/test_db_drivers_channels_and_dialers.py` — Native database drivers, Cloud SQL and AlloyDB dialers, ChannelBus, and ArrowCursor tests.
[^src-db-unified-contracts-tests]: `src/py/tests/unit/db/test_db_unified_driver_contracts.py` — Unified SQL database driver contracts and cloud DB-API semantics tests.
[^src-db-capabilities-purity-tests]: `src/py/tests/unit/db/test_db_capabilities_and_purity_gates.py` — Driver capability protocols, pool_key isolation, provide_driver, and AST purity gate tests.
[^src-oracle-adapter-tests]: `src/py/tests/unit/db/test_oracle_adapter.py` — Oracle adapter configuration, binds, and query pipeline unit tests.
[^src-oracle-context-notify-tests]: `src/py/tests/unit/db/test_oracle_context_notify_and_queues.py` — Oracle session context, AQ notifications, and queue store unit tests.
[^src-queue-worker-scheduler-tests]: `src/py/tests/unit/framework/test_queue_worker_and_scheduler.py` — Distributed queue workers, retry/DLQ lifecycle, cron scheduler, and Django tasks tests.
[^src-queue-backends-channels-tests]: `src/py/tests/unit/framework/test_queue_backends_and_channels.py` — Queue backend configuration, stores, Lua promotion, and channel wiring tests.
[^src-sqlite-returning-tests]: `src/py/tests/unit/db/test_sqlite_driver.py` — SQLite statement completion and transaction regressions.
[^src-postgres-transaction-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — PostgreSQL transaction and affected-count regressions.
[^src-postgres-copy-tests]: `src/py/tests/unit/db/test_postgres_native_copy.py` — PostgreSQL native COPY protocol and binary stream unit tests.
[^src-postgres-copy-cancel-tests]: `src/py/tests/unit/db/test_postgres_native_copy_cancel_and_buffers.py` — PostgreSQL native COPY cancellation and buffer lifecycle unit tests.
[^src-postgres-arrow-streams-tests]: `src/py/tests/unit/db/test_postgres_native_arrow_streams.py` — PostgreSQL native Arrow stream unit tests.
[^src-postgres-listener-tests]: `src/py/tests/unit/db/test_postgres_native_listener.py` — PostgreSQL native async/sync LISTEN and NOTIFY unit tests.
[^src-postgres-notify-source-tests]: `src/py/tests/unit/db/test_postgres_notification_source.py` — PostgreSQL notification source and OAuth token refresh unit tests.
