---
type: Guide
title: Polyglot Test Matrix & Environment Setup
description: Rust, Python, and TypeScript test suites, their Makefile and CI entry
  points, registry auto-setup, extension rebuilds, and /dev/shm cleanup.
status: stable
tags:
- testing
- verification
- python
- rust
- typescript
generated:
  by: flow/0.26.0
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:flow-learn
  at: '2026-11-08T18:26:31.591504Z'
  commit: cda502a4
sources:
- id: src-stack-regressions
  resource: src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  title: Native pipeline and fallback statement/result/bind regressions
- id: src-bigquery-stack-regressions
  resource: src/py/tests/unit/extensions/test_sqlspec_bigquery.py
  title: Actual BigQuery driver stack execution over loopback REST
- id: src-spanner-stack-regressions
  resource: src/py/tests/unit/extensions/test_sqlspec_spanner.py
  title: Actual Spanner driver stack execution over external client ports
- id: src-psycopg-bind-regressions
  resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  title: Optional real psycopg converter acceptance for PostgreSQL primitives
- id: src-provider-regressions
  resource: src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  title: Native provider ownership, lazy resolution, policy and driver error regressions
- id: src-provider-schema-regressions
  resource: src/py/tests/unit/agent/test_core_and_observability.py
  title: Neutral provider schema and validation boundaries
- id: src-provider-snapshot-regressions
  resource: src/py/tests/unit/agent/test_runtime_and_workflows.py
  title: Approval binding and nested invocation ownership across controlled suspension
- id: src-tests-unit-agent-mcp-conformance-py
  resource: src/py/tests/unit/agent/test_mcp_conformance.py
  title: MCP 2026-07-28 protocol, ToolRegistry provider, WebMCP, and HTTP handler
    conformance suite
- id: src-agent-mcp-registry-py
  resource: src/py/vertebra/agent/mcp/registry.py
  title: Unified ToolRegistry and MCPRegistry implementation
- id: src-native-driver-tests
  resource: src/py/tests/unit/db/test_native_db_drivers.py
  title: Cancellation and cloud DBAPI regression coverage
- id: src-sqlite-returning-tests
  resource: src/py/tests/unit/db/test_sqlite_driver.py
  title: SQLite statement completion and transaction regressions
- id: src-postgres-transaction-tests
  resource: src/py/tests/unit/db/test_postgres_driver.py
  title: Asyncpg transaction and affected-count regressions
- id: src-tests-unit-server-stage1-hotpath-py
  resource: src/py/tests/unit/server/test_stage1_hotpath_and_314t.py
  title: Server hot-path batch dispatch and free-threaded 3.14t/3.15t unit tests
- id: src-ts-tests-agent-test-ts
  resource: src/ts/vertebra-client/tests/agent.test.ts
  title: TypeScript Vitest suite for agent, router, and WebMCP client modules
- id: src-makefile
  resource: Makefile
  title: Canonical polyglot build, test, lint, docs, and benchmark targets
- id: src-tools-scripts-setup-env-sh
  resource: tools/scripts/setup-env.sh
  title: Repository tooling module (tools/scripts/setup-env.sh)
- id: src-pyproject-toml
  resource: pyproject.toml
  title: Python package metadata, dependencies, and tool configuration
- id: src-cargo-toml
  resource: src/rs/Cargo.toml
  title: Rust crate manifest (src/rs/Cargo.toml)
- id: src-tests-conftest-py
  resource: src/py/tests/conftest.py
  title: Python implementation module (src/py/tests/conftest.py)
- id: src-package-json
  resource: src/ts/vertebra-client/package.json
  title: TypeScript package manifest (src/ts/vertebra-client/package.json)
- id: src-ci-yml
  resource: .github/workflows/ci.yml
  title: GitHub Actions workflow (ci.yml)
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
  title: Retained native cursor views after release
---

# Polyglot Test Matrix & Environment Setup

<!-- truth: start -->
- Verify polyglot changes with `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, and `make review-check`, running `make dev` after any Rust edit before invoking `pytest`.
- Exercise real local SHM/SQLite/in-memory backends with unique allocation names and ownership cleanup; isolate full-suite cleanup from concurrent workloads.
<!-- truth: end -->

Vertebra runs verification gates in three languages: Rust (`cargo test`), Python (`pytest` with the AnyIO plugin), and TypeScript (`vitest`). The [`Makefile`](../../../../Makefile)[^src-makefile] drives all three locally and in CI.

## 1. Registry Setup (`make setup-env`)

[`tools/scripts/setup-env.sh`](../../../../tools/scripts/setup-env.sh)[^src-tools-scripts-setup-env-sh] runs from `make setup-env`, `make install`, and `make upgrade`:

- On every run it deletes `uv.toml` and `src/rs/.cargo/config.toml` if present. Existing `.npmrc` files are never removed.
- On internal corporate Linux workstations whose package mirrors replace the public registries (detected by an `/etc/os-release` match in the script), it writes `uv.toml` (`[[index]] url = "https://pypi.org/simple"`, `default = true`), appends `registry=https://registry.npmjs.org` to `.npmrc` and `src/ts/vertebra-client/.npmrc`, and writes `src/rs/.cargo/config.toml` (`[registries.crates-io] protocol = "sparse"`, `[net] git-fetch-with-cli = true`).
- On other hosts and in GitHub Actions it writes nothing, so toolchain defaults apply.

The `Makefile` applies the same host check and exports `NPM_CONFIG_REGISTRY`, `PIP_INDEX_URL`, `UV_INDEX_URL`, `UV_DEFAULT_INDEX`, `CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse`, and `CARGO_NET_GIT_FETCH_WITH_CLI=true` for every target.

## 2. Test & Lint Targets (`Makefile`)

| Target | Command | What It Verifies |
|---|---|---|
| `make dev` | `cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage --exclude vertebra-bigquery --exclude vertebra-spanner` + `uv run maturin develop --uv` for `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, and `vertebra-spanner` | Builds non-extension Rust crates and installs debug `vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`, `vertebra._bigquery`, and `vertebra._spanner` into `.venv` (`make build` runs the `--release` / `-r` counterpart plus `uv build`). Required after a Rust edit before running `pytest`. |
| `make rs-test` | `rm -rf /dev/shm/test-* /dev/shm/*vertebra*` + `cd src/rs && cargo test --workspace --features tls,compression,gcp` | Unit and integration tests across all 8 workspace crates (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `vertebra-bigquery`, `vertebra-spanner`). |
| `make py-test` | `make dev` + `uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner` + `uv run pytest src/py/tests -v` | `.pyi` stub parity against all 5 compiled PyO3 extensions plus Python suites under [`src/py/tests/`](../../../../src/py/tests/conftest.py)[^src-tests-conftest-py]. |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=${TMPDIR:-/tmp}/vertebra-vitest-localstorage" bun run test` | Every Vitest suite under `src/ts/vertebra-client/tests/`. |
| `make test` | `rs-test` + `py-test` + `ts-test` | All three languages. |
| `make lint` | `make ts-lint` + `ruff check --fix --unsafe-fixes` & `ruff format` + `cargo fmt --all` + `cargo clippy --fix --allow-dirty --allow-staged --workspace --all-targets --features tls,compression,gcp -- -D warnings` + `mypy` + `mypy.stubtest` across installed native extensions (`vertebra._vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`) + `make review-check` | Auto-fixing lint, format, type, stub, and review-manifest gate. |
| `make lint-all` | `bunx biome check .` + `ruff check` & `ruff format --check` (`src/py/vertebra src/py/tests tools/scripts tools/pgo tools/docs docs/examples`) + `mypy src/py/vertebra tools/scripts tools/pgo tools/docs docs/examples` + `mypy.stubtest` across installed native extensions (`vertebra._vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`) + `cargo fmt --all -- --check` + `cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings` + `make review-check` | Read-only lint, type, stub, and review-manifest gate. |
| `make coverage` | `uv run pytest src/py/tests --cov --quiet` + `coverage xml` + `coverage html` | Python coverage only (`coverage.xml`, `htmlcov/`). |
| `make docs` | `scan_public_docstrings` + `generate_cli_docs --check` + `zensical build --clean` + `postbuild` + `verify_docs_artifacts` | Docstring/overload/`TypedDict`/`Struct` hygiene, CLI reference parity, Zensical build (`site_url = "https://cofin.github.io/vertebra/"`, `repo_name = "cofin/vertebra"`), `llms.txt`/`llms-full.txt` + legacy `.html` redirects, `<= 120 KB` reference HTML / `<= 14` secondary TOC items, and zero empty `Attributes:` rows (`_EMPTY_ATTRIBUTE_ROW_PATTERN`). |
| `make check` | `lint` + `test` + `coverage` | Full local gate. `lint` applies auto-fixes. |

## 3. CI Matrix (`.github/workflows/ci.yml`)

CI runs on pushes to `main` and on every pull request. A `changes` job (`dorny/paths-filter`) classifies paths as `python` (`src/py/**`, `tools/docs/**`, `docs/examples/**`, `pyproject.toml`, `uv.lock`), `rust` (`src/rs/**`), `typescript` (`src/ts/**`), `docs` (`docs/**`, `zensical.toml`, `tools/docs/**`, `.github/workflows/docs.yml`, `src/py/vertebra/**`, `src/ts/vertebra-client/src/**`, `pyproject.toml`, `uv.lock`), or `ci` (`.github/**`, `Makefile`, `.pre-commit-config.yaml`, `biome.json`).

| Job | Runs When | Command | Matrix |
|---|---|---|---|
| `lint` | always | `make ts-install` + `make lint-all` | `ubuntu-latest` |
| `test-python` | `python`, `rust`, or `ci` changed | `make py-test`; on 3.12 also `make coverage` and uploads `coverage.xml` | Python `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t` (`3.15`/`3.15t` pre-release matrix entries) |
| `test-rust` | `rust` or `ci` changed | `make rs-test` | `ubuntu-latest` |
| `test-rust-features` | `rust` or `ci` changed | `cargo test --workspace --features "$CARGO_FEATURES"` | `ubuntu-latest` `tls,compression`; `macos-latest` `tls,compression,macos-ulock` |
| `test-typescript` | `typescript` or `ci` changed | `make ts-install` + `make ts-build` + `make ts-test` | `ubuntu-latest` |
| `build-docs` | `docs` or `ci` changed | `uv sync --no-install-project --group doc` + `make docs` | `ubuntu-latest` |

## 4. Suite Inventory & Test Quality Standards

**Python** (`[tool.pytest.ini_options] testpaths = ["src/py/tests"]`, dependency group `test` = `pytest`, `pytest-cov>=5.0`, `anyio`, `[tool.coverage.report] fail_under = 80`):

- **1:1 Module-Mirrored Layout**: Every directory under `src/py/tests/unit/` mirrors `src/py/vertebra/` (`unit/server/`, `unit/supervisor/`, `unit/framework/`, `unit/extensions/`, `unit/db/`, `unit/storage/`, `unit/telemetry/`, `unit/codegen/`, `unit/build/`, `unit/cli/`, `unit/ffi/`, `unit/native/`, `unit/utils/`, `unit/agent/`) and contains an `__init__.py` so `ruff` (`INP001`) and `pytest` resolve package namespaces cleanly.
- **Top-Level Imports (`PLC0415`) & Server Suite Coverage**: All test files place imports at module top level (`PLC0415` enforced) across `src/py/tests/unit/server/` ([`test_stage1_hotpath_and_314t.py`](../../../../src/py/tests/unit/server/test_stage1_hotpath_and_314t.py)[^src-tests-unit-server-stage1-hotpath-py], `test_runtime_tuning.py`, `test_runner.py`, `test_asgi.py`, `test_wsgi.py`, `test_rsgi_protocol.py`, and `test_litestar_direct_fastpath.py`).
- **Strong vs. Weak Assertions**: Assert exact observable outputs (status codes, wire bytes, SQL strings + bind tuples, round-trip decoded frames, state transitions, and negative error paths with `pytest.raises(SpecificException, match=...)`) rather than `assert x is not None` or `hasattr` checks.

**Rust**: Single-binary integration harnesses (`tests/it/main.rs`) in `vertebra-core` (`tests/it/{shm,spsc,mpmc,streaming,tiered_ring}.rs`) and `vertebra-http` (`tests/it/{server,routing,batcher,dispatcher,backpressure,tls,h2c_mtls,http3,grpc,sse,pubsub,websocket,fastpaths,discovery,streaming}.rs`), plus `vertebra-storage/tests/storage_engine_tests.rs` and `vertebra-db/tests/gcp_credentials_tests.rs` (`--features gcp`). `vertebra-py`, `vertebra-bundler`, `vertebra-db`, `vertebra-bigquery`, and `vertebra-spanner` run crate unit tests (`#[cfg(test)]`) and integration suites under `cargo test --workspace --features tls,compression,gcp`.

**TypeScript**: `package.json` runs `vitest run` over `tests/unit/client.test.ts`, `tests/unit/store.test.ts`, and the top-level [`tests/agent.test.ts`](../../../../src/ts/vertebra-client/tests/agent.test.ts)[^src-ts-tests-agent-test-ts] (covering `./agent` `McpClient`, `A2aClient`, `AgentStreamClient`, and `WorkflowLiveSubscriber`; `./router` `routeUrl`, `wsUrl`, and `interpolateRoutePath`; and `./webmcp` `registerWebMCPTools`), `apex`, `arrow`, `live`, `realtime`, `telemetry`, and `wasm` suites (including error-injection, retry-exhaustion, and reconnect-cap coverage).

## 5. Async Tests & Shared-Memory Hygiene

[`src/py/tests/conftest.py`](../../../../src/py/tests/conftest.py) pins `anyio_backend` to `asyncio`; mark async tests with `@pytest.mark.anyio`. Its `shm_name` fixture returns unique `test-<uuid8>` names. The autouse `_cleanup_shm` removes `/dev/shm/test-*`, `/dev/shm/vtb-*`, and `/dev/shm/*vertebra*` after each test. `make rs-test` removes only the `test-*` and `*vertebra*` globs before Cargo. These are broad cleanup operations; run full suites in an isolated SHM namespace when other workloads share the host.[^src-tests-conftest-py][^src-makefile]

For shared-workspace Rust verification, run `cargo test --workspace --features tls,compression,gcp --locked` from `src/rs` directly to omit the Make cleanup prelude. New ownership tests unlink only their own named allocations. Linux anonymous `memfd_create` regions leave no `/dev/shm` name; named Arrow mappings still require explicit lifecycle assertions.

### Native storage and Arrow evidence

Rebuild all five extensions before Python acceptance after native changes, then run their stubtest gate. Set `PYO3_PYTHON` to the project interpreter; embedded Python Rust tests also need `src/py` and that interpreter's site-packages on `PYTHONPATH`. Preserve installed optional extras with `UV_NO_SYNC=1` when running existing environments.[^src-makefile]

`test_storage_store.py` exercises real native stores and installed codecs: projected rowgroup demand, dictionary and empty-schema retention, IPC-file fallback, native GET/PUT barriers, producer abort, and repeated-cancellation worker barriers. A passing cancellation case must retain owners until both worker and loop-side I/O exit. S3 success fixtures enforce the 5 MiB minimum for every non-final uploaded part, accept a short final part, complete the actual request, and decode the joined output. Cancellation-only or permissive multipart fixtures cannot establish successful provider publication.[^src-storage-public-codec-tests]

Native tests cover incremental 8 MiB S3/R2/OCI parts and S3/B2 backend normalization. Invalid named allocation must report failure without registering anonymous backing; retained native cursor views must decode after release/unlink. Loopback HTTP fixtures establish these client/wire contracts, not live cloud-service acceptance.[^src-storage-native-tests][^src-storage-shm-tests][^src-storage-cursor-view-tests]

## Database driver conformance

`src/py/tests/integration/db/test_driver_conformance.py` parameterizes the shared sync and async SQL contract across eight adapters, checking scalar conversion, execution return values, empty results versus SQL `NULL`, transaction commit and rollback, typed errors, Arrow round trips, idle cancellation, and advertised sync/async pipelining capabilities. SQLite runs locally, while `EngineConformanceSpec` entries declaring `native_extension_module` fail strictly (`strict=True` `xfail`) when that extension module is absent so a missing-extension `xfail` never masquerades as live server behavior. BigQuery requires `VERTEBRA_CONFORMANCE_BIGQUERY_DSN` and Spanner requires `VERTEBRA_CONFORMANCE_SPANNER_DSN` or `SPANNER_EMULATOR_HOST` (skipping when unconfigured); Redis and Valkey default to `memory://` (`VERTEBRA_CONFORMANCE_DOCKER=1` selects `pytest-databases` fixtures); and DataFusion cases cover registry resolution, Arrow normalization, and missing-dependency refusal. Report passed, skipped, and expected-failure cases separately, require live engine evidence when accepting a native driver, and verify built companion wheels outside the source tree so editable imports never mask distribution isolation issues.[^src-db-conformance]

## Driver regression boundaries

Transaction tests exercise synchronous interruption, actual asyncio task cancellation, and AnyIO cancellation scopes against observable database state. Asyncpg interface tests use a local port with real SQLite autocommit behavior and verify transaction ownership, command counts, and connection release. Cloud DBAPI tests pass actual BigQuery, Spanner, and Spangres drivers through local service ports, including a BigQuery port that rejects permanent DDL inside transactions. SQLite `RETURNING` tests check counts, cross-connection visibility, commit, rollback, and reuse. These local ports verify integration contracts; they do not establish live PostgreSQL or GCP service acceptance. [^src-postgres-transaction-tests][^src-sqlite-returning-tests][^src-native-driver-tests]

## SQLSpec, MCP Conformance, and Native Client Boundary Evidence

Tool-provider regressions exercise adapter-owned operations through `build_tools` and [`ToolRegistry.register_provider`](../../../../src/py/vertebra/agent/mcp/registry.py)[^src-agent-mcp-registry-py], and independently exercise `ToolRegistry` with typed non-vendor providers. [`test_mcp_conformance.py`](../../../../src/py/tests/unit/agent/test_mcp_conformance.py)[^src-tests-unit-agent-mcp-conformance-py] verifies MCP `2026-07-28` Streamable HTTP (`POST /mcp`, `204` on `OPTIONS /mcp`, `405` on `GET /mcp`), `server/discover`, `Mcp-Name` observability header parity, `WebMCPConfig` / `build_webmcp_manifest`, task stores, and provider-registered tools across native, Starlette, FastAPI, Flask, and Django mounts. Assert schema types/defaults, named-parameter registration, conversion before policy, lazy resolution, native error identity, and the absence of vendor imports/exports in generic agent code. Barrier-driven tests pause an approved operation, mutate caller-owned nested values, and prove execution plus stored approval retain the original owned invocation. Typed-value regressions distinguish Python-equal boolean/integer/float replacements inside mappings, containers, and converted records while accepting reordered equivalent mappings.[^src-provider-regressions][^src-provider-schema-regressions][^src-provider-snapshot-regressions]

BigQuery and Spanner sync/async driver and config constructors reject non-null tenant enforcement before base initialization, supplied-core access, registry resolution, default-broker creation or native I/O. Constructor tripwires verify these effects are absent; ordinary unconfigured sessions retain script, Arrow, batch and snapshot behavior. This closes unsupported session creation instead of scattering guards across execution methods.[^src-bigquery-stack-regressions][^src-spanner-stack-regressions]

Stack regressions exercise actual SQLSpec preparation and result objects over real native driver classes, replacing only the external DBAPI/client/service port. They assert exact SQL and parameters at that boundary, ordered rows/counts, rollback/error behavior, and retained statement metadata. PostgreSQL/MySQL ports use a local SQLite database after recording native binds; BigQuery uses a loopback REST service and Spanner uses a deterministic native-client port. These establish adapter integration behavior without claiming live PostgreSQL/MySQL/GCP service acceptance.[^src-stack-regressions][^src-bigquery-stack-regressions][^src-spanner-stack-regressions]

Cover pipeline-capable and sequential fallback, default and explicit statement configuration, leading comments/CTE DML/`RETURNING`, native parameter formats, Arrow options versus named binds, prebound `SQL`, `execute_many` sets and filters, empty results, and result-count mismatches. Preserve installed SQLSpec/MyPyC statement and result contracts rather than replacing the system under test with mock objects.[^src-stack-regressions]

`test_postgres_sync_primitives_bind_with_real_psycopg_converter` sends notification, embedding, similarity-search and tenant-session parameters through installed psycopg `PostgresQuery` and `Transformer` objects. Its `pytest.importorskip` calls mean a normal passing suite can omit converter acceptance. Record an explicit converter-enabled run when accepting native binds, and report skipped converter cases separately. A cached dependency or isolated test environment can supply this optional package; never hardcode that environment's filesystem path into production code or durable instructions.[^src-psycopg-bind-regressions]

Provenance sources: [^src-pyproject-toml], [^src-cargo-toml], [^src-package-json], [^src-ci-yml].

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-tools-scripts-setup-env-sh]: `tools/scripts/setup-env.sh` — Repository tooling module (tools/scripts/setup-env.sh).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-tests-conftest-py]: `src/py/tests/conftest.py` — Python implementation module (src/py/tests/conftest.py).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-ci-yml]: `.github/workflows/ci.yml` — GitHub Actions workflow (ci.yml).
[^src-db-conformance]: `src/py/tests/integration/db/test_driver_conformance.py` — Parameterized database contract and environment readiness gates.
[^src-postgres-transaction-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — Asyncpg transaction and affected-count regressions.
[^src-sqlite-returning-tests]: `src/py/tests/unit/db/test_sqlite_driver.py` — SQLite statement completion and transaction regressions.
[^src-native-driver-tests]: `src/py/tests/unit/db/test_native_db_drivers.py` — Cancellation and cloud DBAPI regression coverage.
[^src-tests-unit-server-stage1-hotpath-py]: `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py` — Server hot-path batch dispatch and free-threaded 3.14t/3.15t unit tests.
[^src-ts-tests-agent-test-ts]: `src/ts/vertebra-client/tests/agent.test.ts` — TypeScript Vitest suite for agent, router, and WebMCP client modules.
[^src-tests-unit-agent-mcp-conformance-py]: `src/py/tests/unit/agent/test_mcp_conformance.py` — MCP 2026-07-28 protocol, ToolRegistry provider, WebMCP, and HTTP handler conformance suite.
[^src-agent-mcp-registry-py]: `src/py/vertebra/agent/mcp/registry.py` — Unified ToolRegistry and MCPRegistry implementation.
[^src-stack-regressions]: `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — Native pipeline and fallback statement/result/bind regressions.
[^src-bigquery-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_bigquery.py` — Actual BigQuery driver stack execution over loopback REST.
[^src-spanner-stack-regressions]: `src/py/tests/unit/extensions/test_sqlspec_spanner.py` — Actual Spanner driver stack execution over external client ports.
[^src-psycopg-bind-regressions]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Optional real psycopg converter acceptance for PostgreSQL primitives.
[^src-provider-regressions]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` — Native provider ownership, lazy resolution, policy and driver error regressions.
[^src-provider-schema-regressions]: `src/py/tests/unit/agent/test_core_and_observability.py` — Neutral provider schema and validation boundaries.
[^src-provider-snapshot-regressions]: `src/py/tests/unit/agent/test_runtime_and_workflows.py` — Approval binding and nested invocation ownership across controlled suspension.

[^src-storage-public-codec-tests]: `src/py/tests/unit/storage/test_storage_store.py` — Native storage codec demand, completion and cancellation tests.

[^src-storage-native-tests]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Native streaming and normalized S3/B2 multipart tests.

[^src-storage-shm-tests]: `src/rs/vertebra-db/src/arrow_shm.rs` — Named Arrow allocation rejection tests.

[^src-storage-cursor-view-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Retained native cursor views after release.
