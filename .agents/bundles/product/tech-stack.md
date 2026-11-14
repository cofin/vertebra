---
type: Guide
title: Vertebra Technology Stack
description: Language versions, Rust workspace crates, Python package configuration, TypeScript package exports, and build and verification toolchains.
status: stable
tags: [tech-stack, python, rust, typescript, maturin]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 334dc98d
  - by: agent:flow-setup
    at: "2026-11-12T23:30:00Z"
    commit: 18baadcd
  - by: agent:flow-sync
    at: "2026-11-09T21:46:00Z"
    commit: 14aa83b8
  - by: agent:flow-executor
    at: "2026-11-09T15:21:00Z"
    commit: af1b3d30
sources:
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-vertebra-core-cargo-toml
    resource: src/rs/vertebra-core/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-core/Cargo.toml)"
  - id: src-vertebra-http-cargo-toml
    resource: src/rs/vertebra-http/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-http/Cargo.toml)"
  - id: src-vertebra-py-cargo-toml
    resource: src/rs/vertebra-py/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-py/Cargo.toml)"
  - id: src-vertebra-bundler-cargo-toml
    resource: src/rs/vertebra-bundler/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-bundler/Cargo.toml)"
  - id: src-vertebra-storage-cargo-toml
    resource: src/rs/vertebra-storage/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-storage/Cargo.toml)"
  - id: src-vertebra-db-cargo-toml
    resource: src/rs/vertebra-db/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-db/Cargo.toml)"
  - id: src-vertebra-mysql-cargo-toml
    resource: src/rs/vertebra-mysql/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-mysql/Cargo.toml)"
  - id: src-vertebra-bigquery-cargo-toml
    resource: src/rs/vertebra-bigquery/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-bigquery/Cargo.toml)"
  - id: src-vertebra-mssql-cargo-toml
    resource: src/rs/vertebra-mssql/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-mssql/Cargo.toml)"
  - id: src-vertebra-oracle-cargo-toml
    resource: src/rs/vertebra-oracle/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-oracle/Cargo.toml)"
  - id: src-vertebra-spanner-cargo-toml
    resource: src/rs/vertebra-spanner/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-spanner/Cargo.toml)"
  - id: src-vertebra-duckdb-cargo-toml
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-duckdb/Cargo.toml)"
  - id: src-vertebra-postgres-cargo-toml
    resource: src/rs/vertebra-postgres/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-postgres/Cargo.toml)"
  - id: src-package-json
    resource: src/ts/vertebra-client/package.json
    title: "TypeScript package manifest (src/ts/vertebra-client/package.json)"
  - id: src-tsup-config-ts
    resource: src/ts/vertebra-client/tsup.config.ts
    title: "TypeScript client module (src/ts/vertebra-client/tsup.config.ts)"
  - id: src-cli-init-py
    resource: src/py/vertebra/cli/__init__.py
    title: "Python implementation module (src/py/vertebra/cli/__init__.py)"
  - id: src-ci-yml
    resource: .github/workflows/ci.yml
    title: "GitHub Actions workflow (ci.yml)"
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
---

# Vertebra Technology Stack

<!-- truth: start -->
## Core Stack Invariants

- **Python (`>=3.12`, CI & wheel matrix `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`)**: published on PyPI as `vertebra-core` (`0.3.0`, `import vertebra`), managed by `uv` (`pyproject.toml`, `uv.lock`), built with `maturin` (`>=1.5,<2`) and `pypa/cibuildwheel@v4.3.5` (`cp312-*`..`cp315t-*` across `vertebra-core` and companion wheels `vertebra-bundler`, `vertebra-storage`, `vertebra-mysql`, `vertebra-bigquery`, `vertebra-mssql`, `vertebra-spanner`, `vertebra-duckdb`, `vertebra-oracle`, and `vertebra-postgres`). Runtime dependencies are `anyio>=4.0`, `msgspec>=0.18`, `rich>=13.0`, and `rich-click>=1.7`.
- **Rust (`edition = "2024"`, workspace version `0.3.0`, stable toolchain, `>=1.91.0` required by `google-cloud-*` `rust-version`)**: CI installs `dtolnay/rust-toolchain@stable` across 13 workspace crates under `src/rs/`:
  - **Runtime & HTTP (`vertebra-core`, `vertebra-http`, `vertebra-py`)**: `vertebra-core` (`SpscRing`, `MpmcRing`, `TieredRingSet`, `BatchBridge`, `ShmCapsule`, `jsonwebtoken 11.1`, `lz4_flex`/`zstd`, `gcp-auth`); `vertebra-http` (`vertebra-server` binary over `hyper 1`, `tokio 1.54`, `matchit 0.9`, `tokio-tungstenite 0.30`, optional `tls` [`rustls 0.23`], `http3` [`quinn`/`h3`], `mimalloc`, and `python`); `vertebra-py` (`PyO3 0.30.1` `vertebra._vertebra` with `gil_used = false`).
  - **Frontend & Storage (`vertebra-bundler`, `vertebra-storage`)**: `vertebra-bundler` (`vertebra._bundler`, `oxc_* 0.155`, `rquickjs 0.14`, `rolldown-notify 11.0`); `vertebra-storage` (`vertebra._storage`, `object_store 0.13.2` with S3/GCS/Azure/OCI/R2/HTTP, SigV4/GCS V4/SAS/PAR signing).
  - **Shared Database Core (`vertebra-db`)**: `rlib` `vertebra_db` (`TransportStream`, `rustls 0.23`, Cloud SQL/AlloyDB dialers, `arrow_shm`, wire codecs; optional `gcp` [`google-cloud-{auth,gax,rpc,wkt}` without `spanner`/`bigquery`/`tonic`], `pyo3` [`py_async` + `ShmCapsule`], and `duckdb` marker).
  - **Standalone Database Extensions**: `vertebra-mysql` (`_mysql`, `mysql_async 0.37`, `arrow =60.1.0`), `vertebra-bigquery` (`_bigquery`, `google-cloud-bigquery =0.19.0`), `vertebra-mssql` (`_mssql`, `tiberius =0.13.0` + `rustls`, `arrow =60.1.0`), `vertebra-oracle` (`_oracle`, pure-Rust `oracledb =26.0.0-beta.5`, crate-local `arrow 59`), `vertebra-spanner` (`_spanner`, `google-cloud-spanner =0.34.8-preview`), `vertebra-duckdb` (`_duckdb`, bundled `duckdb =1.20000.1`, `arrow =60.1.0`), and `vertebra-postgres` (`_postgres`, `postgres-protocol =0.6.12`, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB mTLS+IAM, single-`Sync` pipelines, `PGCOPY`, `VTBARROW`, `LISTEN/NOTIFY`, `pgoutput` v1 `AsyncPostgresReplication`).
- **TypeScript (`@vertebra/client` `0.3.0`)**: managed by `bun`, bundled with `tsup ^8.5.1`, type-checked with `typescript ^5.9.3`, linted with `@biomejs/biome ^2.6.6`, tested with `vitest ^5.1.0`, and packaged directly inside the `vertebra-core` wheel under `src/py/vertebra/server/_js/bundle/`. Optional peer dependencies are `react ^19.0.0`, `vue ^3.5.0`, `@angular/core ^22.0.0`, and `vite ^8.0.0`; `svelte ^5.57.3` is a dev dependency only, and the HTMX adapter has no dependency.
- **Verification & Documentation Toolchain**: `ruff` (lint + format, `line-length = 120`, `target-version = "py312"`, rules `E,F,I,UP,B,SIM`, `src = ["src/py", "docs/examples", "tools"]`, `Makefile` targets checking `src/py`, `tools`, and `docs/examples`), `mypy` with `django-stubs>=6.1.2` for the real Django `Storage` base (`strict = true`, `files = ["src/py/vertebra", "tools/scripts", "tools/pgo", "tools/docs", "tools/benchmarks", "docs/examples"]`) + `mypy.stubtest` (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`, `vertebra._mysql`, `vertebra._bigquery`, `vertebra._mssql`, `vertebra._spanner`, `vertebra._duckdb`, `vertebra._oracle`, `vertebra._postgres`), `pytest` + `pytest-cov`, `zensical` (`>=0.0.71`) + `mkdocstrings-python` (`>=2.0.3`) + `pymdown-extensions` (`>=10.16`) (`tools/docs/`, `site_url = "https://cofin.github.io/vertebra/"`, `repo_url = "https://github.com/cofin/vertebra"`, `repo_name = "cofin/vertebra"`), `cargo fmt`, `cargo clippy --workspace --all-targets --features tls,compression,gcp -- -D warnings`, `cargo test --workspace --features tls,compression,gcp`, `bunx biome check`, `criterion 0.8` microbenchmarks, and the `tools.benchmarks` harness.
<!-- truth: end -->

## Rust Workspace Crates (`src/rs/Cargo.toml`)

| Crate | Path | Targets | Features |
| --- | --- | --- | --- |
| `vertebra-core` | [`src/rs/vertebra-core`](../../../src/rs/vertebra-core/Cargo.toml)[^src-vertebra-core-cargo-toml] | `lib` + Criterion benches `spsc`, `guards`, `ring_throughput`, `codec`, `shm_transport` | `compression` + `gcp-auth` (default: `lz4_flex`, `zstd`, `reqwest`, `url`, `chrono`), `macos-ulock` |
| `vertebra-http` | [`src/rs/vertebra-http`](../../../src/rs/vertebra-http/Cargo.toml)[^src-vertebra-http-cargo-toml] | `lib` + `vertebra-server` binary + `server_throughput` bench | `python` + `mimalloc` (default; `python` embeds `vertebra-py`), `tls` (`rustls`, `tokio-rustls`, `rustls-pemfile`), `http3` (`tls` + `quinn`, `h3`, `h3-quinn`), `compression` (enables `vertebra-core/compression`) |
| `vertebra-py` | [`src/rs/vertebra-py`](../../../src/rs/vertebra-py/Cargo.toml)[^src-vertebra-py-cargo-toml] | `cdylib` + `rlib` (`_vertebra`) | `extension-module`, `gcp` (`vertebra-db/gcp`) |
| `vertebra-bundler` | [`src/rs/vertebra-bundler`](../../../src/rs/vertebra-bundler/Cargo.toml)[^src-vertebra-bundler-cargo-toml] | `cdylib` + `rlib` (`_bundler`, `[bundler]` extra) + `ssr_render` bench | `extension-module` |
| `vertebra-storage` | [`src/rs/vertebra-storage`](../../../src/rs/vertebra-storage/Cargo.toml)[^src-vertebra-storage-cargo-toml] | `cdylib` + `rlib` (`_storage`, `[storage]` extra) | `extension-module` |
| `vertebra-db` | [`src/rs/vertebra-db`](../../../src/rs/vertebra-db/Cargo.toml)[^src-vertebra-db-cargo-toml] | `rlib` (`vertebra_db`) | `gcp` (`google-cloud-{auth,gax,rpc,wkt}`), `pyo3` (`py_async`), `duckdb` (marker) |
| `vertebra-mysql` | [`src/rs/vertebra-mysql`](../../../src/rs/vertebra-mysql/Cargo.toml)[^src-vertebra-mysql-cargo-toml] | `cdylib` + `rlib` (`_mysql`, `[mysql]` extra) | `extension-module` |
| `vertebra-bigquery` | [`src/rs/vertebra-bigquery`](../../../src/rs/vertebra-bigquery/Cargo.toml)[^src-vertebra-bigquery-cargo-toml] | `cdylib` + `rlib` (`_bigquery`, `[bigquery]` extra) | `extension-module` |
| `vertebra-mssql` | [`src/rs/vertebra-mssql`](../../../src/rs/vertebra-mssql/Cargo.toml)[^src-vertebra-mssql-cargo-toml] | `cdylib` + `rlib` (`_mssql`, `[mssql]` extra) | `extension-module` |
| `vertebra-oracle` | [`src/rs/vertebra-oracle`](../../../src/rs/vertebra-oracle/Cargo.toml)[^src-vertebra-oracle-cargo-toml] | `cdylib` + `rlib` (`_oracle`, `[oracle]` extra) | `extension-module` |
| `vertebra-spanner` | [`src/rs/vertebra-spanner`](../../../src/rs/vertebra-spanner/Cargo.toml)[^src-vertebra-spanner-cargo-toml] | `cdylib` + `rlib` (`_spanner`, `[spanner]` extra) | `extension-module` |
| `vertebra-duckdb` | [`src/rs/vertebra-duckdb`](../../../src/rs/vertebra-duckdb/Cargo.toml)[^src-vertebra-duckdb-cargo-toml] | `cdylib` + `rlib` (`_duckdb`, `[duckdb]` extra) | `extension-module` |
| `vertebra-postgres` | [`src/rs/vertebra-postgres`](../../../src/rs/vertebra-postgres/Cargo.toml)[^src-vertebra-postgres-cargo-toml] | `cdylib` + `rlib` (`_postgres`, `[postgres]` extra) | `extension-module` |

`rcgen 0.14` and `reqwest 0.13` are dev-dependencies of `vertebra-http` (test certificates and HTTP clients). The release profile sets `lto = true`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.

## Python Package Configuration (`pyproject.toml`)

- **Distribution & Import Namespace**: `[project] name = "vertebra-core"` publishes the PyPI distribution as `vertebra-core` (`vertebra_core-*.whl` and `vertebra_core-*.tar.gz`), while `[tool.maturin]` sets `python-source = "src/py"`, `python-packages = ["vertebra"]`, `module-name = "vertebra._vertebra"`, and `include` globs (`src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `src/rs/vertebra-*/**/*` for `sdist`; `src/py/vertebra/server/_js/bundle/**/*` and `src/py/vertebra/framework/assets/*` for both `sdist` and `wheel`) so installed packages expose the top-level `vertebra` import namespace with bundled client and framework assets, and source distributions contain the full Rust workspace.
- **Build Backend**: `maturin` builds `src/rs/vertebra-py/Cargo.toml` into `vertebra._vertebra` (`python-source = "src/py"`, feature `extension-module`). `make dev` and `make build` run companion `maturin develop` calls that build `src/rs/vertebra-bundler/Cargo.toml` into `vertebra._bundler`, `src/rs/vertebra-storage/Cargo.toml` into `vertebra._storage`, `src/rs/vertebra-bigquery/Cargo.toml` into `vertebra._bigquery`, `src/rs/vertebra-mssql/Cargo.toml` into `vertebra._mssql`, `src/rs/vertebra-mysql/Cargo.toml` into `vertebra._mysql`, `src/rs/vertebra-oracle/Cargo.toml` into `vertebra._oracle`, `src/rs/vertebra-postgres/Cargo.toml` into `vertebra._postgres`, `src/rs/vertebra-spanner/Cargo.toml` into `vertebra._spanner`, and `src/rs/vertebra-duckdb/Cargo.toml` into `vertebra._duckdb`; `make build-pgo` runs two-phase Rust PGO instrumentation and profile-use compilation via `tools/pgo/rust_pgo.py`.
- **CLI Entrypoint**: `vtb = "vertebra.cli:cli"` registers `vtb run [--topology auto|embedded|shm] [--interface auto|asgi|rsgi|wsgi] [--loop auto|uvloop|winloop|rloop|zuvloop|asyncio] [--jit/--no-jit]`, `vtb dev`, `vtb init`, `vtb js {add,remove,list,install,build,generate-types,exec}`, `vtb build [--target ...] [--profile debug|release] [--standalone|--onefile] [--server-binary ...] [--entrypoint ...] [--python-version 3.12|3.13|3.14|3.14t|3.15|3.15t] [--output ...] [--project-dir ...]`, `vtb ai models {list,pull,stage,warm}`, `vtb ai mcp {serve,bridge}`, `vtb workers run`, and entry-point plugin commands via `VertebraExtensionGroup` ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py]).
- **SQLAlchemy Dialect Entry Points**: `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, and `spangres` each register `.vertebra`, `.vtb`, `.vertebra_async`, and `.vtb_async` dialects (36 entry points total) from `vertebra.extensions.sqlalchemy.dialects`.
- **Optional Extras (`[project.optional-dependencies]`)**: `sqlspec` (`sqlspec>=0.69.0`), `alchemy` (`advanced-alchemy>=1.12.0`, `sqlalchemy>=2.0.36`, `alembic>=1.14.0`), `arrow` (`pyarrow>=18.0.0`), `pydantic` (`pydantic>=2.9.0`), `attrs` (`attrs>=24.0.0`), `polars` (`polars>=1.0.0`), `litestar` (`litestar>=2.15.0`), `starlette` (`starlette>=0.40.0`), `fastapi` (`fastapi>=0.115.0`, `pydantic>=2.9.0`), `flask` (`flask>=3.0.0`), `django` (`django>=5.1.0`), `queues` (`litestar-queues>=0.12.0`), `security` (`litestar-security>=0.6.0`), `autowire` (`litestar-autowire>=0.2.0`), `email` (`litestar-email>=0.4.0`), `mcp` (`litestar-mcp>=0.20.0`), `vite` (`litestar-vite>=0.32.0`), `asyncapi` (`litestar-asyncapi>=0.2.0`), `htmx` (`litestar-htmx>=0.5.0`), `embeddings` (`fastembed>=0.6.0`), `iap` (`pyjwt[crypto]>=2.9.0`), `bigquery` (`vertebra-bigquery==0.3.0`), `bundler` (`vertebra-bundler==0.3.0`), `duckdb` (`vertebra-duckdb==0.3.0`), `mssql` (`vertebra-mssql==0.3.0`), `mysql` (`vertebra-mysql==0.3.0`), `oracle` (`vertebra-oracle==0.3.0`), `postgres` (`vertebra-postgres==0.3.0`), `spanner` (`vertebra-spanner==0.3.0`), `storage` (`vertebra-storage==0.3.0`), `uvloop` (`uvloop>=0.23.0; sys_platform != 'win32'`), `winloop` (`winloop>=0.8.1; sys_platform == 'win32'`), `rloop` (`rloop>=0.5.0; sys_platform != 'win32'`), and `zuvloop` (`zuvloop>=0.0.17; python_version >= '3.14'`); `make install` syncs all extras plus the `doc` and `duckdb` groups (`uv sync --all-extras --dev --group doc --group duckdb`).
- **Dependency Groups**:
  - `dev`: includes `test` (`pytest`, `pytest-cov>=5.0`, `pytest-databases>=0.20.0b1`, `pytest-xdist>=3.6.1`, `anyio`, `uvloop>=0.23.0; sys_platform != 'win32'`, `winloop>=0.8.1; sys_platform == 'win32'`, plus ecosystem framework/ORM test dependencies), `lint` (`ruff`, `mypy`, `django-stubs>=6.1.2` for Django base-class and `File[bytes]` typing), and `build` (`bump-my-version>=0.31.1`, `maturin>=1.5,<2`).
  - `doc`: separate group (`anyio>=4.0`, `mkdocstrings-python>=2.0.3`, `msgspec>=0.18`, `pymdown-extensions>=10.16`, `rich>=13.0`, `rich-click>=1.7`, `ruff>=0.4`, `zensical>=0.0.71`), installed on demand via `--group doc`.
  - `duckdb`: separate GIL-only group (`duckdb>=1.0` for `tools.benchmarks` history storage; production `SyncDuckDBDriver`/`AsyncDuckDBDriver` load `vertebra._duckdb`), synced by `make install` and added by `make py-test`, `make coverage`, and `make upgrade` through `PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)` only when the interpreter probe reports `Py_GIL_DISABLED=0` (GIL build), so a free-threaded build or a failed probe adds no group.
  - `bench`: separate group (`{ include-group = "duckdb" }`, `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.23.0; sys_platform != 'win32'`, `granian>=1.6`, `spikard>=0.12`), used only through `uv run --group bench` in `make bench*`.
  - `postgres-bench`: separate comparator group (`asyncpg>=0.30; python_version < '3.14'`, `psycopg[binary]>=3.2,<3.4; python_version < '3.14'`), used by `make postgres-bench` (`tools/benchmarks/postgres.py`), while `make postgres-bench-check` runs Cargo bench tests, Ruff/Mypy, and unit tests offline.
- **Mypy Overrides**: `ignore_missing_imports` (`52` patterns in `pyproject.toml`) for optional third-party and runtime modules (`pydantic.*`, `attrs`, `numpy`, `numpy.*`, `pyarrow`, `pyarrow.*`, `polars`, `polars.*`, `uvloop`, `winloop`, `rloop`, `zuvloop`, `starlette`, `starlette.*`, `fastapi`, `fastapi.*`, `flask`, `flask.*`, `litestar`, `litestar.*`, `litestar_queues*`, `litestar_security*`, `litestar_autowire*`, `litestar_email*`, `litestar_mcp*`, `litestar_vite*`, `litestar_asyncapi*`, `litestar_htmx*`, `advanced_alchemy*`, `sqlalchemy*`, `alembic*`, `sqlspec*`, `django*`, `pytest_databases*`, `concurrent.interpreters`, `duckdb`, `spikard`, `spikard.*`).

## TypeScript Package Configuration (`src/ts/vertebra-client/`)

Subpath exports (`package.json` and [`tsup.config.ts`](../../../src/ts/vertebra-client/tsup.config.ts)[^src-tsup-config-ts]):

| Subpath | Entry | Contents |
| --- | --- | --- |
| `@vertebra/client` | `src/index.ts` | `HttpClient`, `SseClient`, `WebSocketClient`, `SessionStore`, `HotReloadClient`, `LiveStateClient`, `ArrowStreamClient`, `WASMClient`, `ApexRegion`, `TraceContext`, `routeUrl`, `wsUrl`, `registerWebMCPTools`, `McpClient`, `A2aClient`, `computeBackoffDelay`, error classes |
| `/vite` | `src/vite/index.ts` | Vite plugin and dev proxy helpers |
| `/react` | `src/react/index.tsx` | React `ClientProvider`, hooks, and `QueryCache` (`react/cache.ts`) |
| `/vue` | `src/vue/index.ts` | Vue composables (`provideClient`, `useQuery`, `useMutation`) |
| `/svelte` | `src/svelte/index.ts` | Svelte helpers (`setClient`, `createQuery`, `createMutation`) |
| `/angular` | `src/angular/index.ts` | Angular `ClientService`, `injectQuery`, `injectMutation` |
| `/htmx` | `src/htmx/index.ts` | `initHtmx` extension helper |
| `/store` | `src/store/index.ts` | Standalone `SessionStore` |
| `/hmr` | `src/hmr/index.ts` | `HotReloadClient`, `initHotReload` |
| `/live` | `src/live/index.ts` | `LiveStateClient`, `HybridLogicalClock`, `OfflineDeltaQueue` |
| `/apex` | `src/apex/index.ts` | `ApexRegion`, `ApexOfflineOutbox` |
| `/telemetry` | `src/telemetry.ts` | `TraceContext` |
| `/wasm` | `src/wasm.ts` | `WASMClient` |
| `/arrow` | `src/arrow.ts` | `ArrowStreamClient` |
| `/router` | `src/router.ts` | `routeUrl`, `wsUrl`, `interpolateRoutePath`, `RouteUrlOptions` |
| `/webmcp` | `src/webmcp.ts` | `registerWebMCPTools`, `WebMCPManifest`, `WebMCPToolDescriptor` |
| `/agent` | `src/agent/index.ts` | `McpClient`, `A2aClient`, `AgentStreamClient`, `WorkflowLiveSubscriber` |

The `onSuccess` hook in [`tsup.config.ts`](../../../src/ts/vertebra-client/tsup.config.ts) copies the full `dist/` directory into `src/py/vertebra/server/_js/bundle/` for `DevMiddleware` and `ImportMapGenerator.sync_wheel_client`.

## Related Context

See [Product Definition](product.md), [Product Guidelines](product-guidelines.md), [Project Workflow](../knowledge/workflow.md), [Polyglot Test Matrix](../knowledge/testing/polyglot-test-matrix.md), and [Benchmark Suite & Free-Threaded Python 3.14t/3.15t](../knowledge/testing/benchmarks-and-free-threaded.md).

Provenance sources: [^src-pyproject-toml], [^src-cargo-toml], [^src-package-json], [^src-ci-yml], [^src-makefile].

[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-vertebra-core-cargo-toml]: `src/rs/vertebra-core/Cargo.toml` — Rust crate manifest (src/rs/vertebra-core/Cargo.toml).
[^src-vertebra-http-cargo-toml]: `src/rs/vertebra-http/Cargo.toml` — Rust crate manifest (src/rs/vertebra-http/Cargo.toml).
[^src-vertebra-py-cargo-toml]: `src/rs/vertebra-py/Cargo.toml` — Rust crate manifest (src/rs/vertebra-py/Cargo.toml).
[^src-vertebra-bundler-cargo-toml]: `src/rs/vertebra-bundler/Cargo.toml` — Rust crate manifest (src/rs/vertebra-bundler/Cargo.toml).
[^src-vertebra-storage-cargo-toml]: `src/rs/vertebra-storage/Cargo.toml` — Rust crate manifest (src/rs/vertebra-storage/Cargo.toml).
[^src-vertebra-db-cargo-toml]: `src/rs/vertebra-db/Cargo.toml` — Rust crate manifest (src/rs/vertebra-db/Cargo.toml).
[^src-vertebra-mysql-cargo-toml]: `src/rs/vertebra-mysql/Cargo.toml` — Rust crate manifest (src/rs/vertebra-mysql/Cargo.toml).
[^src-vertebra-bigquery-cargo-toml]: `src/rs/vertebra-bigquery/Cargo.toml` — Rust crate manifest (src/rs/vertebra-bigquery/Cargo.toml).
[^src-vertebra-mssql-cargo-toml]: `src/rs/vertebra-mssql/Cargo.toml` — Rust crate manifest (src/rs/vertebra-mssql/Cargo.toml).
[^src-vertebra-oracle-cargo-toml]: `src/rs/vertebra-oracle/Cargo.toml` — Rust crate manifest (src/rs/vertebra-oracle/Cargo.toml).
[^src-vertebra-spanner-cargo-toml]: `src/rs/vertebra-spanner/Cargo.toml` — Rust crate manifest (src/rs/vertebra-spanner/Cargo.toml).
[^src-vertebra-duckdb-cargo-toml]: `src/rs/vertebra-duckdb/Cargo.toml` — Rust crate manifest (src/rs/vertebra-duckdb/Cargo.toml).
[^src-vertebra-postgres-cargo-toml]: `src/rs/vertebra-postgres/Cargo.toml` — Rust crate manifest (src/rs/vertebra-postgres/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-tsup-config-ts]: `src/ts/vertebra-client/tsup.config.ts` — TypeScript client module (src/ts/vertebra-client/tsup.config.ts).
[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-ci-yml]: `.github/workflows/ci.yml` — GitHub Actions workflow (ci.yml).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
