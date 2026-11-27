---
type: Guide
title: Vertebra Technology Stack
description: Language versions, Rust workspace crates, Python package configuration, TypeScript package exports, and build and verification toolchains.
status: stable
tags:
  - tech-stack
  - python
  - rust
  - typescript
  - maturin
generated:
  by: flow/0.30.1
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-27T22:10:00Z"
    commit: f3b624a2
  - by: agent:flow-archive
    at: "2026-11-16T15:16:00Z"
    commit: 11020d84
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
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
  - id: src-vertebra-sqlite-cargo-toml
    resource: src/rs/vertebra-sqlite/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-sqlite/Cargo.toml)"
  - id: src-vertebra-redis-cargo-toml
    resource: src/rs/vertebra-redis/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-redis/Cargo.toml)"
  - id: src-vertebra-valkey-cargo-toml
    resource: src/rs/vertebra-valkey/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-valkey/Cargo.toml)"
  - id: src-vertebra-resp-cargo-toml
    resource: src/rs/vertebra-resp/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-resp/Cargo.toml)"
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

- **Python (`>=3.12`; CI & wheel matrix `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`)**: Published on PyPI as `vertebra-core` (`0.4.0`, `import vertebra`), managed with `uv`, and built via `maturin` (`>=1.5,<2`) and `cibuildwheel` across `vertebra-core` plus 12 companion wheels (`bundler`, `storage`, `mysql`, `bigquery`, `mssql`, `spanner`, `duckdb`, `oracle`, `postgres`, `sqlite`, `redis`, `valkey`). Runtime dependencies: `anyio>=4.0`, `msgspec>=0.18`, `rich>=13.0`, `rich-click>=1.7`.
- **Rust (`edition = "2024"`, version `0.4.0`, stable toolchain `>=1.91.0`)**: 17 workspace crates under `src/rs/`:
  - **Runtime & HTTP**: `vertebra-core` (IPC rings, `BatchBridge`, SHM broker/cache, JWT, compression, `gcp-auth`), `vertebra-http` (`vertebra-server` on `hyper 1`, `tokio 1.54`, `matchit 0.9`, `tokio-tungstenite 0.30`), and `vertebra-py` (`PyO3 0.30.1` `vertebra._vertebra`, `gil_used = false`).
  - **Frontend, Storage & DB Core**: `vertebra-bundler` (`oxc_* 0.155`, `rquickjs 0.14`, `rolldown-notify 11.0`), `vertebra-storage` (`object_store 0.13.2` + native OCI and signing), `vertebra-db` (shared streams, cloud dialers, Arrow SHM, wire codecs), and internal `vertebra-resp` `rlib` (linked by `_redis` and `_valkey`, no separate wheel).
  - **Standalone Driver Extensions**: `vertebra-mysql` (`mysql_async =0.37.1`), `vertebra-bigquery` (`google-cloud-bigquery =0.19.0`), `vertebra-mssql` (`tiberius =0.13.0`), `vertebra-oracle` (`oracledb =26.0.0-beta.6`), `vertebra-spanner` (`google-cloud-spanner =0.34.8-preview`), `vertebra-duckdb` (bundled `duckdb =1.20000.2`), `vertebra-postgres` (`postgres-protocol =0.6.12`), `vertebra-sqlite` (bundled `rusqlite =0.41.1`), `vertebra-redis`, and `vertebra-valkey`.
- **TypeScript (`@vertebra/client` `0.4.0`)**: Managed by `bun`, bundled with `tsup ^8.5.1`, type-checked with `typescript ^5.9.3`, linted with Biome `^2.6.9`, tested with Vitest `^5.1.2`, and synced into `src/py/vertebra/server/_js/bundle/`. Optional peers: `react ^19`, `vue ^3.5`, `@angular/core ^22`, `vite ^8`.
- **Verification & Docs Toolchain**: `ruff` (`line-length = 120`, `py312`, `E,F,I,UP,B,SIM`), `mypy` (`strict = true`) + 13-extension `mypy.stubtest`, `pyright`, `pytest` + `pytest-cov`, `zensical` + `mkdocstrings-python`, `cargo fmt`, `cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings`, `cargo test`, `bunx biome check`, Criterion `0.8`, and `tools.benchmarks`.
<!-- truth: end -->

## Rust Workspace Crates (`src/rs/Cargo.toml`)

| Crate | Path | Targets | Features |
| --- | --- | --- | --- |
| `vertebra-core` | [`src/rs/vertebra-core`](../../../src/rs/vertebra-core/Cargo.toml)[^src-vertebra-core-cargo-toml] | `lib` + 5 Criterion benches (`spsc`, `guards`, `ring_throughput`, `codec`, `shm_transport`) | `compression` + `gcp-auth` (default), `macos-ulock` |
| `vertebra-http` | [`src/rs/vertebra-http`](../../../src/rs/vertebra-http/Cargo.toml)[^src-vertebra-http-cargo-toml] | `lib` + `vertebra-server` binary + `server_throughput` bench | `python` + `mimalloc` (default), `tls`, `http3`, `compression` |
| `vertebra-py` | [`src/rs/vertebra-py`](../../../src/rs/vertebra-py/Cargo.toml)[^src-vertebra-py-cargo-toml] | `cdylib` + `rlib` (`_vertebra`) | `extension-module`, `gcp` (`vertebra-db/gcp`) |
| `vertebra-bundler` | [`src/rs/vertebra-bundler`](../../../src/rs/vertebra-bundler/Cargo.toml)[^src-vertebra-bundler-cargo-toml] | `cdylib` + `rlib` (`_bundler`, `[bundler]`) + `ssr_render` bench | `extension-module` |
| `vertebra-storage` | [`src/rs/vertebra-storage`](../../../src/rs/vertebra-storage/Cargo.toml)[^src-vertebra-storage-cargo-toml] | `cdylib` + `rlib` (`_storage`, `[storage]`) | `extension-module` |
| `vertebra-db` | [`src/rs/vertebra-db`](../../../src/rs/vertebra-db/Cargo.toml)[^src-vertebra-db-cargo-toml] | `rlib` (`vertebra_db`) | `default = []`; opt-in `arrow-shm`, `broker`, `codecs-*`, `dialer`, `gcp`, `pyo3`, `rustls-ring`, `transport` |
| `vertebra-mysql` | [`src/rs/vertebra-mysql`](../../../src/rs/vertebra-mysql/Cargo.toml)[^src-vertebra-mysql-cargo-toml] | `cdylib` + `rlib` (`_mysql`, `[mysql]`) | `extension-module` |
| `vertebra-bigquery` | [`src/rs/vertebra-bigquery`](../../../src/rs/vertebra-bigquery/Cargo.toml)[^src-vertebra-bigquery-cargo-toml] | `cdylib` + `rlib` (`_bigquery`, `[bigquery]`) | `extension-module` |
| `vertebra-mssql` | [`src/rs/vertebra-mssql`](../../../src/rs/vertebra-mssql/Cargo.toml)[^src-vertebra-mssql-cargo-toml] | `cdylib` + `rlib` (`_mssql`, `[mssql]`) | `extension-module` |
| `vertebra-oracle` | [`src/rs/vertebra-oracle`](../../../src/rs/vertebra-oracle/Cargo.toml)[^src-vertebra-oracle-cargo-toml] | `cdylib` + `rlib` (`_oracle`, `[oracle]`) | `extension-module` |
| `vertebra-spanner` | [`src/rs/vertebra-spanner`](../../../src/rs/vertebra-spanner/Cargo.toml)[^src-vertebra-spanner-cargo-toml] | `cdylib` + `rlib` (`_spanner`, `[spanner]`) | `extension-module` |
| `vertebra-duckdb` | [`src/rs/vertebra-duckdb`](../../../src/rs/vertebra-duckdb/Cargo.toml)[^src-vertebra-duckdb-cargo-toml] | `cdylib` + `rlib` (`_duckdb`, `[duckdb]`) | `extension-module` |
| `vertebra-postgres` | [`src/rs/vertebra-postgres`](../../../src/rs/vertebra-postgres/Cargo.toml)[^src-vertebra-postgres-cargo-toml] | `cdylib` + `rlib` (`_postgres`, `[postgres]`) | `extension-module` |
| `vertebra-sqlite` | [`src/rs/vertebra-sqlite`](../../../src/rs/vertebra-sqlite/Cargo.toml)[^src-vertebra-sqlite-cargo-toml] | `cdylib` + `rlib` (`_sqlite`, `[sqlite]`) | `extension-module` |
| `vertebra-redis` | [`src/rs/vertebra-redis`](../../../src/rs/vertebra-redis/Cargo.toml)[^src-vertebra-redis-cargo-toml] | `cdylib` + `rlib` (`_redis`, `[redis]`) | `extension-module` |
| `vertebra-valkey` | [`src/rs/vertebra-valkey`](../../../src/rs/vertebra-valkey/Cargo.toml)[^src-vertebra-valkey-cargo-toml] | `cdylib` + `rlib` (`_valkey`, `[valkey]`) | `extension-module` |
| `vertebra-resp` | [`src/rs/vertebra-resp`](../../../src/rs/vertebra-resp/Cargo.toml)[^src-vertebra-resp-cargo-toml] | `rlib` (`vertebra_resp`, linked into `_redis` and `_valkey`) | `pyo3` |

## Python Package Configuration (`pyproject.toml`)

- **Distribution & Build Backend**: `[project] name = "vertebra-core"` publishes `vertebra_core-*.whl` while `[tool.maturin]` sets `python-source = "src/py"`, `python-packages = ["vertebra"]`, and `module-name = "vertebra._vertebra"`. `make dev` and `make build` install all 13 extensions via `maturin develop`; `make build-pgo` runs two-phase Rust PGO via `tools/pgo/rust_pgo.py`.
- **CLI & SQLAlchemy Entrypoints**: `vtb = "vertebra.cli:cli"` ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py]). Nine database engines (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`) each register `.vertebra`, `.vtb`, `.vertebra_async`, and `.vtb_async` dialects (36 entry points) in `vertebra.extensions.sqlalchemy.dialects`.
- **Optional Extras**: Cover `sqlspec` (`>=0.74.1`), `alchemy` (`advanced-alchemy>=1.13.0b3`, `sqlalchemy>=2.1.0`, `alembic>=1.18.0`), data codecs (`arrow`, `pydantic`, `attrs`, `polars`), host frameworks (`litestar`, `starlette`, `fastapi`, `flask`, `django>=6.0`), Litestar/agent add-ons (`queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `embeddings`, `iap`), event loops (`uvloop`, `winloop`, `rloop`, `zuvloop`), and all 12 companion `vertebra-*==0.4.0` wheels.
- **Dependency Groups**: `dev` (`test` + `lint` + `build`), `doc`, `bench` (`duckdb>=1.0; python_version < '3.14'`, `uvicorn`, `granian`, `spikard`), `duckdb` (`python_version < '3.14'`), and `postgres-bench` (`asyncpg`/`psycopg` on `<3.14`). `make install` runs `uv sync --all-extras --dev --group doc`.
- **Mypy Rules**: `[tool.mypy]` sets `strict = true`, `python_version = "3.12"`, covering `src/py/vertebra`, `src/py/tests/{unit,integration}`, `tools/{scripts,pgo,docs,benchmarks}`, and `docs/examples`. `ignore_missing_imports = true` is limited to untyped C or uninstalled optional modules, and `disallow_subclassing_any = false` is scoped to third-party SQLAlchemy/cache/Django adapter bases.

## TypeScript Package Configuration (`src/ts/vertebra-client/`)

Subpath exports declared in `package.json` and [`tsup.config.ts`](../../../src/ts/vertebra-client/tsup.config.ts)[^src-tsup-config-ts]: `@vertebra/client` (`src/index.ts`), `/vite`, `/react`, `/vue`, `/svelte`, `/angular`, `/htmx`, `/store`, `/hmr`, `/live`, `/apex`, `/telemetry`, `/wasm`, `/arrow`, `/router`, `/webmcp`, and `/agent`.

After `tsup` emits `.js` and `.d.ts` files, `bun run build` invokes `scripts/sync-bundle.mjs` to copy `dist/` into `src/py/vertebra/server/_js/bundle/` for `DevMiddleware` and `ImportMapGenerator.sync_wheel_client`.

## Related Context

See [Product Definition](product.md), [Product Guidelines](product-guidelines.md), [Project Workflow](../knowledge/workflow.md), [Polyglot Test Matrix](../knowledge/testing/polyglot-test-matrix.md), and [Benchmark Suite & Free-Threaded Python](../knowledge/testing/benchmarks-and-free-threaded.md). Provenance: [^src-pyproject-toml], [^src-cargo-toml], [^src-package-json], [^src-ci-yml], [^src-makefile].

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
[^src-vertebra-sqlite-cargo-toml]: `src/rs/vertebra-sqlite/Cargo.toml` — Rust crate manifest (src/rs/vertebra-sqlite/Cargo.toml).
[^src-vertebra-redis-cargo-toml]: `src/rs/vertebra-redis/Cargo.toml` — Rust crate manifest (src/rs/vertebra-redis/Cargo.toml).
[^src-vertebra-valkey-cargo-toml]: `src/rs/vertebra-valkey/Cargo.toml` — Rust crate manifest (src/rs/vertebra-valkey/Cargo.toml).
[^src-vertebra-resp-cargo-toml]: `src/rs/vertebra-resp/Cargo.toml` — Rust crate manifest (src/rs/vertebra-resp/Cargo.toml).
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-tsup-config-ts]: `src/ts/vertebra-client/tsup.config.ts` — TypeScript client module (src/ts/vertebra-client/tsup.config.ts).
[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-ci-yml]: `.github/workflows/ci.yml` — GitHub Actions workflow (ci.yml).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
