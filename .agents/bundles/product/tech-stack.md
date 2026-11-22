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

- **Python (`>=3.12`, CI & wheel matrix `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`)**: published on PyPI as `vertebra-core` (`0.3.0`, `import vertebra`), managed by `uv` (`pyproject.toml`, `uv.lock`), built with `maturin` (`>=1.5,<2`) and `pypa/cibuildwheel@v4.3.5` (`cp312-*`..`cp315t-*` across `vertebra-core` and 12 companion wheels: `vertebra-{bundler,storage,mysql,bigquery,mssql,spanner,duckdb,oracle,postgres,sqlite,redis,valkey}`). Runtime dependencies are `anyio>=4.0`, `msgspec>=0.18`, `rich>=13.0`, and `rich-click>=1.7`.
- **Rust (`edition = "2024"`, workspace version `0.3.0`, stable toolchain, `>=1.91.0` required by `google-cloud-*` `rust-version`)**: 16 workspace crates under `src/rs/`:
  - **Runtime & HTTP (`vertebra-core`, `vertebra-http`, `vertebra-py`)**: `vertebra-core` (`SpscRing`, `MpmcRing`, `TieredRingSet`, `BatchBridge`, `ShmCapsule`, `ShmCache`, `jsonwebtoken 11.1`, `lz4_flex`/`zstd`, `gcp-auth`); `vertebra-http` (`vertebra-server` over `hyper 1`, `tokio 1.54`, `matchit 0.9`, `tokio-tungstenite 0.30`, `tls`/`http3`/`compression`); `vertebra-py` (`PyO3 0.30.1` `vertebra._vertebra`, `gil_used = false`).
  - **Frontend, Storage & DB Core (`vertebra-bundler`, `vertebra-storage`, `vertebra-db`)**: `vertebra-bundler` (`_bundler`, `oxc_* 0.155`, `rquickjs 0.14`, `rolldown-notify 11.0`); `vertebra-storage` (`_storage`, `object_store 0.13.2` S3/GCS/Azure/OCI/R2/HTTP + signing); `vertebra-db` (`TransportStream`, Cloud SQL/AlloyDB dialers, `arrow_shm`, wire codecs).
  - **Standalone Database & Cache Extensions**: `vertebra-mysql` (`_mysql`, `mysql_async 0.37`), `vertebra-bigquery` (`_bigquery`, `google-cloud-bigquery =0.19.0`), `vertebra-mssql` (`_mssql`, `tiberius =0.13.0`), `vertebra-oracle` (`_oracle`, `oracledb =26.0.0-beta.5`), `vertebra-spanner` (`_spanner`, `google-cloud-spanner =0.34.8-preview` + interactive RW tx), `vertebra-duckdb` (`_duckdb`, bundled `duckdb =1.20000.1`), `vertebra-postgres` (`_postgres`, `postgres-protocol =0.6.12`), `vertebra-sqlite` (`_sqlite`, bundled `rusqlite` SQLite `3.50.x` WAL/FTS5/JSONB), `vertebra-redis` (`_redis`, RESP3), and `vertebra-valkey` (`_valkey`, RESP3 + `BCAST`/hash-field expiry/`SPUBLISH`).
- **TypeScript (`@vertebra/client` `0.3.0`)**: managed by `bun`, bundled with `tsup ^8.5.1`, type-checked with `typescript ^5.9.3`, linted with `@biomejs/biome ^2.6.6`, tested with `vitest ^5.1.0`, and packaged inside `src/py/vertebra/server/_js/bundle/`. Optional peer dependencies: `react ^19.0.0`, `vue ^3.5.0`, `@angular/core ^22.0.0`, `vite ^8.0.0`.
- **Verification & Documentation Toolchain**: `ruff` (`line-length = 120`, `target-version = "py312"`, `E,F,I,UP,B,SIM`, checking `src/py`, `tools`, `docs/examples`), `mypy` (`strict = true`, `django-stubs>=6.1.2`, `files = ["src/py/vertebra", "src/py/tests/unit", "src/py/tests/integration", "tools/scripts", "tools/pgo", "tools/docs", "tools/benchmarks", "docs/examples"]`) + `mypy.stubtest` across all 13 extensions, `pytest` + `pytest-cov`, `zensical` (`>=0.0.71`) + `mkdocstrings-python` (`>=2.0.3`), `cargo fmt`, `cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings`, `cargo test --workspace --features tls,http3,compression,gcp`, `bunx biome check`, `criterion 0.8`, and `tools.benchmarks`.
<!-- truth: end -->

## Rust Workspace Crates (`src/rs/Cargo.toml`)

| Crate | Path | Targets | Features |
| --- | --- | --- | --- |
| `vertebra-core` | [`src/rs/vertebra-core`](../../../src/rs/vertebra-core/Cargo.toml)[^src-vertebra-core-cargo-toml] | `lib` + Criterion benches `spsc`, `guards`, `ring_throughput`, `codec`, `shm_transport` | `compression` + `gcp-auth` (default), `macos-ulock` |
| `vertebra-http` | [`src/rs/vertebra-http`](../../../src/rs/vertebra-http/Cargo.toml)[^src-vertebra-http-cargo-toml] | `lib` + `vertebra-server` binary + `server_throughput` bench | `python` + `mimalloc` (default), `tls`, `http3`, `compression` |
| `vertebra-py` | [`src/rs/vertebra-py`](../../../src/rs/vertebra-py/Cargo.toml)[^src-vertebra-py-cargo-toml] | `cdylib` + `rlib` (`_vertebra`) | `extension-module`, `gcp` (`vertebra-db/gcp`) |
| `vertebra-bundler` | [`src/rs/vertebra-bundler`](../../../src/rs/vertebra-bundler/Cargo.toml)[^src-vertebra-bundler-cargo-toml] | `cdylib` + `rlib` (`_bundler`, `[bundler]` extra) + `ssr_render` bench | `extension-module` |
| `vertebra-storage` | [`src/rs/vertebra-storage`](../../../src/rs/vertebra-storage/Cargo.toml)[^src-vertebra-storage-cargo-toml] | `cdylib` + `rlib` (`_storage`, `[storage]` extra) | `extension-module` |
| `vertebra-db` | [`src/rs/vertebra-db`](../../../src/rs/vertebra-db/Cargo.toml)[^src-vertebra-db-cargo-toml] | `rlib` (`vertebra_db`) | `gcp`, `pyo3` (`py_async`), `duckdb` |
| `vertebra-mysql` | [`src/rs/vertebra-mysql`](../../../src/rs/vertebra-mysql/Cargo.toml)[^src-vertebra-mysql-cargo-toml] | `cdylib` + `rlib` (`_mysql`, `[mysql]` extra) | `extension-module` |
| `vertebra-bigquery` | [`src/rs/vertebra-bigquery`](../../../src/rs/vertebra-bigquery/Cargo.toml)[^src-vertebra-bigquery-cargo-toml] | `cdylib` + `rlib` (`_bigquery`, `[bigquery]` extra) | `extension-module` |
| `vertebra-mssql` | [`src/rs/vertebra-mssql`](../../../src/rs/vertebra-mssql/Cargo.toml)[^src-vertebra-mssql-cargo-toml] | `cdylib` + `rlib` (`_mssql`, `[mssql]` extra) | `extension-module` |
| `vertebra-oracle` | [`src/rs/vertebra-oracle`](../../../src/rs/vertebra-oracle/Cargo.toml)[^src-vertebra-oracle-cargo-toml] | `cdylib` + `rlib` (`_oracle`, `[oracle]` extra) | `extension-module` |
| `vertebra-spanner` | [`src/rs/vertebra-spanner`](../../../src/rs/vertebra-spanner/Cargo.toml)[^src-vertebra-spanner-cargo-toml] | `cdylib` + `rlib` (`_spanner`, `[spanner]` extra) | `extension-module` |
| `vertebra-duckdb` | [`src/rs/vertebra-duckdb`](../../../src/rs/vertebra-duckdb/Cargo.toml)[^src-vertebra-duckdb-cargo-toml] | `cdylib` + `rlib` (`_duckdb`, `[duckdb]` extra) | `extension-module` |
| `vertebra-postgres` | [`src/rs/vertebra-postgres`](../../../src/rs/vertebra-postgres/Cargo.toml)[^src-vertebra-postgres-cargo-toml] | `cdylib` + `rlib` (`_postgres`, `[postgres]` extra) | `extension-module` |
| `vertebra-sqlite` | [`src/rs/vertebra-sqlite`](../../../src/rs/vertebra-sqlite/Cargo.toml)[^src-vertebra-sqlite-cargo-toml] | `cdylib` + `rlib` (`_sqlite`, `[sqlite]` extra) | `extension-module` |
| `vertebra-redis` | [`src/rs/vertebra-redis`](../../../src/rs/vertebra-redis/Cargo.toml)[^src-vertebra-redis-cargo-toml] | `cdylib` + `rlib` (`_redis`, `[redis]` extra) | `extension-module` |
| `vertebra-valkey` | [`src/rs/vertebra-valkey`](../../../src/rs/vertebra-valkey/Cargo.toml)[^src-vertebra-valkey-cargo-toml] | `cdylib` + `rlib` (`_valkey`, `[valkey]` extra) | `extension-module` |

## Python Package Configuration (`pyproject.toml`)

- **Distribution & Build Backend**: `[project] name = "vertebra-core"` publishes `vertebra_core-*.whl` while `[tool.maturin]` sets `python-source = "src/py"`, `python-packages = ["vertebra"]`, and `module-name = "vertebra._vertebra"`. `make dev` and `make build` run companion `maturin develop` calls across all 13 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_mssql`, `_mysql`, `_oracle`, `_postgres`, `_spanner`, `_duckdb`, `_sqlite`, `_redis`, `_valkey`); `make build-pgo` runs two-phase Rust PGO via `tools/pgo/rust_pgo.py`.
- **CLI & SQLAlchemy Entrypoints**: `vtb = "vertebra.cli:cli"` ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)[^src-cli-init-py]); `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, and `spangres` each register `.vertebra`, `.vtb`, `.vertebra_async`, and `.vtb_async` dialects (36 entry points) from `vertebra.extensions.sqlalchemy.dialects`.
- **Optional Extras & Dependency Groups**: extras cover `sqlspec` (`>=0.73.0`), `alchemy` (`advanced-alchemy>=1.13.0b3`, `sqlalchemy>=2.1.0`, `alembic>=1.18.0`), `arrow`, `pydantic`, `attrs`, `polars`, `litestar`, `starlette`, `fastapi`, `flask`, `django` (`>=6.0`), `queues` (`litestar-queues>=0.14.0`), `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `embeddings`, `iap`, `uvloop`/`winloop`/`rloop`/`zuvloop`, and all 12 companion `vertebra-*==0.3.0` wheel extras (`make install` runs `uv sync --all-extras --dev --group doc`). Dependency groups: `dev` (`test` + `lint` + `build`), `doc`, `bench` (`duckdb>=1.0; python_version < '3.14'`, `uvicorn`, `granian`, `spikard`), `duckdb` (`duckdb>=1.0; python_version < '3.14'`), and `postgres-bench` (`asyncpg`/`psycopg` on `<3.14`).
- **Mypy Configuration & Minimal Overrides**: `[tool.mypy]` sets `strict = true`, `python_version = "3.12"`, and `files = ["src/py/vertebra", "src/py/tests/unit", "src/py/tests/integration", "tools/scripts", "tools/pgo", "tools/docs", "tools/benchmarks", "docs/examples"]`. `ignore_missing_imports = true` is restricted to untyped C or uninstalled optional modules (`numpy*`, `pyarrow*`, `polars*`, `winloop`, `rloop`, `zuvloop`, `concurrent.interpreters`, `duckdb_engine*`, `sqlalchemy_bigquery*`, `google.cloud.*`, `dogpile*`, `django_spanner*`, `spikard*`), with `disallow_subclassing_any = false` scoped to third-party dialect/cache/Django base adapters (`vertebra.extensions.sqlalchemy.dialects.{spanner,bigquery,duckdb}`, `vertebra.extensions.alchemy.cache`, `vertebra.extensions.django.db.backends.*`).

## TypeScript Package Configuration (`src/ts/vertebra-client/`)

Subpath exports (`package.json` and [`tsup.config.ts`](../../../src/ts/vertebra-client/tsup.config.ts)[^src-tsup-config-ts]): `@vertebra/client` (`src/index.ts`), `/vite`, `/react`, `/vue`, `/svelte`, `/angular`, `/htmx`, `/store`, `/hmr`, `/live`, `/apex`, `/telemetry`, `/wasm`, `/arrow`, `/router`, `/webmcp`, and `/agent`. `tsup.config.ts` copies `dist/` into `src/py/vertebra/server/_js/bundle/` for `DevMiddleware` and `ImportMapGenerator.sync_wheel_client`.

## Related Context

See [Product Definition](product.md), [Product Guidelines](product-guidelines.md), [Project Workflow](../knowledge/workflow.md), [Polyglot Test Matrix](../knowledge/testing/polyglot-test-matrix.md), and [Benchmark Suite & Free-Threaded Python 3.14t/3.15t](../knowledge/testing/benchmarks-and-free-threaded.md). Provenance: [^src-pyproject-toml], [^src-cargo-toml], [^src-package-json], [^src-ci-yml], [^src-makefile].

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
[^src-package-json]: `src/ts/vertebra-client/package.json` — TypeScript package manifest (src/ts/vertebra-client/package.json).
[^src-tsup-config-ts]: `src/ts/vertebra-client/tsup.config.ts` — TypeScript client module (src/ts/vertebra-client/tsup.config.ts).
[^src-cli-init-py]: `src/py/vertebra/cli/__init__.py` — Python implementation module (src/py/vertebra/cli/__init__.py).
[^src-ci-yml]: `.github/workflows/ci.yml` — GitHub Actions workflow (ci.yml).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
