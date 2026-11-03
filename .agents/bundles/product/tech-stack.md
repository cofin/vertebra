---
type: Guide
title: Vertebra Technology Stack
description: Language versions, Rust workspace crates, Python package configuration, TypeScript package exports, and build and verification toolchains.
status: stable
tags: [tech-stack, python, rust, typescript, maturin]
sources:
  - resource: pyproject.toml
  - resource: src/rs/Cargo.toml
  - resource: src/rs/vertebra-core/Cargo.toml
  - resource: src/rs/vertebra-http/Cargo.toml
  - resource: src/rs/vertebra-py/Cargo.toml
  - resource: src/rs/vertebra-bundler/Cargo.toml
  - resource: src/rs/vertebra-storage/Cargo.toml
  - resource: src/rs/vertebra-db/Cargo.toml
  - resource: src/ts/vertebra-client/package.json
  - resource: src/ts/vertebra-client/tsup.config.ts
  - resource: src/py/vertebra/cli/__init__.py
  - resource: .github/workflows/ci.yml
  - resource: .github/workflows/docs.yml
  - resource: .github/workflows/release.yaml
  - resource: zensical.toml
  - resource: Makefile
---

# Vertebra Technology Stack

<!-- truth: start -->
## Core Stack Invariants

- **Python (`>=3.12`, CI & wheel matrix `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`)**: published on PyPI as `vertebra-core` (`import vertebra`), managed by `uv` (`pyproject.toml`, `uv.lock`), built with `maturin` (`>=1.5,<2`) and `pypa/cibuildwheel@v4.3.4` (`cp312-*`..`cp315t-*`). Runtime dependencies are `anyio>=4.0`, `msgspec>=0.20.0`, `rich>=13.0`, and `rich-click>=1.7`; `uvloop>=0.21` is auto-enabled by `EventLoopPool` on POSIX GIL builds when installed.
- **Rust (`edition = "2024"`, stable toolchain, `>=1.91.0` required by the `google-cloud-*` crates' `rust-version`)**: no `rust-version` or `rust-toolchain` file is pinned in the workspace; CI installs `dtolnay/rust-toolchain@stable`. The Cargo workspace under `src/rs/` has these member crates:
  - `vertebra-core`: `crossbeam-channel 0.5`, `crossbeam-utils 0.8`, `matchit 0.9`, `serde 1.0`, `serde_json 1.0`, `thiserror 2.0`, `tokio 1.54`, `bytes 1.12`, `jsonwebtoken 11.1` (`rust_crypto`), `rmp-serde 1.3`, optional `lz4_flex 0.14` and `zstd 0.14`; `libc 0.2` and `rustix 1.1` on Unix, `windows-sys 0.61` on Windows.
  - `vertebra-http`: `vertebra-server` binary and library (`hyper 1`, `hyper-util 0.1`, `http 1`, `http-body-util 0.1`, `socket2`, `tokio 1.54`, `tokio-util 0.7`, `matchit 0.9`, `futures-util 0.3`, `tokio-tungstenite 0.30`, `brotli 7.0`, `flate2 1.1`, `zstd 0.14`, `multer 3.1`, `dashmap 6`, `fnv 1.0`, `tracing 0.1`, `tracing-subscriber 0.3`; optional `rustls 0.23` + `tokio-rustls 0.26` + `rustls-pemfile 2`, `quinn 0.11.9` + `h3 0.0.8` + `h3-quinn 0.0.10`, `mimalloc 0.1.52`, and `pyo3 0.30.1` with `auto-initialize`).
  - `vertebra-py`: PyO3 `0.30.1` extension `vertebra._vertebra` (`vertebra-core`, `vertebra-db`, `bytes 1.12`, `tokio 1.54`, `tokio-util 0.7`, `futures-util 0.3`).
  - `vertebra-bundler`: PyO3 `0.30.1` extension `vertebra._bundler` (`oxc_* 0.155`, `rquickjs 0.14`, `rolldown-notify 11.0` + `rolldown-notify-debouncer-mini 0.9`, `dashmap 6`, `crossbeam-queue 0.3`, `tokio 1.54`).
  - `vertebra-storage`: PyO3 `0.30.1` extension `vertebra._storage` (`object_store 0.13.2` with `aws`/`gcp`/`azure`/`http`, `reqwest 0.13` with `rustls`, `rsa 0.9`, `hmac 0.12`, `sha2 0.10`, `chrono 0.4`, `url 2.5`, `base64 0.22`).
  - `vertebra-db`: rlib `vertebra_db` (`vertebra-core`, `google-cloud-spanner =0.34.8-preview`, `google-cloud-bigquery =0.19.0`, `google-cloud-auth 1.18`, `google-cloud-gax 1.15`, `google-cloud-longrunning 1.14` [all `default-features = false`], `tonic 0.14` [`tls-ring`, `tls-webpki-roots`], `futures 0.3`, `reqwest 0.13`, `rustls 0.23`, `url 2.5`, `sha2 0.10`, `base64 0.22`, `bytes 1.12`, `serde_json 1.0`, `tokio 1.54`; `opentelemetry` crates enter transitively via `google-cloud-gax` with no metrics exporter enabled; includes `vertebra_db::gcp` for credential bridging from `GcpAuthCore` to `google_cloud_auth::credentials::Credentials` and `rustls` ring `CryptoProvider` installation).
- **TypeScript (`@vertebra/client` `0.2.0`)**: managed by `bun`, bundled with `tsup ^8.5.1`, type-checked with `typescript ^5.9.3`, linted with `@biomejs/biome ^2.6.4`, tested with `vitest ^5.0.5`, and packaged directly inside the `vertebra-core` wheel under `src/py/vertebra/server/_js/bundle/`. Optional peer dependencies are `react ^19.0.0`, `vue ^3.5.0`, `@angular/core ^22.0.0`, and `vite ^8.0.0`; `svelte ^5.57.2` is a dev dependency only, and the HTMX adapter has no dependency.
- **Verification & Documentation Toolchain**: `ruff` (lint + format, `line-length = 120`, `target-version = "py312"`, rules `E,F,I,UP,B,SIM`, covering `src/py/vertebra`, `src/py/tests`, `tools/scripts`, `tools/pgo`, `tools/docs`, and `docs/examples`), `mypy` (`strict = true`, `files = ["src/py/vertebra", "tools/scripts", "tools/pgo", "tools/docs", "docs/examples"]`) + `mypy.stubtest` (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`), `pytest` + `pytest-cov`, `zensical` (`>=0.0.71`) + `mkdocstrings-python` (`>=2.0.3`) + `pymdown-extensions` (`>=10.16`) (`tools/docs/`, `site_url = "https://cofin.github.io/vertebra/"`, `repo_url = "https://github.com/cofin/vertebra"`, `repo_name = "cofin/vertebra"`), `cargo fmt`, `cargo clippy --features tls,compression -- -D warnings`, `cargo test`, `bunx biome check`, `criterion 0.8` microbenchmarks, and the `tools.benchmarks` harness.
<!-- truth: end -->

## Rust Workspace Crates (`src/rs/Cargo.toml`)

| Crate | Path | Targets | Features |
| --- | --- | --- | --- |
| `vertebra-core` | [`src/rs/vertebra-core`](../../../src/rs/vertebra-core/Cargo.toml) | `lib` + Criterion benches `spsc`, `mpmc`, `spin_wait`, `router`, `guards`, `ring_throughput`, `codec`, `shm_transport` | `compression` (default: `lz4_flex`, `zstd`), `macos-ulock` |
| `vertebra-http` | [`src/rs/vertebra-http`](../../../src/rs/vertebra-http/Cargo.toml) | `lib` + `vertebra-server` binary + `server_throughput` bench | `python` + `mimalloc` (default; `python` embeds `vertebra-py`), `tls` (`rustls`, `tokio-rustls`, `rustls-pemfile`), `http3` (`tls` + `quinn`, `h3`, `h3-quinn`), `compression` (enables `vertebra-core/compression`) |
| `vertebra-py` | [`src/rs/vertebra-py`](../../../src/rs/vertebra-py/Cargo.toml) | `cdylib` + `rlib` (`_vertebra`) | `extension-module` |
| `vertebra-bundler` | [`src/rs/vertebra-bundler`](../../../src/rs/vertebra-bundler/Cargo.toml) | `cdylib` + `rlib` (`_bundler`) | `extension-module` |
| `vertebra-storage` | [`src/rs/vertebra-storage`](../../../src/rs/vertebra-storage/Cargo.toml) | `cdylib` + `rlib` (`_storage`) | `extension-module` |
| `vertebra-db` | [`src/rs/vertebra-db`](../../../src/rs/vertebra-db/Cargo.toml) | `rlib` (`vertebra_db`) | none |

`rcgen 0.14` and `reqwest 0.13` are dev-dependencies of `vertebra-http` (test certificates and HTTP clients). The release profile sets `lto = true`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.

## Python Package Configuration (`pyproject.toml`)

- **Distribution & Import Namespace**: `[project] name = "vertebra-core"` publishes the PyPI distribution as `vertebra-core` (`vertebra_core-*.whl` and `vertebra_core-*.tar.gz`), while `[tool.maturin]` sets `python-source = "src/py"`, `python-packages = ["vertebra"]`, `module-name = "vertebra._vertebra"`, and `sdist` `include` globs (`src/rs/Cargo.toml`, `src/rs/Cargo.lock`, `src/rs/vertebra-*/**/*`) so installed packages expose the top-level `vertebra` import namespace and source distributions contain the full Rust workspace.
- **Build Backend**: `maturin` builds `src/rs/vertebra-py/Cargo.toml` into `vertebra._vertebra` (`python-source = "src/py"`, feature `extension-module`). `make dev` and `make build` run two more `maturin develop` calls that build `src/rs/vertebra-bundler/Cargo.toml` into `vertebra._bundler` and `src/rs/vertebra-storage/Cargo.toml` into `vertebra._storage`; `make build-pgo` runs two-phase Rust PGO instrumentation and profile-use compilation via `tools/pgo/rust_pgo.py`.
- **CLI Entrypoint**: `vtb = "vertebra.cli:cli"` registers `vtb run [--topology auto|embedded|shm] [--interface auto|asgi|rsgi|wsgi] [--jit/--no-jit]`, `vtb dev`, `vtb init`, `vtb js {add,remove,list,install,build,generate-types,exec}`, `vtb build [--target ...] [--standalone|--onefile] [--entrypoint ...] [--pgo] [--profdata ...] [--server-binary ...] [--python-version 3.12|3.13|3.14|3.14t|3.15|3.15t]`, `vtb mcp bridge`, `vtb workers run`, and entry-point plugin commands ([`cli/__init__.py`](../../../src/py/vertebra/cli/__init__.py)).
- **SQLAlchemy Dialect Entry Points**: `postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, and `spangres` each register `.vertebra` and `.vtb` (plus `.vertebra_async` / `.vtb_async` for BigQuery, Spanner, and Spangres) dialects from `vertebra.extensions.sqlalchemy.dialects`.
- **Optional Extras**: `framework` (`msgspec>=0.20.0`), `sqlspec` (`sqlspec>=0.69.0`), `alchemy` (`advanced-alchemy>=1.12.0`), and `dev` (`pytest>=8.0`, `pytest-anyio>=0.0.0`, `ruff>=0.4`, `mypy>=1.10`); `make install` syncs all extras.
- **Dependency Groups**:
  - `dev`: includes `test` (`pytest`, `pytest-cov>=5.0`, `anyio`), `lint` (`ruff`, `mypy`), and `build` (`maturin>=1.5,<2`).
  - `doc`: separate group (`zensical>=0.0.71`, `mkdocstrings-python>=2.0.3`, `pymdown-extensions>=10.16`), installed on demand via `--group doc`.
  - `bench`: separate group (`duckdb>=1.0`, `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.21`, `granian>=1.6`, `spikard>=0.12`), used only through `uv run --group bench` in `make bench*`.
- **Mypy Overrides**: `ignore_missing_imports` for optional third-party and runtime modules (`pydantic.*`, `orjson`, `attrs`, `numpy`, `numpy.*`, `pyarrow`, `pyarrow.*`, `uvloop`, `fastapi`, `fastapi.*`, `litestar`, `litestar.*`, `sqlspec`, `sqlspec.*`, `django.*`, `concurrent.interpreters`).

## TypeScript Package Configuration (`src/ts/vertebra-client/`)

Subpath exports (`package.json` and [`tsup.config.ts`](../../../src/ts/vertebra-client/tsup.config.ts)):

| Subpath | Entry | Contents |
| --- | --- | --- |
| `@vertebra/client` | `src/index.ts` | `HttpClient`, `RealtimeChannel`, `SseClient`, `WebSocketClient`, `SessionStore`, `HotReloadClient`, `LiveStateClient`, `ArrowStreamClient`, `WASMClient`, `ApexRegion`, `TraceContext`, `routeUrl`, `wsUrl`, `registerWebMCPTools`, `McpClient`, `A2aClient`, `computeBackoffDelay`, error classes |
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

- [Product Definition](product.md)
- [Product Guidelines](product-guidelines.md)
- [Project Workflow](../knowledge/workflow.md)
- [Polyglot Test Matrix](../knowledge/testing/polyglot-test-matrix.md)
- [Benchmark Suite & Free-Threaded Python 3.14t/3.15t](../knowledge/testing/benchmarks-and-free-threaded.md)
