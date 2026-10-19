---
type: Guide
title: Vertebra Technology Stack
description: Authoritative language versions, Rust workspace crates, Python dependencies, TypeScript packages, and build/verification toolchains.
status: stable
tags: [tech-stack, python, rust, typescript, maturin]
sources:
  - resource: pyproject.toml
  - resource: src/rs/Cargo.toml
  - resource: src/rs/vertebra-core/Cargo.toml
  - resource: src/rs/vertebra-http/Cargo.toml
  - resource: src/rs/vertebra-py/Cargo.toml
  - resource: src/rs/vertebra-bundler/Cargo.toml
  - resource: src/ts/vertebra-client/package.json
  - resource: Makefile
---

# Vertebra Technology Stack

<!-- truth: start -->
## Core Stack Invariants

- **Python (`>=3.12`, CI matrix `3.12`, `3.13`, `3.14`, `3.14t`)**: Managed by `uv` (`pyproject.toml`, `uv.lock`), built with `maturin` (`>=1.5,<2`), serialized with `msgspec` (`>=0.18`), async primitives with `anyio` (`>=4.0`), and CLI/TUI powered by `rich` (`>=13.0`) and `rich-click` (`>=1.7`).
- **Rust (`edition = "2024"`, `rustc 1.99.1+`)**: Cargo workspace under `src/rs/` with four member crates:
  - `vertebra-core`: Lock-free `SpscRing` / `MpmcRing`, `ShmRegion`, `BatchBridge`, `RingPair`, `TieredRingSet`, `CompressionCodec` (`lz4_flex 0.14`, `zstd 0.14`), and OS sync (`rustix 1.1`, `libc 0.2`, `windows-sys 0.61`).
  - `vertebra-http`: `vertebra-server` binary and HTTP library (`tokio 1.54`, `hyper 1.9`, `hyper-util 0.1`, `matchit 0.9`, `dashmap 6`, `rustls 0.23`, `tokio-rustls 0.26`, `rcgen 0.14`, `async-compression 0.4`, `tokio-tungstenite 0.29`, `pyo3 0.30.1`).
  - `vertebra-py`: PyO3 `0.30.1` cdylib/rlib extension `vertebra._vertebra` (`bytes 1.12`, `rmp-serde 1.3`, `serde 1.0`).
  - `vertebra-bundler`: PyO3 `0.30.1` cdylib extension `vertebra._bundler` (`oxc_* 0.155`, `rquickjs 0.14`, `rolldown-notify 11.0`, `reqwest 0.13`).
- **TypeScript (`@vertebra/client` `0.1.0`)**: Managed by `bun`, bundled with `tsup 8.5`, type-checked with `typescript 5.9`, linted with `@biomejs/biome 2.5`, tested with `vitest 5.0`, with optional peer adapters for `react ^19.0.0`, `vue ^3.5.0`, `svelte ^5.57.2`, `@angular/core ^22.0.0`, `htmx`, and `vite ^8.0.0`.
- **Verification Toolchain**: `ruff` (lint + format, `line-length = 120`, `target-version = "py312"`), `mypy --strict`, `pytest` + `pytest-cov`, `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`, `bunx biome check`, and `criterion 0.8` + `tools.benchmarks` for benchmarking.
<!-- truth: end -->

## Rust Workspace Crates (`src/rs/Cargo.toml`)

| Crate | Path | Targets | Optional Features |
| --- | --- | --- | --- |
| `vertebra-core` | [`src/rs/vertebra-core`](../../../src/rs/vertebra-core/Cargo.toml) | `rlib` + 8 Criterion benches (`spsc`, `mpmc`, `ring_throughput`, `shm_transport`, `spin_wait`, `codec`, `guards`, `router`) | `compression` (default: `lz4_flex`, `zstd`), `macos-ulock` |
| `vertebra-http` | [`src/rs/vertebra-http`](../../../src/rs/vertebra-http/Cargo.toml) | `rlib` + `vertebra-server` binary + `server_throughput` bench | `python` (default: embeds `_vertebra` via `append_to_inittab!`), `tls` (`tokio-rustls`, `rustls`, `rcgen`), `compression` (`async-compression`, `flate2`, `brotli`, `zstd`) |
| `vertebra-py` | [`src/rs/vertebra-py`](../../../src/rs/vertebra-py/Cargo.toml) | `cdylib` + `rlib` (`_vertebra`) | `extension-module` |
| `vertebra-bundler` | [`src/rs/vertebra-bundler`](../../../src/rs/vertebra-bundler/Cargo.toml) | `cdylib` (`_bundler`) | `extension-module` (default) |

## Python Package Configuration (`pyproject.toml`)

- **Build Backend**: `maturin` building `src/rs/vertebra-py/Cargo.toml` into `vertebra._vertebra` (`python-source = "src/py"`). During development (`make dev`) and release builds (`make build`), a second `maturin develop` invocation builds `src/rs/vertebra-bundler/Cargo.toml` into `vertebra._bundler`.
- **CLI Entrypoint**: `vtb = "vertebra.cli:cli"` (`vtb run`, `vtb dev`, `vtb js build`, `vtb js codegen`, `vtb init`).
- **Dependency Groups**:
  - `dev`: includes `test` (`pytest`, `pytest-cov>=5.0`, `anyio`), `lint` (`ruff`, `mypy`), and `build` (`maturin>=1.5,<2`).
  - `bench`: isolated group (`duckdb>=1.0`, `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.21`, `granian>=1.6`, `spikard>=0.12`) invoked only via `uv run --group bench` in `make bench*`.

## TypeScript Package Configuration (`src/ts/vertebra-client/`)

- **Subpath Exports**:
  - `@vertebra/client` (`./dist/index.js`) — `HttpClient`, `RealtimeChannel`, `SessionStore`, `HotReloadClient`, `computeBackoffDelay`, error classes
  - `@vertebra/client/vite` — Vite 8 plugin and dev proxy helpers
  - `@vertebra/client/react` — React 19 hooks and `QueryCache`
  - `@vertebra/client/vue` — Vue 3.5 composables
  - `@vertebra/client/svelte` — Svelte 5 store bindings
  - `@vertebra/client/angular` — Angular 22 DI service bindings
  - `@vertebra/client/htmx` — HTMX extension helpers
  - `@vertebra/client/store` — Standalone `SessionStore`

## Related Context

- [Product Definition](product.md)
- [Product Guidelines](product-guidelines.md)
- [Project Workflow](../knowledge/workflow.md)
- [Polyglot Test Matrix](../knowledge/testing/polyglot-test-matrix.md)
- [Benchmark Suite & Free-Threaded Python 3.14t](../knowledge/testing/benchmarks-and-free-threaded.md)
