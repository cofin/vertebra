---
name: testing-polyglot-test-matrix
description: Rust, Python, and TypeScript test suites, environment registry auto-setup, maturin rebuilds, and /dev/shm cleanup gates.
sources:
  - Makefile
  - tools/scripts/setup-env.sh
  - pyproject.toml
  - src/rs/Cargo.toml
  - src/py/tests/conftest.py
---

# Polyglot Test Matrix & Environment Setup

Vertebra enforces verification gates across three languages—Rust (`cargo test`), Python (`pytest` + `anyio`), and TypeScript (`vitest`)—orchestrated by [`Makefile`](../../../../Makefile).

## 1. Multi-Environment Registry Setup (`make setup-env`)

[`tools/scripts/setup-env.sh`](../../../../tools/scripts/setup-env.sh) (invoked automatically by `make install` and `make upgrade`) inspects `/etc/os-release` (`rodete`) and `FORCE_PUBLIC_REGISTRIES`:

- **Internal Linux (`rodete`)**: Writes local gitignored `uv.toml`, `.npmrc`, `src/ts/vertebra-client/.npmrc`, and `src/rs/.cargo/config.toml` files pointing `uv`, `bun`/`npm`, and `cargo` directly at public upstream registries (`pypi.org`, `registry.npmjs.org`, `index.crates.io`), matching the `UV_INDEX_URL`, `PIP_INDEX_URL`, `NPM_CONFIG_REGISTRY`, and `CARGO_REGISTRIES_CRATES_IO_PROTOCOL` exports at the top of [`Makefile`](../../../../Makefile).
- **External Local / GitHub Actions CI**: Removes any generated override files so standard toolchain defaults apply without modification.

## 2. Canonical Test & Lint Targets (`Makefile`)

| Target | Underlying Command | What It Verifies |
|---|---|---|
| `make dev` | `cargo build --workspace` + `uv run maturin develop --uv` (`vertebra-py` & `vertebra-bundler`) | Compiles and installs debug `vertebra._vertebra` and `vertebra._bundler` into `.venv`. **Required after any Rust edit before running `pytest`.** |
| `make rs-test` | `rm -rf /dev/shm/test-* /dev/shm/*vertebra*` + `cd src/rs && cargo test --workspace --features tls,compression` | Runs all unit and integration tests across `vertebra-core`, `vertebra-http`, `vertebra-py`, and `vertebra-bundler`. |
| `make py-test` | `make dev && uv run pytest src/py/tests -v` | Runs Python unit and integration tests in [`src/py/tests/`](../../../../src/py/tests/conftest.py). |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=..." bun run test` | Runs Vitest unit tests in [`src/ts/vertebra-client/tests/unit/`](../../../../src/ts/vertebra-client/tests/unit/) with an isolated `--localstorage-file`. |
| `make lint-all` | `bunx biome check .` + `ruff check` + `ruff format --check` + `mypy src/py/vertebra` + `cargo fmt --check` + `cargo clippy -- -D warnings` | Read-only CI gate across TypeScript, Python, and Rust. |
| `make check` | `make lint test coverage` | Full pre-PR verification gate across all three languages plus coverage reporting. |

## 3. Shared-Memory Test Hygiene

Rust SHM tests create real segments in `/dev/shm` (Linux) or POSIX SHM (macOS). `make rs-test` automatically cleans `/dev/shm/test-*` and `/dev/shm/*vertebra*` before running `cargo test`.
