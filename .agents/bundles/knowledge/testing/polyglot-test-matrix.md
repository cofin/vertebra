---
type: Guide
title: "Polyglot Test Matrix & Environment Setup"
description: "Rust, Python, and TypeScript test suites, their Makefile and CI entry points, registry auto-setup, extension rebuilds, and /dev/shm cleanup."
status: stable
tags: [testing, verification, python, rust, typescript]
sources:
  - resource: Makefile
  - resource: tools/scripts/setup-env.sh
  - resource: pyproject.toml
  - resource: src/rs/Cargo.toml
  - resource: src/py/tests/conftest.py
  - resource: src/ts/vertebra-client/package.json
  - resource: .github/workflows/ci.yml
---

# Polyglot Test Matrix & Environment Setup

Vertebra runs verification gates in three languages: Rust (`cargo test`), Python (`pytest` with the AnyIO plugin), and TypeScript (`vitest`). The [`Makefile`](../../../../Makefile) drives all three locally and in CI.

## 1. Registry Setup (`make setup-env`)

[`tools/scripts/setup-env.sh`](../../../../tools/scripts/setup-env.sh) runs from `make setup-env`, `make install`, and `make upgrade`:

- On every run it deletes `uv.toml` and `src/rs/.cargo/config.toml` if present. Existing `.npmrc` files are never removed.
- On internal corporate Linux workstations whose package mirrors replace the public registries (detected by an `/etc/os-release` match in the script), it writes `uv.toml` (`[[index]] url = "https://pypi.org/simple"`, `default = true`), appends `registry=https://registry.npmjs.org` to `.npmrc` and `src/ts/vertebra-client/.npmrc`, and writes `src/rs/.cargo/config.toml` (`[registries.crates-io] protocol = "sparse"`, `[net] git-fetch-with-cli = true`).
- On other hosts and in GitHub Actions it writes nothing, so toolchain defaults apply.

The `Makefile` applies the same host check and exports `NPM_CONFIG_REGISTRY`, `PIP_INDEX_URL`, `UV_INDEX_URL`, `UV_DEFAULT_INDEX`, `CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse`, and `CARGO_NET_GIT_FETCH_WITH_CLI=true` for every target.

## 2. Test & Lint Targets (`Makefile`)

| Target | Command | What It Verifies |
|---|---|---|
| `make dev` | `cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage` + `uv run maturin develop --uv` for `vertebra-py`, `vertebra-bundler`, and `vertebra-storage` | Builds and installs debug `vertebra._vertebra`, `vertebra._bundler`, and `vertebra._storage` into `.venv`. Required after a Rust edit before running `pytest`. |
| `make rs-test` | `rm -rf /dev/shm/test-* /dev/shm/*vertebra*` + `cd src/rs && cargo test --workspace --features tls,compression` | Unit and integration tests across every workspace crate. |
| `make py-test` | `make dev` + `uv run pytest src/py/tests -v` | Python suites under [`src/py/tests/`](../../../../src/py/tests/conftest.py). |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=${TMPDIR:-/tmp}/vertebra-vitest-localstorage" bun run test` | Every Vitest suite under `src/ts/vertebra-client/tests/`. |
| `make test` | `rs-test` + `py-test` + `ts-test` | All three languages. |
| `make lint-all` | `bunx biome check .` + `ruff check` + `ruff format --check` + `mypy src/py/vertebra` + `cargo fmt --all -- --check` + `cargo clippy --workspace --all-targets --features tls,compression -- -D warnings` | Read-only lint and type gate. |
| `make coverage` | `uv run pytest src/py/tests --cov --quiet` + `coverage xml` + `coverage html` | Python coverage only (`coverage.xml`, `htmlcov/`). |
| `make check` | `lint` + `test` + `coverage` | Full local gate. `lint` applies auto-fixes. |

## 3. CI Matrix (`.github/workflows/ci.yml`)

CI runs on pushes to `main` and on every pull request. A `changes` job (`dorny/paths-filter`) classifies paths as `python` (`src/py/**`, `pyproject.toml`, `uv.lock`), `rust` (`src/rs/**`), `typescript` (`src/ts/**`), or `ci` (`.github/**`, `Makefile`, `.pre-commit-config.yaml`, `biome.json`).

| Job | Runs When | Command | Matrix |
|---|---|---|---|
| `lint` | always | `make ts-install` + `make lint-all` | `ubuntu-latest` |
| `test-python` | `python`, `rust`, or `ci` changed | `make py-test`; on 3.12 also `make coverage` and uploads `coverage.xml` | Python `3.12`, `3.13`, `3.14`, `3.14t` |
| `test-rust` | `rust` or `ci` changed | `make rs-test` | `ubuntu-latest` |
| `test-rust-features` | `rust` or `ci` changed | `cargo test --workspace --features "$CARGO_FEATURES"` | `ubuntu-latest` `tls,compression`; `macos-latest` `tls,compression,macos-ulock` |
| `test-typescript` | `typescript` or `ci` changed | `make ts-install` + `make ts-test` | `ubuntu-latest` |

No CI job runs `make ts-build`, and no job enables the `vertebra-http` `http3` feature.

## 4. Suite Inventory

**Python** (`[tool.pytest.ini_options] testpaths = ["src/py/tests"]`, dependency group `test` = `pytest`, `pytest-cov>=5.0`, `anyio`):

- `unit/server/`: ASGI, RSGI, WSGI, SSE, WebSocket, Litestar fastpath/offload/routing, realtime backpressure, runtime tuning, zero-copy buffers, and the free-threaded hot path (`test_stage1_hotpath_and_314t.py`).
- `unit/framework/`: `App`, Inertia, SSR and prerender, import maps, Vite fallback, live state, and framework subdomains.
- `unit/extensions/`: Litestar plugin, gRPC, SQLAlchemy/SQLSpec `+vtb`, Oracle APEX/EBS, worker lanes, codegen targets, drop-in names, and data/storage integrations.
- `unit/db/`, `unit/storage/`, `unit/telemetry/`: database drivers and data plane, object storage, telemetry and platform.
- `unit/codegen/`, `unit/build/` (`test_packaging_and_pgo.py`, which also imports `tools/pgo`), `unit/cli/`, `unit/ffi/`, `unit/utils/`, plus top-level `test_freethreaded_gil.py`, `test_bench_infrastructure.py`, and `test_utils_*.py`.
- `integration/storage/` holds only Floci emulator fixtures (`conftest.py`) and no collected tests.
- `unit/build/`, `unit/storage/`, and `unit/telemetry/` have no `__init__.py`. Under pytest's default `prepend` import mode their modules import by basename, so a duplicate basename elsewhere would collide.

**Rust**: `vertebra-core/tests/` (`shm_streaming_tests`, `test_mpmc`, `test_ring`, `test_shm`); `vertebra-http/tests/` (server, router, collector, dispatcher, TLS, h2c/mTLS, gRPC, SSE, pub/sub, WebSocket mounts, ingress fastpaths, MCP/discovery, streaming bodies, HTTP/3); `vertebra-storage/tests/storage_engine_tests.rs`. `vertebra-py`, `vertebra-bundler`, and `vertebra-db` use inline `#[cfg(test)]` modules. `test_tls.rs` is gated on `tls`; `http3_quic_tests.rs` is gated on `http3`, which `make rs-test` does not enable; run `cargo test -p vertebra-http --features http3` to exercise it.

**TypeScript**: `package.json` runs `vitest run` with no Vitest config file, so the default glob collects `tests/unit/client.test.ts`, `tests/unit/store.test.ts`, and the top-level `apex`, `arrow`, `live`, `realtime`, `telemetry`, and `wasm` suites.

## 5. Async Tests & Shared-Memory Hygiene

[`src/py/tests/conftest.py`](../../../../src/py/tests/conftest.py) pins the `anyio_backend` fixture to `asyncio`; mark async tests with `@pytest.mark.anyio`. Its `shm_name` fixture returns `test-<uuid8>` names, and the autouse `_cleanup_shm` fixture unlinks `/dev/shm/test-*` and `/dev/shm/*vertebra*` after every test. `make rs-test` removes the same globs before `cargo test`.

Runtime segments are named `/vtb-{pid:x}-{seq:x}-{attempt:x}` and match neither glob. Remove leftovers from interrupted server or worker runs with `rm -f /dev/shm/vtb-*`. On Linux most regions are anonymous `memfd_create` descriptors and leave nothing in `/dev/shm`; `shm_open` names apply on macOS and other Unix hosts.
