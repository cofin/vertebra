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
| `make py-test` | `make dev` + `uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage` + `uv run pytest src/py/tests -v` | `.pyi` stub parity against compiled extensions plus Python suites under [`src/py/tests/`](../../../../src/py/tests/conftest.py). |
| `make ts-test` | `cd src/ts/vertebra-client && NODE_OPTIONS="--localstorage-file=${TMPDIR:-/tmp}/vertebra-vitest-localstorage" bun run test` | Every Vitest suite under `src/ts/vertebra-client/tests/`. |
| `make test` | `rs-test` + `py-test` + `ts-test` | All three languages. |
| `make lint-all` | `bunx biome check .` + `ruff check` & `ruff format --check` (`src/py/vertebra src/py/tests tools/scripts tools/pgo`) + `mypy src/py/vertebra tools/scripts tools/pgo` + `mypy.stubtest` + `cargo fmt --all -- --check` + `cargo clippy --workspace --all-targets --features tls,compression -- -D warnings` + `make review-check` | Read-only lint, type, stub, and review-manifest gate. |
| `make coverage` | `uv run pytest src/py/tests --cov --quiet` + `coverage xml` + `coverage html` | Python coverage only (`coverage.xml`, `htmlcov/`). |
| `make check` | `lint` + `test` + `coverage` | Full local gate. `lint` applies auto-fixes. |

## 3. CI Matrix (`.github/workflows/ci.yml`)

CI runs on pushes to `main` and on every pull request. A `changes` job (`dorny/paths-filter`) classifies paths as `python` (`src/py/**`, `pyproject.toml`, `uv.lock`), `rust` (`src/rs/**`), `typescript` (`src/ts/**`), or `ci` (`.github/**`, `Makefile`, `.pre-commit-config.yaml`, `biome.json`).

| Job | Runs When | Command | Matrix |
|---|---|---|---|
| `lint` | always | `make ts-install` + `make lint-all` | `ubuntu-latest` |
| `test-python` | `python`, `rust`, or `ci` changed | `make py-test`; on 3.12 also `make coverage` and uploads `coverage.xml` | Python `3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t` (`3.15`/`3.15t` pre-release matrix entries) |
| `test-rust` | `rust` or `ci` changed | `make rs-test` | `ubuntu-latest` |
| `test-rust-features` | `rust` or `ci` changed | `cargo test --workspace --features "$CARGO_FEATURES"` | `ubuntu-latest` `tls,compression`; `macos-latest` `tls,compression,macos-ulock` |
| `test-typescript` | `typescript` or `ci` changed | `make ts-install` + `make ts-build` + `make ts-test` | `ubuntu-latest` |

## 4. Suite Inventory & Test Quality Standards

**Python** (`[tool.pytest.ini_options] testpaths = ["src/py/tests"]`, dependency group `test` = `pytest`, `pytest-cov>=5.0`, `anyio`, `[tool.coverage.report] fail_under = 80`):

- **1:1 Module-Mirrored Layout**: Every directory under `src/py/tests/unit/` mirrors `src/py/vertebra/` (`unit/server/`, `unit/supervisor/`, `unit/framework/`, `unit/extensions/`, `unit/db/`, `unit/storage/`, `unit/telemetry/`, `unit/codegen/`, `unit/build/`, `unit/cli/`, `unit/ffi/`, `unit/native/`, `unit/utils/`) and contains an `__init__.py` so `ruff` (`INP001`) and `pytest` resolve package namespaces cleanly.
- **Top-Level Imports (`PLC0415`) & Zero Stage-Numbered Tests**: All test files place imports at module top level (`PLC0415` enforced) and use domain-named filenames (`test_hotpath_and_314t.py`, `test_loops.py`, `test_asgi_runner.py`, `test_wsgi_adapter.py`, `test_rsgi_adapter.py`) rather than stage-numbered or audit-remediation filenames.
- **Strong vs. Weak Assertions**: Assert exact observable outputs (status codes, wire bytes, SQL strings + bind tuples, round-trip decoded frames, state transitions, and negative error paths with `pytest.raises(SpecificException, match=...)`) rather than `assert x is not None` or `hasattr` checks.

**Rust**: Single-binary integration harnesses (`tests/it/main.rs`) in `vertebra-core` (`tests/it/{shm,spsc,mpmc,streaming,tiered_ring}.rs`) and `vertebra-http` (`tests/it/{server,routing,batcher,dispatcher,backpressure,tls,h2c_mtls,http3,grpc,sse,pubsub,websocket,fastpaths,discovery,streaming}.rs`), plus `vertebra-storage/tests/storage_engine_tests.rs`. `vertebra-py` and `vertebra-bundler` use inline `#[cfg(test)]` modules.

**TypeScript**: `package.json` runs `vitest run` over `tests/unit/client.test.ts`, `tests/unit/store.test.ts`, `tests/unit/adapters.test.ts`, and the top-level `apex`, `arrow`, `live`, `realtime`, `telemetry`, and `wasm` suites (including error-injection, retry-exhaustion, and reconnect-cap coverage).

## 5. Async Tests & Shared-Memory Hygiene

[`src/py/tests/conftest.py`](../../../../src/py/tests/conftest.py) pins the `anyio_backend` fixture to `asyncio`; mark async tests with `@pytest.mark.anyio`. Its `shm_name` fixture returns `test-<uuid8>` names, and the autouse `_cleanup_shm` fixture unlinks `/dev/shm/test-*` and `/dev/shm/*vertebra*` after every test. `make rs-test` removes the same globs before `cargo test`.

Runtime segments are named `/vtb-{pid:x}-{seq:x}-{attempt:x}` and match neither glob. Remove leftovers from interrupted server or worker runs with `rm -f /dev/shm/vtb-*`. On Linux most regions are anonymous `memfd_create` descriptors and leave nothing in `/dev/shm`; `shm_open` names apply on macOS and other Unix hosts.
