---
type: Concept
title: "Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling"
description: "Relocatable StandaloneBuilder, self-extracting VTB1FILE OnefileBuilder, BuildTarget resolution, cibuildwheel 3.12-3.15t release matrix, and Rust/CPython PGO pipelines."
status: stable
tags:
  - domains
  - frontend
  - packaging
  - pgo
  - build
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:02:00Z"
    commit: 5a833832
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 1b191388
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-bundler-wheel-manifest
    resource: src/rs/vertebra-bundler/pyproject.toml
    title: "Bundler wheel source ownership"
  - id: src-storage-wheel-manifest
    resource: src/rs/vertebra-storage/pyproject.toml
    title: "Storage wheel source ownership"
  - id: src-spanner-wheel-manifest
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: "Spanner wheel source ownership"
  - id: src-bigquery-wheel-manifest
    resource: src/rs/vertebra-bigquery/pyproject.toml
    title: "BigQuery wheel source ownership"
  - id: src-postgres-wheel-manifest
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "PostgreSQL wheel source ownership"
  - id: src-sqlite-wheel-manifest
    resource: src/rs/vertebra-sqlite/pyproject.toml
    title: "SQLite wheel source ownership"
  - id: src-redis-wheel-manifest
    resource: src/rs/vertebra-redis/pyproject.toml
    title: "Redis wheel source ownership"
  - id: src-valkey-wheel-manifest
    resource: src/rs/vertebra-valkey/pyproject.toml
    title: "Valkey wheel source ownership"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Root package manifest, optional extras, and maturin exclusions"
  - id: src-check-extras-py
    resource: tools/scripts/check_extras.py
    title: "Bidirectional pyproject.toml optional extras verifier"
  - id: src-wheel-verify-py
    resource: tools/scripts/_wheel_verify.py
    title: "Shared wheel and sdist ownership verifier"
  - id: src-verify-postgres-wheel-py
    resource: tools/scripts/verify_postgres_wheel.py
    title: "PostgreSQL wheel and sdist isolation verifier"
  - id: src-build-packaging-init-py
    resource: src/py/vertebra/build/packaging/__init__.py
    title: "Python implementation module (src/py/vertebra/build/packaging/__init__.py)"
  - id: src-build-packaging-standalone-py
    resource: src/py/vertebra/build/packaging/standalone.py
    title: "Python implementation module (src/py/vertebra/build/packaging/standalone.py)"
  - id: src-build-packaging-onefile-py
    resource: src/py/vertebra/build/packaging/onefile.py
    title: "Python implementation module (src/py/vertebra/build/packaging/onefile.py)"
  - id: src-build-packaging-targets-py
    resource: src/py/vertebra/build/packaging/targets.py
    title: "Python implementation module (src/py/vertebra/build/packaging/targets.py)"
  - id: src-cli-build-py
    resource: src/py/vertebra/cli/build.py
    title: "Python implementation module (src/py/vertebra/cli/build.py)"
  - id: src-tools-pgo-init-py
    resource: tools/pgo/__init__.py
    title: "Repository tooling module (tools/pgo/__init__.py)"
  - id: src-tools-pgo-rust-pgo-py
    resource: tools/pgo/rust_pgo.py
    title: "Repository tooling module (tools/pgo/rust_pgo.py)"
  - id: src-tools-pgo-python-pgo-py
    resource: tools/pgo/python_pgo.py
    title: "Repository tooling module (tools/pgo/python_pgo.py)"
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
  - id: src-cargo-toml
    resource: src/rs/Cargo.toml
    title: "Rust crate manifest (src/rs/Cargo.toml)"
  - id: src-release-yaml
    resource: .github/workflows/release.yaml
    title: "GitHub Actions workflow (release.yaml)"
---

# Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling

<!-- truth: start -->
- `vertebra.build.packaging` exports user-facing production packaging (`BuildTarget`, `StandaloneBuilder`, `OnefileBuilder`) and stages project sources and lockfiles agnostically across pip, uv, pdm, poetry, and conda projects.
- `StandaloneBuilder` writes `CHECKED_HASH` `.pyc` files with app-relative paths; `OnefileBuilder` defaults to stdlib `zlib` (`--compression zstd` requires `zstandard`), normalizes tar metadata (`SOURCE_DATE_EPOCH` or `0`), checks the `VTB1FILE` SHA-256 trailer, and extracts with `filter="data"`.
- `release.yaml` builds `vertebra-core` plus all 12 companion wheels and sdists, gated by `validate_release_manifest` (blocking git source pins) and `github.event_name == 'release'` on PyPI upload; `check_extras.py` enforces optional-import parity in `make lint`.
- Contributor PGO tooling lives only in `tools/pgo/` (`rust_pgo.py`, `python_pgo.py`, `make build-pgo`) and discovers extension crates from `src/rs/vertebra-*/pyproject.toml`.
<!-- truth: end -->

[`vertebra.build.packaging`](../../../../../src/py/vertebra/build/packaging/__init__.py)[^src-build-packaging-init-py] implements relocatable standalone staging and self-extracting executables (`vtb build --standalone|--onefile`). Multi-version wheel distribution (`cp312`–`cp315t`) runs in `.github/workflows/release.yaml`, while contributor Rust/CPython PGO tooling lives under [`tools/pgo/`](../../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py].

## 1. Build Targets, Standalone & Onefile Builders (`build/packaging/`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py)[^src-build-packaging-targets-py] | `targets.py` | Frozen `msgspec.Struct` validating `SUPPORTED_PYTHON_VERSIONS` (`3.12`, `3.13`, `3.14`, `3.14t`, `3.15`, `3.15t`), resolving host/cross target triples (Linux glibc/musl, macOS, Windows), and formatting `python-build-standalone` URLs. |
| [`StandaloneBuilder`](../../../../../src/py/vertebra/build/packaging/standalone.py)[^src-build-packaging-standalone-py] | `standalone.py` | Copies project sources and lockfiles (excluding VCS, `.venv*`, caches, `node_modules`, `dist`, `target`), precompiles `CHECKED_HASH` `.pyc` files, stages optional `server_binary` into `bin/` with `VERTEBRA_SERVER_BINARY` bootstrap in `__main__.py`, and writes `manifest.json` / `build-manifest.json`. |
| [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py)[^src-build-packaging-onefile-py] | `onefile.py` | Tars a standalone dir with normalized ownership/mtimes (`SOURCE_DATE_EPOCH` or `0`), compresses with `zlib` (level 9) or optional `zstandard` (level 19), prepends a `/bin/sh` extractor stub, and appends a 48-byte `ONEFILE_TRAILER_STRUCT` (`VTB1FILE` magic, length, SHA-256). |

- **Safe Verification & Extraction**: `OnefileBuilder.inspect_trailer` validates magic, payload length, and SHA-256; `extract_to` blocks path traversal and extracts with `filter="data"`.
- **Release Workflow Guards**: [`release.yaml`](../../../../../.github/workflows/release.yaml)[^src-release-yaml] runs `validate_release_manifest` first to block `[tool.uv.sources]` git/rev/branch/tag pins before `build_wheels` (`pypa/cibuildwheel@v4.3.5`, `cp312-*`..`cp315t-*`) and `build_sdist` (`uv build --sdist`). `upload_release` and `upload_pypi` run only on `github.event_name == 'release'`. Release Rust binaries use `[profile.release]` in [`src/rs/Cargo.toml`](../../../../../src/rs/Cargo.toml)[^src-cargo-toml].

## 2. Contributor PGO Tooling (`tools/pgo/`)

- **Two-Phase Rust PGO ([`rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py], `make build-pgo`)**: Discovers `vertebra-py` and every `src/rs/vertebra-*` crate with its own `pyproject.toml`, compiles instrumented binaries and extensions, runs the default or custom workload, merges and validates `.profraw` counters into `.profdata`, and recompiles with `-Cprofile-use` (falling back to a standard release build if `.profdata` is missing or corrupt; supports `PROFDATA=<path> make build-pgo`).
- **CPython Runtime Inspection ([`python_pgo.py`](../../../../../tools/pgo/python_pgo.py)[^src-tools-pgo-python-pgo-py])**: `detect_python_build` and `check_pgo_availability` report PGO, LTO, BOLT, tail-call interpreter, JIT (`sys._jit`), free-threaded (`Py_GIL_DISABLED`), and frame-pointer build flags into `PythonBuildInfo`.

## 3. Companion Wheel Isolation & Extras Parity

- **Wheel Ownership**: `vertebra-core` and all 12 companion distributions (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `redis`, `spanner`, `sqlite`, `storage`, `valkey`) ship isolated wheels and sdists. Each companion manifest pins `vertebra-core==0.3.0`, includes only its own `vertebra/_<name>.pyi`, and excludes `vertebra/**/*.py`, `*.so`, `py.typed`, and sibling `.pyi` stubs; root [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject-toml] excludes all 12 companion stubs from `vertebra-core`.
- **Extras Parity Gate**: [`tools/scripts/check_extras.py`](../../../../../tools/scripts/check_extras.py)[^src-check-extras-py] (run in `make lint` and `make lint-all`) verifies bidirectional parity between optional imports in `src/py/vertebra/**/*.py` and `[project.optional-dependencies]`.
- **Wheel Verification Targets**: [`_wheel_verify.py`](../../../../../tools/scripts/_wheel_verify.py)[^src-wheel-verify-py] and [`verify_postgres_wheel.py`](../../../../../tools/scripts/verify_postgres_wheel.py)[^src-verify-postgres-wheel-py] back `make <name>-wheel-check`, building wheels/sdists and verifying archive isolation in fresh environments [^src-release-yaml][^src-bundler-wheel-manifest][^src-bigquery-wheel-manifest][^src-postgres-wheel-manifest][^src-sqlite-wheel-manifest][^src-redis-wheel-manifest][^src-valkey-wheel-manifest][^src-spanner-wheel-manifest][^src-storage-wheel-manifest].

Provenance sources: [^src-cli-build-py], [^src-tools-pgo-init-py], [^src-makefile].

[^src-build-packaging-init-py]: `src/py/vertebra/build/packaging/__init__.py` — Python implementation module (src/py/vertebra/build/packaging/__init__.py).
[^src-build-packaging-standalone-py]: `src/py/vertebra/build/packaging/standalone.py` — Python implementation module (src/py/vertebra/build/packaging/standalone.py).
[^src-build-packaging-onefile-py]: `src/py/vertebra/build/packaging/onefile.py` — Python implementation module (src/py/vertebra/build/packaging/onefile.py).
[^src-build-packaging-targets-py]: `src/py/vertebra/build/packaging/targets.py` — Python implementation module (src/py/vertebra/build/packaging/targets.py).
[^src-cli-build-py]: `src/py/vertebra/cli/build.py` — Python implementation module (src/py/vertebra/cli/build.py).
[^src-tools-pgo-init-py]: `tools/pgo/__init__.py` — Repository tooling module (tools/pgo/__init__.py).
[^src-tools-pgo-rust-pgo-py]: `tools/pgo/rust_pgo.py` — Repository tooling module (tools/pgo/rust_pgo.py).
[^src-tools-pgo-python-pgo-py]: `tools/pgo/python_pgo.py` — Repository tooling module (tools/pgo/python_pgo.py).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-release-yaml]: `.github/workflows/release.yaml` — GitHub Actions workflow (release.yaml).
[^src-bigquery-wheel-manifest]: `src/rs/vertebra-bigquery/pyproject.toml` — BigQuery wheel source ownership.
[^src-postgres-wheel-manifest]: `src/rs/vertebra-postgres/pyproject.toml` — PostgreSQL wheel source ownership.
[^src-sqlite-wheel-manifest]: `src/rs/vertebra-sqlite/pyproject.toml` — SQLite wheel source ownership.
[^src-redis-wheel-manifest]: `src/rs/vertebra-redis/pyproject.toml` — Redis wheel source ownership.
[^src-valkey-wheel-manifest]: `src/rs/vertebra-valkey/pyproject.toml` — Valkey wheel source ownership.
[^src-pyproject-toml]: `pyproject.toml` — Root package manifest, optional extras, and maturin exclusions.
[^src-check-extras-py]: `tools/scripts/check_extras.py` — Bidirectional `pyproject.toml` optional extras verifier.
[^src-spanner-wheel-manifest]: `src/rs/vertebra-spanner/pyproject.toml` — Spanner wheel source ownership.
[^src-storage-wheel-manifest]: `src/rs/vertebra-storage/pyproject.toml` — Storage wheel source ownership.
[^src-bundler-wheel-manifest]: `src/rs/vertebra-bundler/pyproject.toml` — Bundler wheel source ownership.
[^src-wheel-verify-py]: `tools/scripts/_wheel_verify.py` — Shared wheel and sdist ownership verifier.
[^src-verify-postgres-wheel-py]: `tools/scripts/verify_postgres_wheel.py` — PostgreSQL wheel and sdist isolation verifier.
