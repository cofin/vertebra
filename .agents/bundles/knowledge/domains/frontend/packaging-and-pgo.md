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
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 334dc98d
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
- `vertebra.build.packaging` exports only user-facing production packaging (`BuildTarget`, `StandaloneBuilder`, `OnefileBuilder`) and stages project sources and lockfiles as-is, so pip/venv, uv, pdm, poetry, and conda projects package identically.
- `StandaloneBuilder` writes `CHECKED_HASH` `.pyc` files with app-relative source paths and `OnefileBuilder` normalizes tar ownership and mtimes (`SOURCE_DATE_EPOCH` or `0`), so identical trees rebuild to byte-identical bundles; `OnefileBuilder` verifies a `VTB1FILE` SHA-256 trailer and extracts with `filter="data"`.
- Contributor PGO tooling lives only in `tools/pgo/` (`rust_pgo.py`, `python_pgo.py`, `make build-pgo`), discovering PyO3 extension crates from `src/rs/vertebra-*/pyproject.toml`.
<!-- truth: end -->

[`vertebra.build.packaging`](../../../../../src/py/vertebra/build/packaging/__init__.py)[^src-build-packaging-init-py] implements relocatable application staging and single-file self-extracting executables (`vtb build --standalone|--onefile [--entrypoint ...] [--server-binary ...]`) for production deployments. Multi-version wheel distribution (`cp312`–`cp315t`) runs in the release workflow, and contributor Rust/CPython Profile-Guided Optimization (PGO) tooling lives in [`tools/pgo/`](../../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py], outside the shipped package.

## 1. Build Targets, Standalone & Onefile Builders (`build/packaging/`, `release.yaml`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py)[^src-build-packaging-targets-py] / `KNOWN_TARGETS` / `SUPPORTED_PYTHON_VERSIONS` | `vertebra.build.packaging.targets` | Frozen `msgspec.Struct(gc=False)` validating `SUPPORTED_PYTHON_VERSIONS` (`"3.12"`, `"3.13"`, `"3.14"`, `"3.14t"`, `"3.15"`, `"3.15t"`) in `detect_host` and `resolve`, resolving host or cross-target triples (`linux-x86_64`, `linux-aarch64`, `linux-x86_64-musl`, `linux-aarch64-musl`, `darwin-aarch64`, `darwin-x86_64`, `windows-x86_64`), and formatting `python-build-standalone` (`PBS_BASE_URL`, `PBS_RELEASE_TAG`) archive URLs. |
| [`StandaloneBuilder`](../../../../../src/py/vertebra/build/packaging/standalone.py)[^src-build-packaging-standalone-py] | `vertebra.build.packaging.standalone` | Copies project sources and lockfiles (excluding VCS, `.venv*`, caches, `node_modules`, `dist`, `target`), precompiles `CHECKED_HASH` `.pyc` files with app-relative source paths via `py_compile`, optionally stages `server_binary` into `bin/<name>` (`0o755`) with `VERTEBRA_SERVER_BINARY` bootstrap injection in `__main__.py` (invoking zero-arg CLI callables directly and launching `vertebra-server` with `VERTEBRA_APP=<entrypoint>` for `App`, ASGI, RSGI, and WSGI targets), and writes `manifest.json` / `build-manifest.json` (`entrypoint`, `profile`, `target`, `compiled_modules`, optional `server_binary`). |
| [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py)[^src-build-packaging-onefile-py] | `vertebra.build.packaging.onefile` | Packs a staged standalone directory into a tar archive with normalized headers (`uid`/`gid` `0`, empty owner names, `SOURCE_DATE_EPOCH` or `0` mtimes) compressed with `zstandard` (`b"ZSTD"`, level 19) or `zlib` (`b"ZLIB"`, level 9), prepends a POSIX `/bin/sh` self-extracting stub, and appends a 48-byte `ONEFILE_TRAILER_STRUCT` (`<8sQ32s`: `ONEFILE_MAGIC = b"VTB1FILE"`, 8-byte payload length, 32-byte SHA-256 digest). |

- **Verification & Safe Extraction**: `OnefileBuilder.inspect_trailer(binary_path)` validates the `VTB1FILE` magic, payload bounds, and SHA-256 digest; `extract_to(binary_path, destination)` enforces path-traversal checks and extracts with `filter="data"`.
- **Release Wheel Matrix & Cargo Profile ([`release.yaml`](../../../../../.github/workflows/release.yaml)[^src-release-yaml], [`pyproject.toml`](../../../../../pyproject.toml), [`Cargo.toml`](../../../../../src/rs/Cargo.toml)[^src-cargo-toml])**: `.github/workflows/release.yaml` installs `dtolnay/rust-toolchain@stable` and `oven-sh/setup-bun@v2`, builds `src/ts/vertebra-client` (`tsup` compiles 17 subpath entries and copies `dist/` recursively into `src/py/vertebra/server/_js/bundle/`), provisions Rust inside manylinux containers via `CIBW_BEFORE_ALL_LINUX` / `CIBW_ENVIRONMENT_LINUX`, and runs `pypa/cibuildwheel@v4.3.5` (`CIBW_BUILD: "cp312-* cp313-* cp314-* cp314t-* cp315-* cp315t-*"`) and `uv build --sdist` across `vertebra-core` and all nine companion extension manifests (`vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`, `vertebra-spanner`), while `src/rs/Cargo.toml` `[profile.release]` sets `lto = true`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.

## 2. Contributor Rust & CPython PGO Tooling (`tools/pgo/`, `Makefile`)

- **Two-Phase Rust PGO Pipeline ([`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py], `make build-pgo`)**: `python -m tools.pgo.rust_pgo` (`--profile-dir`, `--profdata`, `--baseline-profdata`, `--workload-cmd`, `--llvm-profdata`, `--no-extensions`, `--use-existing`) runs `run_pgo_pipeline` (`PGOPipelineResult`). It resolves `CARGO_TARGET_DIR` via `resolve_cargo_release_dir`, compiles `vertebra-http` (`build_instrumented`) and every PyO3 extension in `EXTENSION_MANIFESTS` (`discover_extension_manifests`: root-configured `vertebra-py` plus each `src/rs/vertebra-*` crate with its own maturin `pyproject.toml`, built via `-m`), runs `run_default_pgo_workload` (importing each discovered `extension_module_name`) or a custom workload, merges and validates `.profraw` counters via `merge_profiles` / `weighted_merge_profiles` + single-pass `validate_profdata`, and recompiles with `-Cprofile-use` (`build_optimized`, `build_optimized_extensions`), falling back to a standard release build when `.profdata` is missing or corrupt. `make build-pgo` runs the full pipeline (or reuses an existing profile via `PROFDATA=<path> make build-pgo`).
- **CPython PGO/LTO/BOLT & 3.14/3.15 Runtime Detection ([`tools/pgo/python_pgo.py`](../../../../../tools/pgo/python_pgo.py)[^src-tools-pgo-python-pgo-py])**: `detect_python_build` and `check_pgo_availability` inspect `sysconfig` and `sys._jit` into `PythonBuildInfo`, reporting `--enable-optimizations` (PGO), `--with-lto`, `--enable-bolt`, `is_tail_call_interp` (`--with-tail-call-interp`), `is_jit_available` / `is_jit_enabled` (`sys._jit`), `is_free_threaded` (`Py_GIL_DISABLED`), and `has_frame_pointers` (`-fno-omit-frame-pointer` / `-mno-omit-leaf-frame-pointer` for `sys.monitoring` / Tachyon profiling).


## Optional extension distributions

The release workflow builds `vertebra-core` plus all nine companion distributions (`vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-duckdb`, `vertebra-mssql`, `vertebra-mysql`, `vertebra-oracle`, `vertebra-postgres`, and `vertebra-spanner`) as platform wheels and source distributions. Every companion manifest declares Maturin `exclude` rules blocking `**/py/vertebra/**/*.py`, `**/py/vertebra/**/*.so`, `**/py/vertebra/py.typed`, and sibling `*.pyi` stubs during wheel and sdist builds, while `vertebra-postgres` and `vertebra-oracle` also declare explicit Maturin `include` rules for `vertebra/_postgres.pyi` and `vertebra/_oracle.pyi`. Fresh-environment installs pair `vertebra-core` with extras such as `[bundler]`, `[storage]`, `[bigquery]`, `[duckdb]`, `[mssql]`, `[mysql]`, `[oracle]`, `[postgres]`, or `[spanner]`.

[`tools/scripts/_wheel_verify.py`](../../../../../tools/scripts/_wheel_verify.py)[^src-wheel-verify-py] powers `make {duckdb,mysql,mssql}-wheel-check` (building via `maturin build` and checking wheel archive layout/entries, with `--forbid-python-files` on MySQL and SQL Server) as well as `make postgres-wheel-check` ([`verify_postgres_wheel.py`](../../../../../tools/scripts/verify_postgres_wheel.py)[^src-verify-postgres-wheel-py]), `make oracle-wheel-check`, and `make {storage,bigquery,spanner,bundler}-wheel-check` (which install each built wheel and sdist inside an isolated environment). Removing an optional wheel leaves core and sibling extensions intact; cross-platform validation runs in the hosted release workflow. [^src-release-yaml][^src-bundler-wheel-manifest][^src-bigquery-wheel-manifest][^src-postgres-wheel-manifest][^src-spanner-wheel-manifest][^src-storage-wheel-manifest]

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
[^src-spanner-wheel-manifest]: `src/rs/vertebra-spanner/pyproject.toml` — Spanner wheel source ownership.

[^src-storage-wheel-manifest]: `src/rs/vertebra-storage/pyproject.toml` — Storage wheel source ownership.
[^src-bundler-wheel-manifest]: `src/rs/vertebra-bundler/pyproject.toml` — Bundler wheel source ownership.
[^src-wheel-verify-py]: `tools/scripts/_wheel_verify.py` — Shared wheel and sdist ownership verifier.
[^src-verify-postgres-wheel-py]: `tools/scripts/verify_postgres_wheel.py` — PostgreSQL wheel and sdist isolation verifier.
