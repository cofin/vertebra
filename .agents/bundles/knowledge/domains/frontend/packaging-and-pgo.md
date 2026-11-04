---
type: Concept
title: "Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling"
description: "Relocatable StandaloneBuilder, self-extracting VTB1FILE OnefileBuilder, BuildTarget resolution, cibuildwheel 3.12-3.15t release matrix, and Rust/CPython PGO pipelines."
status: stable
tags: [domains, frontend, packaging, pgo, build]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
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
  - id: src-build-packaging-pgo-py
    resource: src/py/vertebra/build/packaging/pgo.py
    title: "Python implementation module (src/py/vertebra/build/packaging/pgo.py)"
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
- `StandaloneBuilder` and `OnefileBuilder` (`VTB1FILE` trailer with SHA-256 verification and `filter="data"` safe extraction) package relocatable `python-build-standalone` runtimes across Python `3.12`–`3.15t`.
- `vertebra.build.packaging.pgo` (`make build-pgo`) drives instrumented Rust and PyO3 extension builds, workload profiling, validated `.profdata` merging, and `-Cprofile-use` optimization.
<!-- truth: end -->

[`vertebra.build.packaging`](../../../../../src/py/vertebra/build/packaging/__init__.py)[^src-build-packaging-init-py] and [`tools/pgo/`](../../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py] implement relocatable application staging, single-file self-extracting executables (`vtb build --standalone|--onefile [--entrypoint ...] [--pgo] [--profdata ...] [--server-binary ...]`), multi-version wheel distribution (`cp312`–`cp315t`), and Rust/CPython Profile-Guided Optimization (PGO) pipelines.

## 1. Build Targets, Standalone & Onefile Builders (`build/packaging/`, `release.yaml`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py)[^src-build-packaging-targets-py] / `KNOWN_TARGETS` / `SUPPORTED_PYTHON_VERSIONS` | `vertebra.build.packaging.targets` | Frozen `msgspec.Struct(gc=False)` validating `SUPPORTED_PYTHON_VERSIONS` (`"3.12"`, `"3.13"`, `"3.14"`, `"3.14t"`, `"3.15"`, `"3.15t"`) in `detect_host` and `resolve`, resolving host or cross-target triples (`linux-x86_64`, `linux-aarch64`, `linux-x86_64-musl`, `linux-aarch64-musl`, `darwin-aarch64`, `darwin-x86_64`, `windows-x86_64`), and formatting `python-build-standalone` (`PBS_BASE_URL`, `PBS_RELEASE_TAG`) archive URLs. |
| [`StandaloneBuilder`](../../../../../src/py/vertebra/build/packaging/standalone.py)[^src-build-packaging-standalone-py] | `vertebra.build.packaging.standalone` | Copies project sources (excluding VCS, `.venv*`, caches, `node_modules`, `dist`, `target`), precompiles `.pyc` files via `py_compile`, optionally stages `server_binary` into `bin/<name>` (`0o755`) with `VERTEBRA_SERVER_BINARY` bootstrap injection in `__main__.py` (invoking zero-arg CLI callables directly and launching `vertebra-server` with `VERTEBRA_APP=<entrypoint>` for `App`, ASGI, RSGI, and WSGI targets), and writes `manifest.json` (`python_build`, `pgo_mode`, `profdata_bytes`, `profdata_sha256`, `profdata_valid`, `rustflags`). |
| [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py)[^src-build-packaging-onefile-py] | `vertebra.build.packaging.onefile` | Packs a staged standalone directory into a tar archive compressed with `zstandard` (`b"ZSTD"`, level 19) or `zlib` (`b"ZLIB"`, level 9), prepends a POSIX `/bin/sh` self-extracting stub, and appends a 48-byte `ONEFILE_TRAILER_STRUCT` (`<8sQ32s`: `ONEFILE_MAGIC = b"VTB1FILE"`, 8-byte payload length, 32-byte SHA-256 digest). |

- **Verification & Safe Extraction**: `OnefileBuilder.inspect_trailer(binary_path)` validates the `VTB1FILE` magic, payload bounds, and SHA-256 digest; `extract_to(binary_path, destination)` enforces path-traversal checks and extracts with `filter="data"`.
- **Release Wheel Matrix & Cargo Profile ([`release.yaml`](../../../../../.github/workflows/release.yaml)[^src-release-yaml], [`pyproject.toml`](../../../../../pyproject.toml), [`Cargo.toml`](../../../../../src/rs/Cargo.toml)[^src-cargo-toml])**: `.github/workflows/release.yaml` installs `dtolnay/rust-toolchain@stable` and `oven-sh/setup-bun@v2`, builds `src/ts/vertebra-client` (`dist/index.js` and `dist/hmr/index.js` copied into `src/py/vertebra/server/_js/bundle/`), provisions Rust inside manylinux containers via `CIBW_BEFORE_ALL_LINUX` / `CIBW_ENVIRONMENT_LINUX`, and runs `pypa/cibuildwheel@v4.3.4` (`CIBW_BUILD: "cp312-* cp313-* cp314-* cp314t-* cp315-* cp315t-*"`) and `uv build --sdist` to publish `vertebra-core` (`https://pypi.org/p/vertebra-core`, unpacking into `vertebra/` via `[tool.maturin] python-packages = ["vertebra"]`), while `src/rs/Cargo.toml` `[profile.release]` sets `lto = true`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.

## 2. Rust & CPython PGO / 3.15 Capability Tooling (`build/packaging/pgo.py`, `tools/pgo/`, `Makefile`)

- **Two-Phase Rust PGO Pipeline ([`build/packaging/pgo.py`](../../../../../src/py/vertebra/build/packaging/pgo.py)[^src-build-packaging-pgo-py], [`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py), `make build-pgo`)**: [`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py) re-exports the canonical Rust PGO implementation from [`vertebra.build.packaging.pgo`](../../../../../src/py/vertebra/build/packaging/pgo.py) and exposes `python -m tools.pgo.rust_pgo` (`--profile-dir`, `--profdata`, `--baseline-profdata`, `--workload-cmd`, `--llvm-profdata`, `--no-extensions`, `--use-existing`). [`run_pgo_pipeline`](../../../../../src/py/vertebra/build/packaging/pgo.py) (`PgoPipelineResult`) resolves `CARGO_TARGET_DIR` via `resolve_cargo_release_dir`, compiles `vertebra-http` (`build_instrumented`) and all three PyO3 extensions in `EXTENSION_MANIFESTS` (`build_instrumented_extensions`: `vertebra-py` via root `pyproject.toml`, `vertebra-bundler` and `vertebra-storage` via `-m`), runs `run_default_pgo_workload` (or a custom workload), merges and validates `.profraw` counters via `merge_profiles` / `weighted_merge_profiles` + single-pass `validate_profdata`, and recompiles with `-Cprofile-use` (`build_optimized`, `build_optimized_extensions`), falling back cleanly to a standard release build when `.profdata` is missing or corrupt. `make build-pgo` runs the full two-phase pipeline (or reuses an existing profile via `PROFDATA=<path> make build-pgo`).
- **CPython PGO/LTO/BOLT & 3.14/3.15 Runtime Detection ([`build/packaging/pgo.py`](../../../../../src/py/vertebra/build/packaging/pgo.py), [`tools/pgo/python_pgo.py`](../../../../../tools/pgo/python_pgo.py)[^src-tools-pgo-python-pgo-py])**: [`detect_python_build`](../../../../../src/py/vertebra/build/packaging/pgo.py) and `check_pgo_availability` inspect `sysconfig` and `sys._jit` into [`PythonBuildInfo`](../../../../../src/py/vertebra/build/packaging/pgo.py), reporting `--enable-optimizations` (PGO), `--with-lto`, `--enable-bolt`, `is_tail_call_interp` (`--with-tail-call-interp`), `is_jit_available` / `is_jit_enabled` (`sys._jit`), `is_free_threaded` (`Py_GIL_DISABLED`), and `has_frame_pointers` (`-fno-omit-frame-pointer` / `-mno-omit-leaf-frame-pointer` for `sys.monitoring` / Tachyon profiling).

Provenance sources: [^src-cli-build-py], [^src-tools-pgo-init-py], [^src-makefile].

[^src-build-packaging-init-py]: `src/py/vertebra/build/packaging/__init__.py` — Python implementation module (src/py/vertebra/build/packaging/__init__.py).
[^src-build-packaging-standalone-py]: `src/py/vertebra/build/packaging/standalone.py` — Python implementation module (src/py/vertebra/build/packaging/standalone.py).
[^src-build-packaging-onefile-py]: `src/py/vertebra/build/packaging/onefile.py` — Python implementation module (src/py/vertebra/build/packaging/onefile.py).
[^src-build-packaging-targets-py]: `src/py/vertebra/build/packaging/targets.py` — Python implementation module (src/py/vertebra/build/packaging/targets.py).
[^src-build-packaging-pgo-py]: `src/py/vertebra/build/packaging/pgo.py` — Python implementation module (src/py/vertebra/build/packaging/pgo.py).
[^src-cli-build-py]: `src/py/vertebra/cli/build.py` — Python implementation module (src/py/vertebra/cli/build.py).
[^src-tools-pgo-init-py]: `tools/pgo/__init__.py` — Repository tooling module (tools/pgo/__init__.py).
[^src-tools-pgo-rust-pgo-py]: `tools/pgo/rust_pgo.py` — Repository tooling module (tools/pgo/rust_pgo.py).
[^src-tools-pgo-python-pgo-py]: `tools/pgo/python_pgo.py` — Repository tooling module (tools/pgo/python_pgo.py).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-cargo-toml]: `src/rs/Cargo.toml` — Rust crate manifest (src/rs/Cargo.toml).
[^src-release-yaml]: `.github/workflows/release.yaml` — GitHub Actions workflow (release.yaml).
