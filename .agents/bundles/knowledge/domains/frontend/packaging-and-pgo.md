---
type: Concept
title: "Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling"
description: "Relocatable StandaloneBuilder, self-extracting VTB1FILE OnefileBuilder, BuildTarget resolution, cibuildwheel 3.12-3.15t release matrix, and Rust/CPython PGO pipelines."
status: stable
tags: [domains, frontend, packaging, pgo, build]
sources:
  - resource: src/py/vertebra/build/packaging/__init__.py
  - resource: src/py/vertebra/build/packaging/standalone.py
  - resource: src/py/vertebra/build/packaging/onefile.py
  - resource: src/py/vertebra/build/packaging/targets.py
  - resource: src/py/vertebra/build/packaging/pgo.py
  - resource: src/py/vertebra/cli/build.py
  - resource: tools/pgo/__init__.py
  - resource: tools/pgo/rust_pgo.py
  - resource: tools/pgo/python_pgo.py
  - resource: Makefile
  - resource: src/rs/Cargo.toml
  - resource: .github/workflows/release.yaml
---

# Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling

[`vertebra.build.packaging`](../../../../../src/py/vertebra/build/packaging/__init__.py) and [`tools/pgo/`](../../../../../tools/pgo/rust_pgo.py) implement relocatable application staging, single-file self-extracting executables (`vtb build --standalone|--onefile [--entrypoint ...] [--pgo] [--profdata ...] [--server-binary ...]`), multi-version wheel distribution (`cp312`–`cp315t`), and Rust/CPython Profile-Guided Optimization (PGO) pipelines.

## 1. Build Targets, Standalone & Onefile Builders (`build/packaging/`, `release.yaml`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py) / `KNOWN_TARGETS` / `SUPPORTED_PYTHON_VERSIONS` | `vertebra.build.packaging.targets` | Frozen `msgspec.Struct(gc=False)` validating `SUPPORTED_PYTHON_VERSIONS` (`"3.12"`, `"3.13"`, `"3.14"`, `"3.14t"`, `"3.15"`, `"3.15t"`) in `detect_host` and `resolve`, resolving host or cross-target triples (`linux-x86_64`, `linux-aarch64`, `linux-x86_64-musl`, `linux-aarch64-musl`, `darwin-aarch64`, `darwin-x86_64`, `windows-x86_64`), and formatting `python-build-standalone` (`PBS_BASE_URL`, `PBS_RELEASE_TAG`) archive URLs. |
| [`StandaloneBuilder`](../../../../../src/py/vertebra/build/packaging/standalone.py) | `vertebra.build.packaging.standalone` | Copies project sources (excluding VCS, `.venv*`, caches, `node_modules`, `dist`, `target`), precompiles `.pyc` files via `py_compile`, optionally stages `server_binary` into `bin/<name>` (`0o755`) with `VERTEBRA_SERVER_BINARY` bootstrap injection in `__main__.py`, and writes `manifest.json` (`python_build`, `pgo_mode`, `profdata_bytes`, `profdata_sha256`, `profdata_valid`, `rustflags`). |
| [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py) | `vertebra.build.packaging.onefile` | Packs a staged standalone directory into a tar archive compressed with `zstandard` (`b"ZSTD"`, level 19) or `zlib` (`b"ZLIB"`, level 9), prepends a POSIX `/bin/sh` self-extracting stub, and appends a 48-byte `ONEFILE_TRAILER_STRUCT` (`<8sQ32s`: `ONEFILE_MAGIC = b"VTB1FILE"`, 8-byte payload length, 32-byte SHA-256 digest). |

- **Verification & Safe Extraction**: `OnefileBuilder.inspect_trailer(binary_path)` validates the `VTB1FILE` magic, payload bounds, and SHA-256 digest; `extract_to(binary_path, destination)` enforces path-traversal checks and extracts with `filter="data"`.
- **Release Wheel Matrix & Cargo Profile ([`release.yaml`](../../../../../.github/workflows/release.yaml), [`Cargo.toml`](../../../../../src/rs/Cargo.toml))**: `.github/workflows/release.yaml` uses `pypa/cibuildwheel@v4.3.4` with `CIBW_BUILD: "cp312-* cp313-* cp314-* cp314t-* cp315-* cp315t-*"` (natively building Python 3.15 and free-threaded `3.14t`/`3.15t` wheels without deprecated `CIBW_ENABLE`), while `src/rs/Cargo.toml` `[profile.release]` sets `lto = true`, `codegen-units = 1`, `opt-level = 3`, and `panic = "abort"`.

## 2. Rust & CPython PGO / 3.15 Capability Tooling (`build/packaging/pgo.py`, `tools/pgo/`, `Makefile`)

- **Two-Phase Rust PGO Pipeline ([`build/packaging/pgo.py`](../../../../../src/py/vertebra/build/packaging/pgo.py), [`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py), `make build-pgo`)**: [`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py) re-exports the canonical Rust PGO implementation from [`vertebra.build.packaging.pgo`](../../../../../src/py/vertebra/build/packaging/pgo.py) and exposes `python -m tools.pgo.rust_pgo` (`--profile-dir`, `--profdata`, `--baseline-profdata`, `--workload-cmd`, `--llvm-profdata`, `--no-extensions`, `--use-existing`). [`run_pgo_pipeline`](../../../../../src/py/vertebra/build/packaging/pgo.py) (`PgoPipelineResult`) resolves `CARGO_TARGET_DIR` via `resolve_cargo_release_dir`, compiles `vertebra-http` (`build_instrumented`) and all three PyO3 extensions in `EXTENSION_MANIFESTS` (`build_instrumented_extensions`: `vertebra-py` via root `pyproject.toml`, `vertebra-bundler` and `vertebra-storage` via `-m`), runs `run_default_pgo_workload` (or a custom workload), merges and validates `.profraw` counters via `merge_profiles` / `weighted_merge_profiles` + single-pass `validate_profdata`, and recompiles with `-Cprofile-use` (`build_optimized`, `build_optimized_extensions`), falling back cleanly to a standard release build when `.profdata` is missing or corrupt. `make build-pgo` runs the full two-phase pipeline (or reuses an existing profile via `PROFDATA=<path> make build-pgo`).
- **CPython PGO/LTO/BOLT & 3.14/3.15 Runtime Detection ([`build/packaging/pgo.py`](../../../../../src/py/vertebra/build/packaging/pgo.py), [`tools/pgo/python_pgo.py`](../../../../../tools/pgo/python_pgo.py))**: [`detect_python_build`](../../../../../src/py/vertebra/build/packaging/pgo.py) and `check_pgo_availability` inspect `sysconfig` and `sys._jit` into [`PythonBuildInfo`](../../../../../src/py/vertebra/build/packaging/pgo.py), reporting `--enable-optimizations` (PGO), `--with-lto`, `--enable-bolt`, `is_tail_call_interp` (`--with-tail-call-interp`), `is_jit_available` / `is_jit_enabled` (`sys._jit`), `is_free_threaded` (`Py_GIL_DISABLED`), and `has_frame_pointers` (`-fno-omit-frame-pointer` / `-mno-omit-leaf-frame-pointer` for `sys.monitoring` / Tachyon profiling).
