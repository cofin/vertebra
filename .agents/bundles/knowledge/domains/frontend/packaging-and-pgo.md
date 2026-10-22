---
type: Concept
title: "Standalone & Onefile Packaging and PGO Tooling"
description: "Relocatable StandaloneBuilder, self-extracting VTB1FILE OnefileBuilder, BuildTarget resolution, and Rust/CPython PGO pipelines."
status: stable
tags: [domains, frontend, packaging, pgo, build]
sources:
  - resource: src/py/vertebra/build/packaging/__init__.py
  - resource: src/py/vertebra/build/packaging/standalone.py
  - resource: src/py/vertebra/build/packaging/onefile.py
  - resource: src/py/vertebra/build/packaging/targets.py
  - resource: src/py/vertebra/cli/build.py
  - resource: tools/pgo/rust_pgo.py
  - resource: tools/pgo/python_pgo.py
---

# Standalone & Onefile Packaging and PGO Tooling

[`vertebra.build.packaging`](../../../../../src/py/vertebra/build/packaging/__init__.py) and [`tools/pgo/`](../../../../../tools/pgo/rust_pgo.py) implement relocatable application staging, single-file self-extracting executables (`vtb build --standalone|--onefile [--pgo]`), and Rust/CPython Profile-Guided Optimization (PGO) pipelines.

## 1. Build Targets, Standalone & Onefile Builders (`build/packaging/`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BuildTarget`](../../../../../src/py/vertebra/build/packaging/targets.py) / `KNOWN_TARGETS` | `vertebra.build.packaging.targets` | Frozen `msgspec.Struct(gc=False)` resolving host or cross-target triples (`linux-x86_64`, `linux-aarch64`, `linux-x86_64-musl`, `darwin-aarch64`, `darwin-x86_64`, `windows-x86_64`), Python version (`3.12`–`3.14t`), and `python-build-standalone` (`PBS_BASE_URL`, `PBS_RELEASE_TAG`) archive URLs. |
| [`StandaloneBuilder`](../../../../../src/py/vertebra/build/packaging/standalone.py) | `vertebra.build.packaging.standalone` | Copies project sources (excluding VCS, `.venv*`, caches, `node_modules`, `dist`, `target`), precompiles `.pyc` files via `py_compile`, and writes `manifest.json` and a bootstrap `__main__.py`. |
| [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py) | `vertebra.build.packaging.onefile` | Packs a staged standalone directory into a tar archive compressed with `zstandard` (`b"ZSTD"`, level 19) or `zlib` (`b"ZLIB"`, level 9), prepends a POSIX `/bin/sh` self-extracting stub, and appends a 48-byte `ONEFILE_TRAILER_STRUCT` (`<8sQ32s`: `ONEFILE_MAGIC = b"VTB1FILE"`, 8-byte payload length, 32-byte SHA-256 digest). |

- **Verification & Safe Extraction**: `OnefileBuilder.inspect_trailer(binary_path)` validates the `VTB1FILE` magic, payload bounds, and SHA-256 digest; `extract_to(binary_path, destination)` enforces path-traversal checks and extracts with `filter="data"`.

## 2. Rust & CPython PGO Tooling (`tools/pgo/`)

- **Rust PGO Pipeline ([`tools/pgo/rust_pgo.py`](../../../../../tools/pgo/rust_pgo.py))**: [`RustPgoConfig`](../../../../../tools/pgo/rust_pgo.py) drives `build_flags` (`PgoMode = Literal["generate", "use", "baseline"]`, `-Cprofile-generate`, `-Cprofile-use`, `-Ctarget-cpu=native`), `build_instrumented` / `build_instrumented_extension`, `find_llvm_profdata`, `merge_profiles`, `weighted_merge_profiles`, `validate_profdata`, and `build_optimized` / `build_optimized_extension`.
- **CPython PGO/LTO/BOLT Detection ([`tools/pgo/python_pgo.py`](../../../../../tools/pgo/python_pgo.py))**: [`detect_python_build`](../../../../../tools/pgo/python_pgo.py) and `check_pgo_availability` inspect `sysconfig` (`--enable-optimizations`, `--with-lto`, `--enable-bolt`, `--with-pydebug`) into [`PythonBuildInfo`](../../../../../tools/pgo/python_pgo.py), while `get_platform_support()` looks up [`PLATFORM_MATRIX`](../../../../../tools/pgo/python_pgo.py). Note that `tools/pgo/` is a library invoked by tests and packaging scripts rather than a standalone `Makefile` target.
