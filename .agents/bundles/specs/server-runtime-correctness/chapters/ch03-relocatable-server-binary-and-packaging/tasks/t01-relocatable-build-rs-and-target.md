---
type: Task
id: "server-runtime-correctness:ch03:t01"
flow_id: server-runtime-correctness
chapter_id: ch03-relocatable-server-binary-and-packaging
title: Remove Absolute Build Rpaths and Create ServerBinaryBuilder
description: Drop absolute build-machine rpaths in vertebra-http/build.rs and implement per-ABI relocatable server binary building in tools/scripts/build_server_binary.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-http/build.rs
  - tools/scripts/build_server_binary.py
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - src/rs/vertebra-http/build.rs
  - src/rs/vertebra-http/Cargo.toml
  - .github/workflows/release-binaries.yml
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 220
tags:
  - rust
  - packaging
  - binary
  - pyo3
sources:
  - id: http-build-rs
    resource: src/rs/vertebra-http/build.rs
    title: Current build script setting absolute build-host rpath
  - id: release-binaries-yml
    resource: .github/workflows/release-binaries.yml
    title: Release server binary cargo flags
---

# Task `t01`: Remove Absolute Build Rpaths and Create ServerBinaryBuilder

## Objective & Scope Boundary

Remove non-relocatable build-machine rpaths from `src/rs/vertebra-http/build.rs` and create `tools/scripts/build_server_binary.py` (`ServerBinaryTarget`, `ServerBinaryBuilder`) per [`../spec.md` §2](../spec.md) [^http-build-rs] [^release-binaries-yml].

## Bounded Read Manifest

- `src/rs/vertebra-http/build.rs` & `Cargo.toml` — PyO3 link flags and server feature flags.
- `.github/workflows/release-binaries.yml` — canonical `--bin vertebra-server` release flags.

## Contracts & Edge Cases

- `build.rs` emits library search/link flags for compilation without embedding the build machine's absolute `LIBDIR` into the ELF/Mach-O runtime rpath.
- `ServerBinaryTarget` models `python_version`, `free_threaded` (`'t'` ABI suffix), `platform`, `rust_target` (honoring macOS `ARCHFLAGS`), `binary_name`, `libpython_soname`, and `libpython_dylib`.
- `ServerBinaryBuilder` selects a shared-libpython interpreter (falling back to `uv python find cpython-<ver><t>` when static), runs `cargo build --bin vertebra-server --release`, rewrites macOS `libpython` install names to `@rpath/<dylib>` + ad-hoc `codesign`, and stages `src/py/vertebra/bin/<binary_name>`.

## Test Matrix

- Parameterized unit tests in `test_packaging_and_pgo.py` covering `3.12`–`3.15t` target detection, static-to-`uv python find` fallback, macOS `ARCHFLAGS` + `otool` rewrite calculation, and missing-`uv` fail-loud `SystemExit`.

## Verification

```bash
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-http --bin vertebra-server && uv run --no-sync ruff check tools/scripts/build_server_binary.py && uv run --no-sync mypy tools/scripts/build_server_binary.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [ ] `vertebra-http/build.rs` embeds zero build-host absolute rpaths.
- [ ] `ServerBinaryTarget` and `ServerBinaryBuilder` pass strict typing and unit tests across Linux, macOS, Windows, and free-threaded ABIs.

[^http-build-rs]: `src/rs/vertebra-http/build.rs` — Current build script setting absolute build-host rpath.
[^release-binaries-yml]: `.github/workflows/release-binaries.yml` — Release server binary cargo flags.
