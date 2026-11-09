---
type: Task
id: "native-oracle-driver:ch01:t02"
chapter_id: ch01-crate-and-packaging
title: Register workspace member, Maturin wheel manifest, and _oracle.pyi type stub
description: Add vertebra-oracle to src/rs/Cargo.toml (resolving src/rs/Cargo.lock), author src/rs/vertebra-oracle/pyproject.toml for the vertebra._oracle namespace wheel, and create src/py/vertebra/_oracle.pyi declaring only the metadata constants exported at ch01:t02.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-oracle-driver:ch01:t01"
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-oracle/Cargo.toml
  - src/rs/vertebra-oracle/src/lib.rs
  - src/rs/vertebra-spanner/pyproject.toml
  - src/py/vertebra/_spanner.pyi
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
files:
  - src/rs/Cargo.toml
  - src/rs/vertebra-oracle/pyproject.toml
  - src/py/vertebra/_oracle.pyi
tests: []
estimated_loc: 75
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Rust workspace members manifest
  - id: spanner-pyproject
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Reference Maturin namespace wheel manifest and file exclusions
  - id: spanner-pyi
    resource: src/py/vertebra/_spanner.pyi::SyncSpannerClient
    title: Reference native extension .pyi stub header
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Standalone vertebra._oracle wheel boundary and module exports
tags:
  - oracle
  - cargo
  - wheel
  - stubs
---

# Task t02: Register Workspace Member, Maturin Wheel Manifest, and `_oracle.pyi` Type Stub

## Objective & Scope Boundary

Register `"vertebra-oracle"` in `src/rs/Cargo.toml` `[workspace].members` (and sync `src/rs/Cargo.lock` so `--locked` workspace builds pass), author `src/rs/vertebra-oracle/pyproject.toml` packaging `vertebra._oracle` as an isolated Maturin namespace wheel, and create `src/py/vertebra/_oracle.pyi` declaring strictly the metadata constants exported by `src/rs/vertebra-oracle/src/lib.rs` at `ch01:t02` (`__version__`, `FREE_THREADED`, `GIL_USED`) [^ws-cargo] [^spanner-pyproject] [^spanner-pyi] [^ora-contracts]. Root `pyproject.toml`, `Makefile`, and `.agents/skills/review-core/references/areas.toml` wiring belong to `native-oracle-driver:ch01:t03` ([../spec.md](../spec.md)), and client/result class stubs are added incrementally in `ch02:t03` and `ch03:t02` as each symbol is registered in `lib.rs`.

## Bounded Read Manifest

- `src/rs/Cargo.toml` — `[workspace].members` list [^ws-cargo].
- `src/rs/vertebra-oracle/Cargo.toml` — Crate manifest created in `ch01:t01`.
- `src/rs/vertebra-oracle/src/lib.rs` — Exported `__version__`, `FREE_THREADED`, and `GIL_USED` constants from `ch01:t01`.
- `src/rs/vertebra-spanner/pyproject.toml` — Reference `maturin` `python-source`, `module-name`, and `exclude` globs [^spanner-pyproject].
- `src/py/vertebra/_spanner.pyi` — Stub docstring, metadata constants, and `__all__` conventions [^spanner-pyi].
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Optional namespace wheel isolation invariants.

## Symbol & Signature Contracts

- `src/rs/Cargo.toml`:
  - Append `"vertebra-oracle"` to `[workspace].members` (and resolve `src/rs/Cargo.lock` via `cargo check --manifest-path src/rs/Cargo.toml --workspace` without upgrading unrelated workspace crates).
- `src/rs/vertebra-oracle/pyproject.toml`:
  - `[build-system]`: `requires = ["maturin>=1.5,<2"]`, `build-backend = "maturin"`.
  - `[project]`: `name = "vertebra-oracle"`, `version = "0.3.0"`, `description = "Native Rust Oracle Thin 26ai client and pool extension for Vertebra."`, `requires-python = ">=3.12"`, `license = { text = "MIT" }`.
  - `[tool.maturin]`:
    - `python-source = "../../py"`
    - `module-name = "vertebra._oracle"`
    - `features = ["extension-module"]`
    - `exclude` containing `"**/py/vertebra/**/*.py"`, `"**/py/vertebra/**/*.so"`, `"**/py/vertebra/**/*.pyd"`, `"**/py/vertebra/py.typed"`, `"**/py/vertebra/_bigquery.pyi"`, `"**/py/vertebra/_bundler.pyi"`, `"**/py/vertebra/_spanner.pyi"`, `"**/py/vertebra/_storage.pyi"`, `"**/py/vertebra/_vertebra.pyi"`.
- `src/py/vertebra/_oracle.pyi` (incremental stub matching `src/lib.rs` at `ch01:t02`):
  - Module docstring `"""Type stubs for the vertebra._oracle native extension module."""`
  - `__version__: str`
  - `FREE_THREADED: bool`
  - `GIL_USED: bool`
  - `__all__ = ["FREE_THREADED", "GIL_USED", "__version__"]`

## Edge Cases & Error Semantics

- Resolve `src/rs/Cargo.lock` via `cargo check --manifest-path src/rs/Cargo.toml --workspace` rather than `cargo update` so existing workspace dependency versions stay pinned and `cargo clippy --locked` and `cargo test -p vertebra-oracle --locked` pass immediately.
- Declare only `__version__`, `FREE_THREADED`, and `GIL_USED` in `src/py/vertebra/_oracle.pyi` during `ch01:t02`; declaring classes not yet registered in `src/rs/vertebra-oracle/src/lib.rs` causes `mypy.stubtest vertebra._oracle` to fail.
- Ensure `src/rs/vertebra-oracle/pyproject.toml` excludes every `*.py`, `*.so`, `*.pyd`, `py.typed`, and sibling `*.pyi` file under `**/py/vertebra/` so the built wheel only ships `vertebra/_oracle.<abi>.*` and `vertebra/_oracle.pyi`.

## Public-Seam Test Blueprint

- **Canonical gate**: `tomllib` manifest verification, `--locked` Cargo check/Clippy/test, `maturin develop`, and `mypy.stubtest vertebra._oracle` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| Workspace member & wheel TOML | Parse `src/rs/Cargo.toml` and `src/rs/vertebra-oracle/pyproject.toml` with `tomllib` | `"vertebra-oracle"` in `workspace["members"]`; `pyproject["project"]["name"] == "vertebra-oracle"` and `pyproject["tool"]["maturin"]["module-name"] == "vertebra._oracle"`. |
| Locked Cargo check, Clippy & test | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked`, `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings`, `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked` | All three exit `0` with zero warnings. |
| Editable install & incremental stubtest parity | `uv run maturin develop --uv -m src/rs/vertebra-oracle/Cargo.toml && uv run --no-sync python -m mypy.stubtest vertebra._oracle` | Exits `0`; `vertebra._oracle.GIL_USED is False` and `vertebra._oracle.__version__ == "0.3.0"`. |

## Verification

Verify the manifests, locked Cargo gates, editable build, and `mypy.stubtest` exit `0`:

```bash
uv run --no-sync python -c '
import pathlib, tomllib
ws = tomllib.loads(pathlib.Path("src/rs/Cargo.toml").read_text())
assert "vertebra-oracle" in ws["workspace"]["members"]
pyp = tomllib.loads(pathlib.Path("src/rs/vertebra-oracle/pyproject.toml").read_text())
assert pyp["project"]["name"] == "vertebra-oracle"
assert pyp["tool"]["maturin"]["module-name"] == "vertebra._oracle"
'
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run maturin develop --uv -m src/rs/vertebra-oracle/Cargo.toml
uv run --no-sync python -m mypy.stubtest vertebra._oracle
```

Commit as `build(oracle): register workspace member, wheel manifest, and _oracle.pyi stub`.

## Acceptance Criteria

- [ ] `src/rs/Cargo.toml` includes `"vertebra-oracle"` in `[workspace].members` and `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` plus `cargo clippy -p vertebra-oracle --locked -- -D warnings` exit `0` (R1).
- [ ] `src/rs/vertebra-oracle/pyproject.toml` configures Maturin `module-name = "vertebra._oracle"` with namespace file exclusions matching sibling optional wheels (R1).
- [ ] `src/py/vertebra/_oracle.pyi` declares only `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool` matching `src/lib.rs`, and `uv run --no-sync python -m mypy.stubtest vertebra._oracle` exits `0` (R1).

## Notes & Discoveries

- [2026-11-09 01:15] Sibling `pyproject.toml` exclusions for `**/py/vertebra/_oracle.pyi` and `tools/scripts/verify_oracle_wheel.py` + `.github/workflows/release.yaml` are completed in `ch07:t02` once all public symbols are exported on `vertebra._oracle`.

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace members manifest.
[^spanner-pyproject]: `src/rs/vertebra-spanner/pyproject.toml` — Reference Maturin namespace wheel manifest and file exclusions.
[^spanner-pyi]: `src/py/vertebra/_spanner.pyi::SyncSpannerClient` — Reference native extension `.pyi` stub header.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Standalone `vertebra._oracle` wheel boundary and module exports.
