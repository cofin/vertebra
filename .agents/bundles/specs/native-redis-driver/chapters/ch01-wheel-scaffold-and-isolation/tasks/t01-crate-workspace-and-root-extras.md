---
type: Task
id: native-redis-driver:ch01-wheel-scaffold-and-isolation:t01
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Scaffold vertebra-redis Crate and Wire Cargo Workspace + [redis]/[valkey] Extras
description: Create src/rs/vertebra-redis with pure-Rust redis 1.8.1 + rustls and gil_used = false, and add vertebra-redis to Cargo workspace and root pyproject.toml [redis], [valkey], uv.sources, maturin.exclude, and bumpversion.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 1b191388
depends_on: []
files:
- src/rs/vertebra-redis/Cargo.toml
- src/rs/vertebra-redis/pyproject.toml
- src/rs/vertebra-redis/src/lib.rs
- src/rs/Cargo.toml
- src/rs/Cargo.lock
- pyproject.toml
tests: []
read_set:
- src/rs/vertebra-mysql/Cargo.toml
- src/rs/vertebra-postgres/pyproject.toml
- src/rs/vertebra-mysql/src/lib.rs
- src/rs/Cargo.toml
- pyproject.toml
estimated_loc: 220
tags:
- redis
- valkey
- rust
- pyo3
- packaging
sources:
- id: mysql-cargo
  resource: src/rs/vertebra-mysql/Cargo.toml
  title: Pure-Rust async network PyO3 crate precedent
- id: pg-pyproj
  resource: src/rs/vertebra-postgres/pyproject.toml
  title: Maturin standalone wheel isolation precedent
- id: mysql-lib
  resource: src/rs/vertebra-mysql/src/lib.rs
  title: Async Tokio + PyO3 gil_used = false extension precedent
- id: cargo-ws
  resource: src/rs/Cargo.toml
  title: Rust workspace member manifest
- id: root-pyproject
  resource: pyproject.toml
  title: Root vertebra-core extras, uv sources, maturin excludes, and bumpversion
---

# Task `t01`: Scaffold vertebra-redis Crate and Wire Cargo Workspace + [redis]/[valkey] Extras

## Objective & Scope Boundary

Create `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}` over `redis = "=1.8.1"` + `rustls` per [`../spec.md` §2](../spec.md) [^mysql-cargo] [^pg-pyproj] [^mysql-lib].

Register `vertebra-redis` in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` per [`../spec.md` §2](../spec.md) [^cargo-ws] [^root-pyproject].

## Bounded Read Manifest

- `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust Tokio + rustls PyO3 crate layout.
- `src/rs/vertebra-postgres/pyproject.toml` — Maturin `include` and sibling `.pyi` `exclude` globs.
- `src/rs/vertebra-mysql/src/lib.rs` — `#[pymodule(gil_used = false)]` initialization.

- `src/rs/Cargo.toml` — Workspace members list.
- `pyproject.toml` — Extras, `uv.sources`, `maturin.exclude`, and `bumpversion`.

## Contracts & Edge Cases

- Initialize `rustls` `ring` provider idempotently and export `__version__`, `FREE_THREADED`, `GIL_USED = False`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.
- Include `vertebra/_redis.pyi` and exclude `vertebra/**/*.py` plus all sibling `_*.pyi` stubs in `vertebra-redis/pyproject.toml`.
- **Edge Case**: Hash-tag `{tag}` extraction in `redis_hash_slot` handles empty `{}` and multiple braces per Redis Cluster spec (`16384` slots).

- Add both `redis = ["vertebra-redis==0.3.0"]` and `valkey = ["vertebra-redis==0.3.0"]` in `[project.optional-dependencies]` (without adding an `[all]` extra, which `test_optional_driver_wheels.py` forbids).
- Exclude `src/py/vertebra/_redis.pyi` from the root `vertebra-core` wheel and add `bumpversion` entries.
- **Edge Case**: Both `[redis]` and `[valkey]` extras resolve to the single `vertebra-redis` workspace package in `[tool.uv.sources]`.

## Test Matrix

- Rust unit tests verify `redis_hash_slot(b"123456789") == 0x31C3 % 16384`, `{user}:1` == `{user}:2`, and `MOVED`/`ASK` redirect parsing.

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis` passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-redis` compiles with `#[pymodule(gil_used = false)]` and zero OpenSSL dependencies.
- [ ] `vertebra-core[redis]` and `vertebra-core[valkey]` both resolve to `vertebra-redis`.
- [ ] Zero regressions in the fast verification gate.

[^mysql-cargo]: `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust async network PyO3 crate precedent.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^mysql-lib]: `src/rs/vertebra-mysql/src/lib.rs` — Async Tokio + PyO3 gil_used = false extension precedent.
[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
