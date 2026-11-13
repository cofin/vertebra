---
type: Task
id: "native-redis-driver:ch01:t01"
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Scaffold vertebra-redis Crate, Maturin Manifest, and CRC16/Redirect Exports"
description: "Create src/rs/vertebra-redis with pure-Rust redis 1.8.1 + rustls, gil_used = false, and CRC16/redirect helpers."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-redis/Cargo.toml
  - src/rs/vertebra-redis/pyproject.toml
  - src/rs/vertebra-redis/src/lib.rs
tests: []
read_set:
  - src/rs/vertebra-mysql/Cargo.toml
  - src/rs/vertebra-postgres/pyproject.toml
  - src/rs/vertebra-mysql/src/lib.rs
estimated_loc: 185
tags:
  - redis
  - valkey
  - rust
  - pyo3
sources:
  - id: mysql-cargo
    resource: src/rs/vertebra-mysql/Cargo.toml
    title: "Pure-Rust async network PyO3 crate precedent"
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "Maturin standalone wheel isolation precedent"
  - id: mysql-lib
    resource: src/rs/vertebra-mysql/src/lib.rs
    title: "Async Tokio + PyO3 gil_used = false extension precedent"
---

# Task `t01`: Scaffold vertebra-redis Crate, Maturin Manifest, and CRC16/Redirect Exports

## Objective & Scope Boundary

Create `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}` over `redis = "=1.8.1"` + `rustls` per [`../spec.md` §2](../spec.md) [^mysql-cargo] [^pg-pyproj] [^mysql-lib].

## Bounded Read Manifest

- `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust Tokio + rustls PyO3 crate layout.
- `src/rs/vertebra-postgres/pyproject.toml` — Maturin `include` and sibling `.pyi` `exclude` globs.
- `src/rs/vertebra-mysql/src/lib.rs` — `#[pymodule(gil_used = false)]` initialization.

## Contracts & Edge Cases

- Initialize `rustls` `ring` provider idempotently and export `__version__`, `FREE_THREADED`, `GIL_USED = False`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.
- Include `vertebra/_redis.pyi` and exclude `vertebra/**/*.py` plus all sibling `_*.pyi` stubs in `vertebra-redis/pyproject.toml`.
- **Edge Case**: Hash-tag `{tag}` extraction in `redis_hash_slot` handles empty `{}` and multiple braces per Redis Cluster spec (`16384` slots).

## Test Matrix

- Rust unit tests verify `redis_hash_slot(b"123456789") == 0x31C3 % 16384`, `{user}:1` == `{user}:2`, and `MOVED`/`ASK` redirect parsing.

## Verification

```bash
cargo test --manifest-path src/rs/vertebra-redis/Cargo.toml || true
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-redis` compiles with `#[pymodule(gil_used = false)]` and zero OpenSSL dependencies.
- [ ] Zero regressions in the chapter fast verification gate.

[^mysql-cargo]: `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust async network PyO3 crate precedent.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^mysql-lib]: `src/rs/vertebra-mysql/src/lib.rs` — Async Tokio + PyO3 gil_used = false extension precedent.
