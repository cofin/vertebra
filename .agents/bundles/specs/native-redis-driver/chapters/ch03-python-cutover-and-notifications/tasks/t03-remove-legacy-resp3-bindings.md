---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t03
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Remove Legacy RESP3 Bindings from vertebra-py and Delete vertebra-db/src/resp3.rs
description: Delete resp3_*, redis_*, crc16_xmodem, and redis_hash_slot from vertebra-py/_vertebra.pyi and remove src/rs/vertebra-db/src/resp3.rs.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch03-python-cutover-and-notifications:t02
files:
- src/rs/vertebra-py/src/db.rs
- src/py/vertebra/_vertebra.pyi
- src/rs/vertebra-db/src/resp3/mod.rs
- src/rs/vertebra-db/src/lib.rs
tests:
- src/py/tests/unit/db/test_redis_valkey_resp3.py
- src/rs/vertebra-redis/tests/redis_client.rs
read_set:
- src/rs/vertebra-py/src/db.rs
- src/py/vertebra/_vertebra.pyi
- src/rs/vertebra-db/src/resp3/mod.rs
- src/rs/vertebra-db/src/lib.rs
estimated_loc: 220
tags:
- pyo3
- resp3
- debloat
- rust
sources:
- id: resp3-bindings
  resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
  title: Legacy RESP3 helper bindings on vertebra._vertebra
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Core vertebra._vertebra stub declarations
- id: resp3-rs
  resource: src/rs/vertebra-db/src/resp3/mod.rs
  title: Legacy RESP3 codec module in vertebra-db
- id: db-lib
  resource: src/rs/vertebra-db/src/lib.rs
  title: vertebra-db crate root re-exporting resp3
---

# Task `t03`: Remove Legacy RESP3 Bindings from vertebra-py and Delete vertebra-db/src/resp3.rs

## Objective & Scope Boundary

Remove all 5 RESP3 bindings from `src/rs/vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi` per [`../spec.md` §2](../spec.md) [^resp3-bindings] [^vertebra-pyi].

Delete `src/rs/vertebra-db/src/resp3/mod.rs` and remove its exports from `src/rs/vertebra-db/src/lib.rs` per [`../spec.md` §2](../spec.md) [^resp3-rs] [^db-lib].

## Bounded Read Manifest

- `src/rs/vertebra-py/src/db.rs` — RESP3 imports, `#[pyfunction]` wrappers, and module registrations.
- `src/py/vertebra/_vertebra.pyi` — RESP3 stub declarations.

- `src/rs/vertebra-db/src/resp3/mod.rs` — 475-line legacy RESP3 parser/encoder to delete.
- `src/rs/vertebra-db/src/lib.rs` — `pub mod resp3;` and `pub use resp3::{...};`.

## Contracts & Edge Cases

- Keep `vertebra._redis` as the sole owner of `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.
- **Edge Case**: `mypy.stubtest vertebra._vertebra` must pass immediately after rebuild.

- `vertebra-db` compiles cleanly across the workspace with zero unused dependencies or dead code warnings.
- **Edge Case**: Verify no Rust crate in `src/rs/` references `vertebra_db::resp3`.

## Test Matrix

- Assert `not hasattr(vertebra._vertebra, "encode_resp_command")` and run `mypy.stubtest vertebra._vertebra`.

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-redis` passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-redis
```

## Acceptance Criteria

- [ ] `vertebra._vertebra` exports zero RESP3 functions and passes `stubtest`.
- [ ] `src/rs/vertebra-db/src/resp3/mod.rs` is deleted and workspace Cargo tests pass.
- [ ] Zero regressions in the fast verification gate.

[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate root re-exporting resp3.
