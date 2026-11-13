---
type: Task
id: "native-redis-driver:ch06:t02"
flow_id: native-redis-driver
chapter_id: ch06-redis-notifications-and-resp3-cleanup
title: "Delete src/rs/vertebra-db/src/resp3/mod.rs from vertebra-db"
description: "Delete legacy resp3 codec module and remove pub mod resp3 re-exports from vertebra-db/src/lib.rs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch06:t01"
files:
  - src/rs/vertebra-db/src/resp3/mod.rs
  - src/rs/vertebra-db/src/lib.rs
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
read_set:
  - src/rs/vertebra-db/src/resp3/mod.rs
  - src/rs/vertebra-db/src/lib.rs
estimated_loc: 60
tags:
  - rust
  - resp3
  - debloat
sources:
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: "Legacy RESP3 codec module in vertebra-db"
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: "vertebra-db crate root re-exporting resp3"
---

# Task `t02`: Delete src/rs/vertebra-db/src/resp3/mod.rs from vertebra-db

## Objective & Scope Boundary

Delete `src/rs/vertebra-db/src/resp3/mod.rs` and remove its exports from `src/rs/vertebra-db/src/lib.rs` per [`../spec.md` §2](../spec.md) [^resp3-rs] [^db-lib].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/resp3/mod.rs` — 475-line legacy RESP3 parser/encoder to delete.
- `src/rs/vertebra-db/src/lib.rs` — `pub mod resp3;` and `pub use resp3::{...};`.

## Contracts & Edge Cases

- `vertebra-db` compiles cleanly across the workspace with zero unused dependencies or dead code warnings.
- **Edge Case**: Verify no Rust crate in `src/rs/` references `vertebra_db::resp3`.

## Test Matrix

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-redis` passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py -p vertebra-redis
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-db/src/resp3/mod.rs` is deleted and workspace Cargo tests pass.
- [ ] Zero regressions in the chapter fast verification gate.

[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate root re-exporting resp3.
