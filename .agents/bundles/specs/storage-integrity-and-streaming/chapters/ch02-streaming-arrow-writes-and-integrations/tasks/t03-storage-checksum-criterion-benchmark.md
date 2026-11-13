---
type: Task
id: "storage-integrity-and-streaming:ch02:t03"
flow_id: storage-integrity-and-streaming
chapter_id: ch02-streaming-arrow-writes-and-integrations
title: "Add Criterion put_checksum Benchmark for vertebra-storage"
description: "Add benches/put_checksum.rs to vertebra-storage comparing default vs checksum 1 MiB and 64 MiB memory writes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch01:t01"
files:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-storage/benches/put_checksum.rs
tests: []
read_set:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-storage/src/store.rs
estimated_loc: 85
tags:
  - storage
  - benchmark
  - rust
  - performance
sources:
  - id: core-cargo
    resource: src/rs/vertebra-core/Cargo.toml
    title: "Workspace Criterion dev-dependency and [[bench]] stanza pattern"
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: "ObjectStoreEngine::put_opts and PutConditionalOptions"
---

# Task `t03`: Add Criterion `put_checksum` Benchmark for `vertebra-storage`

## Objective & Scope Boundary

Register `criterion = { workspace = true }` and `[[bench]] name = "put_checksum"` (`harness = false`) in `src/rs/vertebra-storage/Cargo.toml` and create `src/rs/vertebra-storage/benches/put_checksum.rs` per [`../spec.md` §2](../spec.md) [^core-cargo] [^store-rs].

## Bounded Read Manifest

- `src/rs/vertebra-storage/Cargo.toml` — `[dev-dependencies]` and `[lib] name = "_storage"`.
- `src/rs/vertebra-core/Cargo.toml` — workspace Criterion benchmark configuration [^core-cargo].
- `src/rs/vertebra-storage/src/store.rs` — `BackendConfig::Memory`, `ObjectStoreEngine::put_opts`, and `PutConditionalOptions` [^store-rs].

## Contracts & Edge Cases

- Import `_storage::store::{BackendConfig, ObjectStoreEngine, PutConditionalOptions}` (matching `vertebra-storage`'s `[lib] name = "_storage"`).
- Benchmark IDs: `put_memory_default/1MiB`, `put_memory_default/64MiB`, `put_memory_checksum/1MiB`, and `put_memory_checksum/64MiB` with `Throughput::Bytes`, comparing `PutConditionalOptions::default()` (`checksum: false`) against `PutConditionalOptions { checksum: true, ..Default::default() }` over a reused `Bytes` payload so zero-cost default writes are directly measurable.

## Test Matrix

- Compile and smoke-run the benchmark harness via `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --benches` (fast `< 15s` gate) and verify Clippy `--all-targets` cleanliness.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --benches && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings
```

## Acceptance Criteria

- [ ] `benches/put_checksum.rs` compiles cleanly with zero Clippy warnings and reports separate `1MiB` and `64MiB` throughput for default vs checksum puts (`R2`).
- [ ] Zero regressions in the chapter's fast verification gate.

[^core-cargo]: `src/rs/vertebra-core/Cargo.toml` — Workspace Criterion dev-dependency and `[[bench]]` stanza pattern.
[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine::put_opts` and `PutConditionalOptions`.
