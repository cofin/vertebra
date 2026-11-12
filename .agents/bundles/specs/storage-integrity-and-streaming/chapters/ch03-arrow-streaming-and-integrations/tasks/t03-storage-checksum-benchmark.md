---
type: Task
id: "storage-integrity-and-streaming:ch03:t03"
chapter_id: ch03-arrow-streaming-and-integrations
title: Storage checksum Criterion benchmark
description: Add a vertebra-storage Criterion benchmark comparing default and checksum put throughput on the memory backend to prove the default path does no hashing.
status: draft
state: pending
priority: P2
verification_strategy: benchmark
depends_on:
  - "storage-integrity-and-streaming:ch02:t01"
read_set:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-storage/src/store.rs
files:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-storage/benches/put_checksum.rs
tests: []
estimated_loc: 80
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: core-cargo
    resource: src/rs/vertebra-core/Cargo.toml
    title: Criterion dev-dependency and bench stanza pattern
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: put_opts and PutConditionalOptions
tags:
  - benchmark
  - storage
---

# Task t03: Storage Checksum Criterion Benchmark

## Objective & Scope Boundary

1. `vertebra-storage/Cargo.toml`: add `criterion = { workspace = true }` to `[dev-dependencies]` (as in `vertebra-core/Cargo.toml`) and `[[bench]] name = "put_checksum"`, `harness = false` [^core-cargo].
2. `benches/put_checksum.rs`: build `ObjectStoreEngine::new(BackendConfig::Memory)`, a Tokio current-thread runtime, and Criterion groups `put_memory_default` and `put_memory_checksum` with `Throughput::Bytes` for 1 MiB and 64 MiB payloads calling `put_opts` with `PutConditionalOptions::default()` and `PutConditionalOptions { checksum: true, ..Default::default() }` [^store-rs].

## Bounded Read Manifest

- Both `Cargo.toml` files, `store.rs::put_opts`.

## Symbol & Signature Contracts

- Bench IDs: `put_memory_default/1MiB`, `put_memory_default/64MiB`, `put_memory_checksum/1MiB`, `put_memory_checksum/64MiB`.

## Edge Cases & Error Semantics

- `vertebra-storage` is a cdylib with an rlib target used by integration tests; the bench uses the same `_storage::store` path as `storage_engine_tests.rs`.

## Public-Seam Test Blueprint

- **Canonical gate**: `cargo bench -p vertebra-storage --bench put_checksum -- --quick`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Bench runs | `--quick` | exit `0`; four bench IDs reported |
| Default cost | compare `put_memory_default/64MiB` against `put_memory_checksum/64MiB` | default strictly faster; checksum delta reported in the PR body |

## Verification

- `cd src/rs && cargo bench -p vertebra-storage --bench put_checksum -- --quick`
- `cd src/rs && cargo clippy -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Benchmark demonstrates the default path does no hashing work (R4).

## Notes & Discoveries

- [2026-11-12 01:30] `vertebra-storage` has no `benches/` directory at HEAD; `make rs-bench` covers core and http only.

[^core-cargo]: `src/rs/vertebra-core/Cargo.toml` — Criterion pattern.
[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `put_opts`.
