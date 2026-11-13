---
type: Task
id: "server-runtime-correctness:ch06:t01"
flow_id: server-runtime-correctness
chapter_id: ch06-runtime-native-cleanup
title: Enforce Fail-Loud getrandom Entropy and Post-Fork Reseed in id.rs
description: "Replace time-seeded fallback in vertebra-core/src/id.rs with getrandom::fill panic-on-failure and reseed thread entropy pools on fork()."
status: stable
state: completed
commit: db02a0b6
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/Cargo.toml
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-core/src/id.rs
tests:
  - src/rs/vertebra-core/src/id.rs
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-core/src/id.rs
estimated_loc: 110
tags:
  - rust
  - entropy
  - uuid
  - fork-safety
sources:
  - id: id-rs
    resource: "src/rs/vertebra-core/src/id.rs::fill_os_random"
    title: Current UUID entropy pool and splitmix fallback
---

# Task `t01`: Enforce Fail-Loud getrandom Entropy and Post-Fork Reseed in id.rs

## Objective & Scope Boundary

Replace the predictable `SystemTime` fallback in `src/rs/vertebra-core/src/id.rs` with `getrandom::fill` and `pthread_atfork` reseeding per [`../spec.md` §2](../spec.md) [^id-rs].

## Bounded Read Manifest

- `src/rs/Cargo.toml` & `src/rs/vertebra-core/Cargo.toml` — workspace `getrandom = '0.3'` dependency.
- `src/rs/vertebra-core/src/id.rs` — `EntropyPool` and `fill_os_random`.

## Contracts & Edge Cases

- `fill_os_random` calls `getrandom::fill(dest)` and panics with `'vertebra-core: OS entropy source unavailable: ...'` on error; zero `SystemTime` fallback code remains.
- Register a `std::sync::Once`-guarded `libc::pthread_atfork(None, None, Some(on_child_fork))` on Unix incrementing `FORK_GENERATION: AtomicU64`; `EntropyPool` tracks `generation: u64` and refills when `generation != FORK_GENERATION.load(Ordering::Relaxed)`.

## Test Matrix

- Rust unit tests in `id.rs` verifying UUIDv4/UUIDv7/nanoid uniqueness and a Unix `libc::fork()` test drawing from a partially consumed pool in parent and child and asserting zero colliding outputs.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib id:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -- -D warnings
```

## Acceptance Criteria

- [ ] `id.rs` uses `getrandom::fill` exclusively with zero time-seeded fallback.
- [ ] Forked Unix child processes reseed partially consumed thread entropy pools.

[^id-rs]: `src/rs/vertebra-core/src/id.rs::fill_os_random` — Current UUID entropy pool and splitmix fallback.
