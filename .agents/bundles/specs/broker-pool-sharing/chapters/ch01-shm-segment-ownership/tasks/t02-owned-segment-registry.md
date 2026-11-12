---
type: Task
id: "broker-pool-sharing:ch01:t02"
chapter_id: ch01-shm-segment-ownership
title: Owned-segment registry, signal cleanup, and owner-aware reaper
description: Replace the empty cleanup_all with an async-signal-safe owned-name registry registered on create and unregistered on drop, call it from the panic hook and SIGTERM/SIGINT, and make startup reaping owner-PID-aware.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch01:t01"
read_set:
  - src/rs/vertebra-core/src/cleanup.rs
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-http/src/main.rs
  - src/rs/vertebra-core/tests/it/shm.rs
files:
  - src/rs/vertebra-core/src/cleanup.rs
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/rs/vertebra-core/tests/it/shm.rs
tests:
  - src/rs/vertebra-core/tests/it/shm.rs
estimated_loc: 200
plan_revision: 3
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
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: cleanup-all
    resource: src/rs/vertebra-core/src/cleanup.rs::cleanup_all
    title: Empty cleanup function
  - id: stale-reaper
    resource: src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm
    title: Pattern-based stale SHM reaper and panic hook installation
tags:
  - shm
  - cleanup
  - signals
---

# Task t02: Owned-Segment Registry, Signal Cleanup, and Owner-Aware Reaper

## Objective & Scope Boundary

Implement [#96](https://github.com/cofin/vertebra/issues/96):

1. In `src/rs/vertebra-core/src/cleanup.rs` [^cleanup-all], add a fixed table `static OWNED: [OwnedSlot; 256]` where each slot holds `AtomicU8` state and a `[u8; 64]` NUL-terminated name; `register_owned` claims a slot with CAS, `unregister_owned` clears it; `cleanup_all` iterates slots and calls `libc::shm_unlink` (async-signal-safe, no allocation).
2. `install_signal_cleanup()` installs `SIGTERM`/`SIGINT` handlers via `sigaction` that call `cleanup_all` then re-raise with the default disposition.
3. `reap_dead_owner_segments(prefixes)` lists `/dev/shm` (Linux; no-op returning `0` on macOS), opens each matching name read-only, reads `ShmOwnerHeader`, and unlinks only when `owner_pid` is dead.
4. `ShmRegion::open_or_create_named` (from `t01`) calls `register_owned` on `Created`; `ShmRegion::drop` for owners calls `unregister_owned` before unlink (edit through the `shm.rs` hook added in `t01`; the hook is a single call line).
5. In `src/rs/vertebra-http/src/workers/factory.rs` [^stale-reaper], `cleanup_stale_shm` delegates to `reap_dead_owner_segments(&["vertebra-", "vtb-"])`; the panic-hook installer also calls `install_signal_cleanup`.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/cleanup.rs` — `cleanup_all`.
- `src/rs/vertebra-core/src/shm.rs` — `open_or_create_named`, `Drop for ShmRegion`.
- `src/rs/vertebra-http/src/workers/factory.rs` — `cleanup_stale_shm`, panic hook installer.
- `src/rs/vertebra-http/src/main.rs` — startup call site of `cleanup_stale_shm`.
- `src/rs/vertebra-core/tests/it/shm.rs`.

## Symbol & Signature Contracts

- `pub const OWNED_CAPACITY: usize = 256;`
- `pub fn register_owned(name: &str) -> bool` (returns `false` when full or name longer than 63 bytes).
- `pub fn unregister_owned(name: &str)`.
- `pub fn cleanup_all()`.
- `pub fn install_signal_cleanup() -> std::io::Result<()>`.
- `pub fn reap_dead_owner_segments(prefixes: &[&str]) -> u32` (count unlinked).

## Edge Cases & Error Semantics

- Registry full: `register_owned` returns `false` and emits one `tracing::warn!`; the region still works and is unlinked on drop.
- Segment without owner header (legacy layout): reaper skips it.
- Header read fails (permissions): skip, never unlink.
- Signal during `register_owned`: slot state `CLAIMING` is skipped by `cleanup_all`.
- Re-raised signal keeps the conventional exit status (`128 + signo`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-core/tests/it/shm.rs`. Dependency category: local OS resource (Linux-gated `#[cfg(target_os = "linux")]` for `/dev/shm` listing).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Panic cleanup | Forked child creates `/vtb-it-panic-<pid>` via `open_or_create_named`, sets a panic hook calling `cleanup_all`, panics | `/dev/shm/vtb-it-panic-<pid>` absent after `waitpid` |
| SIGTERM cleanup | Child creates segment, `install_signal_cleanup()`, parent sends `SIGTERM` | Segment absent; child exit status signal `SIGTERM` |
| Live peer survives | Parent holds `/vtb-it-peer-<pid>`; `reap_dead_owner_segments(&["vtb-it-peer"])` | Returns `0`; segment present |
| SIGKILL orphan | Child creates `/vtb-it-orphan-<pid>`, parent `SIGKILL`s it, then reaps | Returns `1`; segment absent |
| Registry capacity | Register 257 distinct names | 257th call returns `false` |

## Verification

- Red/Green: `rm -f /dev/shm/vtb-it-* && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it shm::`.
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,compression,gcp workers::` exits `0`.
- Lint: `cargo clippy --manifest-path src/rs/Cargo.toml --workspace --features tls,compression,gcp -- -D warnings`.

## Acceptance Criteria

- [ ] `cleanup_all` unlinks every registered owned name (R2).
- [ ] Panic and SIGTERM in an owning process leave no owned `/dev/shm` entries (R2).
- [ ] Startup reaping removes dead-owner segments and keeps live peers (R2).

## Notes & Discoveries

- [2026-11-12 01:40] `cleanup_all` is `pub fn cleanup_all() {}` at HEAD; `factory.rs` still installs a panic hook calling it.

[^cleanup-all]: `src/rs/vertebra-core/src/cleanup.rs::cleanup_all` — Empty cleanup function.
[^stale-reaper]: `src/rs/vertebra-http/src/workers/factory.rs::cleanup_stale_shm` — Stale reaper and panic hook.
