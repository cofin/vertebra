---
type: Spec
flow_id: core-ipc-consolidation
title: "Chapter 2: Rust core, IPC rings, and storage consolidation"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T03:30:00Z
description: "Fix lock-free ring and SHM frame correctness bugs, remove dead core modules and the unused vertebra-db crate, split channel.rs into ring/{mod,spsc,mpmc,slot}.rs with shared park/publish helpers, unify the response wire format on ResponseSlot + RawHeaders, eliminate BatchBridge lock-across-send contention, and deduplicate vertebra-storage."
tags: [refactor, rust, ipc, correctness]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 2: Rust core, IPC rings, and storage consolidation

**Flow ID:** `core-ipc-consolidation`

## Specification

### Parent Context

This chapter is Chapter 2 of the [`vertebra-design-quality`](../vertebra-design-quality/spec.md) master roadmap. It depends on Chapter 1 (`review-infrastructure`) so `make review-check` and `make bench-check BASELINE=pre-refactor` are active before any Rust IPC or storage code changes. It owns roadmap requirements:

- **R4 (Lock-free ring and frame correctness):** Fix `MpmcRing::try_recv` slot wedging on `BufferTooSmall` (`CORE-01`), `MpmcRing::try_send_with` / `SpscRing::try_send_with` length bounds and unwind poisoning (`CORE-02`), SHM zero-copy read bounds validation (`OKF-02`), `select_ring` payload capacity comparison (`OKF-03`), `headers_len > u16::MAX` rejection (`CORE-04`/`OKF-04`), and `BatchSnapshot::deserialize_from` node offset bounds validation (`CORE-20`).
- **R5 (Core and workspace debloat):** Delete dead `vertebra-core` modules (`protocol/state.rs`, `cleanup.rs`, `router::Router`, `BufferPool`, `BufferChannel`, `SharedMutex`/`SharedRwLock`/`SharedCondvar`/`SpinLock`/`FileLock`, `Opcode`, dead protocol header structs) and unused dependencies (`rmp-serde`, `rustix`, `matchit`, `tokio-test`), and remove the unused `src/rs/vertebra-db` crate (`CORE-15`, `CORE-44`). Keep `guards.rs`, `logger.rs`, and `telemetry.rs` for ingress pre-dispatch guard offload (Task 3.6) and PyO3 Litestar/native boosting bindings (Task 4.10, Task 5.6) per Decision D4. Retain `WaitWord` in `src/rs/vertebra-core/src/platform/mod.rs` (moved from `sync.rs`) and retain `ShmRegion::create_named` and `ShmRegion::open_named` in `src/rs/vertebra-core/src/shm.rs` for cross-worker SHM lease tables (`vertebra-shm-connection-broker`).
- **R6 (Single response wire format and modular rings):** Split `channel.rs` (2,374 lines) into `ring/{mod,spsc,mpmc,slot}.rs` with shared `park_until` and `publish_*` helpers (`CORE-07..09`), unify on `ResponseSlot` + `ResponseSlotView` + `RawHeaders` (`CORE-03`, `CORE-17..19`, `CORE-42`, `HTTP-09`), collapse 4-tier ring fields into `[SpscRing; 4]` with round-robin fair drain (`CORE-21..26`), make `BatchBridge` non-blocking under `write_lock` with lock-free response receiver handoff (`CORE-27..29`), make macOS `ulock` default (`CORE-30..32`), and fix `vertebra-storage` default timestamps and sync/async method duplication (`CORE-36..38`).

### Code Analysis Summary

Inspected at commit `edceea6`:

- **`src/rs/vertebra-core/src/channel.rs` (2,374 lines):**
  - `MpmcRing::try_recv` (`:1563`) and `try_recv_with_flags` (`:1596`) return `Err(ChannelError::BufferTooSmall)` after CAS-claiming `head` without storing `head + mask + 1` to the slot sequence, permanently wedging the ring (`CORE-01`).
  - `MpmcRing::try_send_with` (`:1506-1520`) neither bounds-checks `written <= self.payload_capacity()` nor releases the sequence if `f` unwinds (`CORE-02`).
  - `SpscRing::read_slot_zero_copy` (`:1005-1014`) and `MpmcRing::read_slot_zero_copy` (`:1666-1673`) construct `std::slice::from_raw_parts(payload_ptr, len)` from SHM `(*header).len` without clamping or validating `len <= self.payload_capacity()` (`OKF-02`).
  - Futex park/re-check loop is copy-pasted 10 times (`:346-823`, `CORE-07`); 11 receive/send variants have zero external callers (`CORE-08`); `SlotView` `slot_borrowed` is ignored by copy-receive paths (`CORE-09`); `BufferChannel` (`:1017-1271`) has zero external callers (`CORE-12`).
- **`src/rs/vertebra-core/src/transport/ring_pair.rs` (865 lines) & `ipc.rs` (222 lines):**
  - Two incompatible response wire formats share `TieredRingSet`: VRES `ResponseData` (`ring_pair.rs:31-77`) and `ResponseSlot` (`ipc.rs:24-170`). `vertebra-http/src/response_reader.rs:271` only calls `try_recv_response()`, which rejects `ResponseSlot` frames with `"bad magic"` (`CORE-03`).
  - `write_response_slot` (`ring_pair.rs:191`) casts `headers_raw.len() as u16` without checking `<= u16::MAX` (`CORE-04` / `OKF-04`).
  - `TieredRingSet::select_ring` (`ring_pair.rs:269-281`) compares `total_size <= tier.slot_size` instead of `ring.payload_capacity()` (`slot_size - 8`), causing payloads within 8 bytes of a tier boundary to fail with `PayloadTooLarge` (`OKF-03`).
  - `try_recv_response` (`ring_pair.rs:255-266`) always scans `small -> medium -> large -> xlarge`, starving larger tiers under sustained small-response load (`CORE-26`).
- **`src/rs/vertebra-core/src/transport/mod.rs` (1,471 lines):**
  - `BatchSnapshot::deserialize_from` (`:786-819`) never validates `node_count * size_of::<TransportNode>()` overflow or node `(offset, len)` ranges against `slab_len` (`CORE-20`).
  - `encode_headers_compact` (`:931-955`) silently truncates header names `> 255` bytes and values `> 65535` bytes (`CORE-42`), and `vertebra-http/src/dispatcher/shm.rs:226` duplicates the encoder loop (`HTTP-09`).
- **`src/rs/vertebra-core/src/bridge.rs` (700 lines):**
  - `BatchWriter::seal` (`:195-227`) calls blocking `self.batch_tx.send(snapshot)` while holding `write_lock: Mutex<()>`, stalling Tokio worker threads when `batch_tx` is full (`CORE-27`). `BridgeResponse` allocates `Vec<(Vec<u8>, Vec<u8>)>` per response and wraps single-consumer `resp_rx` in `tokio::sync::Mutex` (`CORE-29`).
- **`src/rs/vertebra-db/` (523 lines) & `src/rs/vertebra-storage/` (2,397 lines):**
  - `vertebra-db` has zero dependents and no PyO3 module, duplicating `src/py/vertebra/db/` (`CORE-15`).
  - `vertebra-storage/src/lib.rs:447,479,534,570,610` defaults `timestamp=1700000000` (Nov 2023) on all 5 signing pyfunctions (`CORE-37`) and hand-writes 14 sync/async method pairs (`CORE-38`).

#### Findings not carried in this chapter

- **`CORE-10` (MPMC futex parking in `poll_batch`) & `CORE-41` (conditional `SeqCst` RMW on SPSC wait words with loom proof):** Carried in Chapter 8 Task 8.2 after the `ring/` split (`Task 2.4`) establishes the shared `park_until` and `publish_*` helpers.
- **`CORE-13` (`logger.rs` and `telemetry.rs` subset) & `CORE-34` (`sanitize_sql_statement` UTF-8/`JOIN (`/`VALUES` fix):** Per Decision D4, `logger.rs` and `telemetry.rs` are kept in `vertebra-core` and bound into `vertebra-py` in Chapter 4 Task 4.10.
- **`CORE-40` (new `benches/mpmc_contention.rs` and `benches/bridge.rs` and `response_slot` bench group):** Carried in Chapter 8 Task 8.1 (`Makefile` `rs-bench` target is in Chapter 1 Task 1.6; dead bench deletions `router.rs`, `guards.rs`, `mpmc.rs` and merging `spin_wait.rs` into `spsc.rs` are in Task 2.2).

### Relevant Patterns

- [Lock-Free SHM and Sharding](../../knowledge/patterns/rust/lock-free-shm-and-sharding.md) — `Release` cursor publish, `SeqCst` wait-word RMW and waiter count load before `futex_wake`, and `#[repr(C, align(64))]` cache-line isolation.
- [SHM and Cross-Platform IPC Gotchas](../../knowledge/gotchas/shm-and-cross-platform-ipc.md) — macOS `shm_open` 31-byte name limit, `__ulock_wait` `ULF_NO_ERRNO` `rc >= 0` contract, and slot bounds checks.
- [Ring Buffers and SHM](../../knowledge/architecture/ipc/ring-buffers-and-shm.md) — SPSC and MPMC slot layouts and sequence protocols.

### Requirements

| ID | Requirement |
| --- | --- |
| C2-R1 | Fix `MpmcRing` and `SpscRing` slot safety defects (`CORE-01`, `CORE-02`, `OKF-02`, `OKF-03`, `CORE-04`/`OKF-04`, `CORE-20` offset validation) with focused regression tests. |
| C2-R2 | Delete dead `vertebra-core` modules (`guards.rs`, `protocol/state.rs`, `cleanup.rs`, `router::Router`, `BufferPool`, `BufferChannel`, `SharedMutex`/`SharedRwLock`/`SharedCondvar`/`FileLock`, `Opcode`, dead protocol headers) and unused dependencies (`CORE-05`, `CORE-06`, `CORE-11..14`, `CORE-16`, `CORE-31`, `CORE-33`, `CORE-35`, `CORE-39`, `CORE-40`, `CORE-43`). |
| C2-R3 | Remove the unused `src/rs/vertebra-db` crate and consolidate workspace dependencies (`CORE-15`, `CORE-44`). |
| C2-R4 | Split `channel.rs` into `ring/{mod,spsc,mpmc,slot}.rs` and `buffer.rs` into `layout.rs` with shared `park_until`, `publish_write`, and `publish_read` helpers and pruned receive variants (`CORE-07..09`). |
| C2-R5 | Unify response framing on `ResponseSlot` + `ResponseSlotView<'a>` + `RawHeaders<'a>` (`protocol/{flags,response}.rs`, `transport/headers.rs`), deleting VRES `ResponseData` and updating `vertebra-http` and `vertebra-py` (`CORE-03`, `CORE-17..19`, `CORE-42`, `HTTP-09`). |
| C2-R6 | Split batch transport into `transport/batch.rs` (`BatchHeader`, `BatchBufferPool`, single constructor, `From<RingPairError>`) and fold `tiered_ring.rs` into `transport/ring_pair.rs` with `[Arc<SpscRing>; 4]` and round-robin fair drain (`CORE-21..26`). |
| C2-R7 | Make `BatchBridge::seal` non-blocking under `write_lock`, own `DataSlab`/`TransportArena` by `&mut` inside `BatchWriter`, store `headers_raw: Bytes` on `BridgeResponse`, and hand off `resp_rx` without a per-poll Tokio `Mutex` (`CORE-27..29`). |
| C2-R8 | Deduplicate `ShmRegion` mmap construction (`map_fd_region`) and make macOS `ulock` always-on without a `macos-ulock` Cargo feature (`CORE-30`, `CORE-32`). |
| C2-R9 | Deduplicate crypto helpers (`constant_time_eq`, `sha256_hex`), fix `vertebra-storage` signing pyfunctions to default `timestamp: Option<i64> = None` to `Utc::now().timestamp()`, and replace hand-written sync/async pairs with a macro + zero-copy `Bytes::from_owner` (`CORE-36..38`). |

### Target Layout

```text
src/rs/
  Cargo.toml                           workspace members without vertebra-db; consolidated [workspace.dependencies]
  vertebra-core/
    Cargo.toml                         trimmed dependencies; macos-ulock feature removed
    src/
      lib.rs                           module exports only; no unused root re-exports or VERSION
      layout.rs                        renamed from buffer.rs: ShmHeader, CacheLine, SlotHeader, RingLayout, ChannelMode
      shm.rs                           ShmRegion::{create, map} + shared map_fd_region
      notify.rs                        Notifier without bench-only spin helpers or redundant unsafe impls
      guards.rs                        JwtValidator, RateLimiter, GuardSpec (retained for Task 3.6 ingress & Task 4.10/5.6 PyO3 guard offload)
      logger.rs                        AsyncLogCore (retained for Chapter 4 Task 4.10 PyO3 binding)
      telemetry.rs                     W3cTraceContext, SpanRecord, sanitize_sql_statement (retained for Chapter 4 Task 4.10)
      util.rs                          constant_time_eq and sha256_hex shared across storage and http
      platform/
        mod.rs                         futex facade (wait_on_address[_timeout], wake_one/all, SyncError, sleep fallback)
        linux.rs  macos.rs  windows.rs ulock default on macOS
        process.rs                     moved from src/process.rs
      ring/
        mod.rs                         ChannelError, shared park_until and publish helpers
        spsc.rs                        SpscRing
        mpmc.rs                        MpmcRing
        slot.rs                        SlotView
      protocol/
        mod.rs                         re-exports flags, codec, response
        flags.rs                       RequestFlags(u32) + FrameFlags(u16)
        codec.rs                       CompressionCodec (lz4/zstd)
        response.rs                    ResponseSlot + borrowed ResponseSlotView<'a>
      transport/
        mod.rs                         Transport, Backend, TransportError (+ From<RingPairError>)
        batch.rs                       TransportNode, BatchHeader, BatchSnapshot, ShmBatchWriter, BatchBufferPool
        headers.rs                     RawHeaders<'a> compact header codec
        ring_pair.rs                   RingPair, TieredRingSet([Arc<SpscRing>; 4]), RingPairFds([RingFd; 4])
      bridge.rs                        BatchBridge, BatchWriter(&mut DataSlab, &mut TransportArena), BridgeResponse(headers_raw: Bytes)
    benches/
      spsc.rs  ring_throughput.rs  shm_transport.rs  codec.rs
  vertebra-storage/
    Cargo.toml                         workspace dependencies + repository.workspace = true
    src/
      lib.rs                           macro-generated sync/async methods, timestamp: Option<i64> = None, zero-copy PyBytes
      store.rs  signing.rs  oci.rs  r2.rs
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Refactoring `SpscRing` futex park/wake into `park_until` alters memory ordering and introduces lost wakeups. | Preserve the exact `Release` cursor store -> `SeqCst` wait-word `fetch_add` -> `SeqCst` waiter load sequence in `publish_*` and `park_until`, verified by `test_ring` stress tests and `spsc` Criterion benchmarks. |
| Replacing VRES `ResponseData` with `ResponseSlot` across `vertebra-core`, `vertebra-http`, and `vertebra-py` breaks SHM response decoding. | Task 2.5 updates all three crates atomically and adds a cross-crate round-trip regression test driving `submit_raw_response` through `RingPair` into `vertebra-http`'s reader. |
| Removing `RingPairFds` named fields breaks worker environment variable handoff. | Keep the exact `VERTEBRA_REQ_SHM_FD` and `VERTEBRA_RESP_{SMALL,MEDIUM,LARGE,XLARGE}_SHM_FD` environment variable names generated from `TIER_NAMES` and verify with `env_vars_protocol_complete`. |

## Implementation Plan

### Phase 1: Correctness Fixes, Dead Code Removal, and Ring Split
- [x] 2.1 Ring and frame correctness fixes ([worksheet](tasks/2.1.md))
- [x] 2.2 Remove dead core modules and dependencies ([worksheet](tasks/2.2.md))
- [x] 2.3 Remove the `vertebra-db` crate ([worksheet](tasks/2.3.md))
- [x] 2.4 Split rings into `ring/` with shared park and publish helpers ([worksheet](tasks/2.4.md))
- [x] Checkpoint: `make lint-all && make rs-test && make bench-check BASELINE=pre-refactor`

### Phase 2: Wire Format Unification, Transport & Bridge Cleanup, and Storage Consolidation
- [x] 2.5 One response wire format ([worksheet](tasks/2.5.md))
- [x] 2.6 Transport batch and tiered ring cleanup ([worksheet](tasks/2.6.md))
- [x] 2.7 Batch bridge without blocking under lock ([worksheet](tasks/2.7.md))
- [x] 2.8 Shared memory, notifier, and macOS wait cleanup ([worksheet](tasks/2.8.md))
- [x] 2.9 Storage crate fixes and dedupe ([worksheet](tasks/2.9.md))
- [x] Chapter gate: `make lint-all && make rs-test && make py-test && make ts-test && make bench-check BASELINE=pre-refactor`, then area review (`runtime-ipc-reviewer`, `pyo3-bridge-reviewer`, `performance-reviewer`).

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C2-R1 | 2.1 | `cd src/rs && cargo test -p vertebra-core` |
| C2-R2 | 2.2 | `cd src/rs && cargo build --workspace --all-targets && make rs-test` |
| C2-R3 | 2.3 | `cd src/rs && cargo metadata --format-version 1 >/dev/null && make rs-test` |
| C2-R4 | 2.4 | `cd src/rs && cargo test -p vertebra-core && cargo bench -p vertebra-core --bench spsc` |
| C2-R5 | 2.5 | `make rs-test && make py-test` |
| C2-R6 | 2.6 | `cd src/rs && cargo test -p vertebra-core && cargo bench -p vertebra-core --bench shm_transport` |
| C2-R7 | 2.7 | `cd src/rs && cargo test -p vertebra-core && cargo bench -p vertebra-http --bench server_throughput -- bridge_dispatch` |
| C2-R8 | 2.8 | `cd src/rs && cargo test -p vertebra-core` |
| C2-R9 | 2.9 | `cd src/rs && cargo test -p vertebra-storage && uv run pytest src/py/tests -k storage -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| CORE-01 | 2.1 |
| CORE-02 | 2.1 |
| OKF-02 | 2.1 |
| OKF-03 | 2.1 |
| CORE-04 | 2.1 |
| OKF-04 | 2.1 |
| CORE-20 | 2.1 (offset validation), 2.6 (pooledBatchSnapshot) |
| CORE-05 | 2.2 |
| CORE-06 | 2.2 |
| CORE-11 | 2.2 |
| CORE-12 | 2.2 |
| CORE-13 | 2.2 (dead modules deleted; `logger.rs` & `telemetry.rs` bound in 4.10) |
| CORE-14 | 2.2 |
| CORE-16 | 2.2 |
| CORE-31 | 2.2 |
| CORE-33 | 2.2 |
| CORE-35 | 2.2 |
| CORE-39 | 2.2 |
| CORE-40 | 2.2 (dead bench deletions & spin_wait merge) |
| CORE-43 | 2.2 |
| CORE-15 | 2.3 |
| CORE-44 | 2.3 |
| CORE-07 | 2.4 |
| CORE-08 | 2.4 |
| CORE-09 | 2.4 |
| CORE-03 | 2.5 |
| CORE-17 | 2.5 |
| CORE-18 | 2.5 |
| CORE-19 | 2.5 |
| CORE-42 | 2.5 |
| HTTP-09 | 2.5 |
| CORE-21 | 2.6 |
| CORE-22 | 2.6 |
| CORE-23 | 2.6 |
| CORE-24 | 2.6 |
| CORE-25 | 2.6 |
| CORE-26 | 2.6 |
| CORE-27 | 2.7 |
| CORE-28 | 2.7 |
| CORE-29 | 2.7 |
| CORE-30 | 2.8 |
| CORE-32 | 2.8 |
| CORE-36 | 2.9 |
| CORE-37 | 2.9 |
| CORE-38 | 2.9 |

## Continuity Snapshot

- Active flow: `core-ipc-consolidation`, state `planned`. Current task: none. Next step: `/flow:implement core-ipc-consolidation` after Chapter 1 completes.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 0.
- Blockers: none. Decisions: inherited D4 (`logger.rs` and `telemetry.rs` retained in `vertebra-core` for binding in Chapter 4 Task 4.10).
- Knowledge paths: `.agents/bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md`, `.agents/bundles/knowledge/gotchas/shm-and-cross-platform-ipc.md`, `.agents/bundles/knowledge/architecture/ipc/ring-buffers-and-shm.md`, `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md`.
