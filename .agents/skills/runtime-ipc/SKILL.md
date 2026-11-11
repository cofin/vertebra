---
name: runtime-ipc
description: Use when implementing or debugging Rust shared-memory rings, futex/ulock handshakes, atomic orderings, SHM supervision, Arrow IPC tables, or storage slabs across vertebra-core, vertebra-storage, and vertebra-db.
---

<!-- truth: start -->
- **Crates**: `src/rs/vertebra-core` (`shm.rs`, `buffer.rs`, `channel.rs`, `tiered_ring.rs`, `notify.rs`, `broker.rs`, `platform/{linux,macos,windows}.rs`), `src/rs/vertebra-storage`, `src/rs/vertebra-db`.
- **Memory Ordering**: Producer cursor publish uses `Ordering::Release` after payload and slot length writes; consumer cursor load uses `Ordering::Acquire` (`RT-03`).
- **Waiters**: Wait-word RMW (`wait_read`, `wait_write`, `send_waiters`, `recv_waiters`, `consumer_sleeping`) and wake checks use `Ordering::SeqCst` (`RT-04`).
- **Bounds & Safety**: Every `unsafe` block includes a `// SAFETY:` justification (`RT-01`) and validates `offset + len <= data_len` and `slot_len <= max_payload` before `std::slice::from_raw_parts` (`RT-02`).
- **Platform IPC**: macOS `shm_open` names `<= 31` bytes (`/vtb-{pid:x}-{seq:x}-{attempt:x}`), `__ulock_wait` treats `rc >= 0` as success, and Linux `futex` loops handle `EAGAIN` and `EINTR` (`RT-08`).
- **Verify**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage -p vertebra-db`
<!-- truth: end -->

# Runtime & Shared-Memory IPC (`runtime-ipc`)

Implementation guide for Rust lock-free shared-memory ring buffers (`SpscRing`, `MpmcRing`, `TieredRingSet`), cross-process wait primitives, SHM connection/lease brokers, storage slabs, and Arrow/Parquet data-plane tables.

## Architecture & Domain References

- [Lock-free SHM and sharding](../../bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md)
- [SHM and cross-platform IPC gotchas](../../bundles/knowledge/gotchas/shm-and-cross-platform-ipc.md)
- [Ring buffers and SHM](../../bundles/knowledge/architecture/ipc/ring-buffers-and-shm.md)
- [Tiered rings and batch bridge](../../bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md)
- [SHM supervision and recovery](../../bundles/knowledge/architecture/runtime/shm-supervision-and-recovery.md)
- [Object storage](../../bundles/knowledge/domains/storage/object-storage.md)
- [SHM connection broker and leases](../../bundles/knowledge/domains/data/broker-and-shm-leases.md)
- [Database drivers and dialers](../../bundles/knowledge/domains/data/database-drivers-and-dialers.md)
- Implementation patterns reference: [`references/shm-and-rings.md`](references/shm-and-rings.md)
- Review checklist & suppressions: [`review-runtime-ipc`](../review-runtime-ipc/SKILL.md)

## Core Implementation Rules

1. **Header & Cache-Line Layout (`RT-07`)**:
   - Mark shared-memory headers (`ShmHeader` in `buffer.rs`) and ring descriptors `#[repr(C)]`.
   - Wrap hot atomics (`read_head`, `write_head`, `wait_read`, `wait_write`, `consumer_sleeping`, `active_flags`, and MPMC sequence entries) in `CacheLine<T>` (`64` bytes on x86_64, `128` bytes on aarch64 macOS) to prevent false sharing.
   - Bump `ABI_VERSION` (`buffer.rs`) whenever a `#[repr(C)]` layout changes.

2. **Slot Lifetimes & Capacity (`RT-05`, `RT-06`)**:
   - Ensure every borrowed ring or slab slot (`SlotView`, `ShmSlotHolder`) is released on all return paths, including early error returns.
   - In `MpmcRing` (`channel.rs`), determine slot readiness via the signed sequence difference `(seq as i64 - tail as i64) >= 0` rather than SPSC bitmasking.

3. **Wait/Wake & Dekker Handshake (`RT-03`, `RT-04`, `RT-08`)**:
   - Set `consumer_sleeping` / increment waiter counters with `Ordering::SeqCst` *before* re-checking ring readiness so wakeups cannot slip between check and sleep.
   - Clear sleep state via an RAII drop guard (`cancel_sleep`) before clearing `Notifier` readiness.

4. **SHM Lifecycle & Overflow Regions (`RT-09`, `RT-10`)**:
   - Use `memfd_create` on Linux with `shm_open` fallback (`create_overflow_region` / `/tmp/vtb-*` file-backed fallback when `/dev/shm` is constrained) and unlink one-shot overflow segments immediately upon consumer attach.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage -p vertebra-db
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression,gcp -- -D warnings
```
