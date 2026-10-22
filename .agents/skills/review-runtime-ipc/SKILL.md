---
name: review-runtime-ipc
description: Reviews Rust shared-memory rings, futex/ulock handshakes, atomic orderings, SHM lifecycle, and unsafe code across vertebra-core, vertebra-storage, and vertebra-db.
---

<!-- truth: start -->
- **Ordering**: Cursor publish is `Release`; wait-word RMW and waiter loads are `SeqCst` (`RT-03`, `RT-04`).
- **Bounds**: Validate `offset + len <= data_len` and `slot_len <= max_payload` before `from_raw_parts` (`RT-02`).
- **Slots**: Release every borrowed slot on all exit paths; MPMC capacity uses slot sequence (`RT-05`, `RT-06`).
- **Layout**: `#[repr(C)]` header, cache-line isolation for cursors and waiters (`RT-07`).
- **Platforms**: macOS names <= 31 bytes, `__ulock_wait` `rc >= 0`, Linux futex handles `EAGAIN`/`EINTR` (`RT-08`).
- **Suppressions**: Respect `FP-RT-01`..`FP-RT-03` in the shared catalog.
<!-- truth: end -->

# Runtime and IPC Review (`review-runtime-ipc`)

## Scope

- Paths: `[areas.runtime-ipc]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `RT-01`..`RT-12` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Lock-free SHM and sharding](../../bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md)
- [SHM and cross-platform IPC gotchas](../../bundles/knowledge/gotchas/shm-and-cross-platform-ipc.md)
- [Ring buffers and SHM](../../bundles/knowledge/architecture/ipc/ring-buffers-and-shm.md)
- [Tiered rings and batch bridge](../../bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md)
- [SHM supervision and recovery](../../bundles/knowledge/architecture/runtime/shm-supervision-and-recovery.md)
- Deep analysis reference: [`references/memory-ordering.md`](references/memory-ordering.md)

## Review Process

1. **Header layout and lifecycle**: Verify cache-line isolation, version assertions, unique naming, and unlink on drop (`RT-07`, `RT-09`).
2. **Atomic pairing**: Verify every store has an acquire/release partner and wait words use `SeqCst` (`RT-03`, `RT-04`).
3. **Safety and bounds**: Require `// SAFETY:` invariants and checked arithmetic on all SHM slices (`RT-01`, `RT-02`).
4. **Exit path slot releases**: Check `?` and early returns inside slot scopes (`RT-05`, `RT-06`, `RT-11`).
5. **Platform abstractions**: Verify Linux futex and Darwin ulock semantics (`RT-08`).
6. **Async discipline**: No blocking waits or sync mutexes across `.await` points (`RT-10`).
7. **Test rigor**: Concurrency tests must use real threads and wrap around ring capacity (`TEST-04`).

## Suppressions Index

- `FP-RT-01`: SeqCst on futex wait words
- `FP-RT-02`: Rust comments and safety invariants
- `FP-RT-03`: Darwin ulock return code convention

## Focused Commands

```bash
cargo test -p vertebra-core --test test_ring
cargo test -p vertebra-core --test test_mpmc
cargo test -p vertebra-core --test test_shm
```
