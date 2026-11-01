# Shared-Memory Rings, Waiters, and Tiered Allocation

## Producer / Consumer Ordering Protocol (`buffer.rs`, `channel.rs`)

```rust
// Producer publish sequence:
// 1. Write payload bytes into slot data region.
// 2. Write 8-byte SlotHeader { len, flags }.
// 3. Record active tier flag (mark_tier_active) and publish write_head / sequence with Ordering::Release.
// 4. Check should_notify_and_clear() / waiter counters with Ordering::SeqCst; wake only when consumer is sleeping.

// Consumer acquire sequence:
// 1. Load write_head or slot sequence with Ordering::Acquire.
// 2. Validate offset + len <= data_len and slot_len <= payload_capacity() (slot_size - 8).
// 3. Construct slice via unsafe { std::slice::from_raw_parts(...) } with // SAFETY: comment.
// 4. Release slot (SlotView::release) with Ordering::Release and increment wait_read with Ordering::SeqCst.
```

## Cross-Platform Wait Primitives (`platform/{linux,macos,windows}.rs`, `notify.rs`)

- **Linux (`futex`)**:
  - `FUTEX_WAIT` / `FUTEX_WAKE` on `AtomicU32`.
  - Treat `EAGAIN` (value changed before sleep) and `EINTR` (signal interruption) as non-fatal wakeup conditions; re-evaluate the ring predicate in the caller loop.
- **macOS (`__ulock_wait` / `__ulock_wake`)**:
  - `UL_COMPARE_AND_WAIT` (`1`) with `ULF_NO_ERRNO` (`0x0100_0000`) returns negative `-errno` directly on failure and `>= 0` (remaining waiters) on success.
  - POSIX shared-memory names passed to `shm_open` on macOS must not exceed `PSHMNAMLEN` (`31` bytes including leading `/`, formatted as `/vtb-{pid:x}-{seq:x}-{attempt:x}`).
- **Windows (`WaitOnAddress` / `WakeByAddressAll`)**:
  - `Notifier` in `notify.rs` uses `AtomicU32` with `WaitOnAddress` and `WakeByAddressAll`.

## Tiered Payload Allocation (`tiered_ring.rs`, `transport/ring_pair.rs`)

- **`TieredRingConfig::default()`**: `small` (`4 KiB x 512`), `medium` (`32 KiB x 128`), `large` (`256 KiB x 32`), `xlarge` (`2 MiB x 8`), plus `TieredRingConfig::compact()` (`8 MiB`) when cgroup memory or `/dev/shm` capacity is constrained.
- **Overflow Spillover**: Requests `> 64 KiB` and responses `> 2 MiB` (up to `MAX_OVERFLOW_RESPONSE_SIZE = 64 MiB`) spill into one-shot named `ShmRegion` overflow regions (`REQUEST_OVERFLOW_MAGIC = "VQOV"`, `RESPONSE_OVERFLOW_MAGIC = "VROV"`).
