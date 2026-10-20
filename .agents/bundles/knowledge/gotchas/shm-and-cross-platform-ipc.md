---
name: gotchas-shm-and-cross-platform-ipc
description: Critical shared-memory ring, futex/ulock memory ordering, macOS XNU shm_open/pipe FD, and matchit 0.9 routing gotchas.
sources:
  - src/rs/vertebra-core/src/channel.rs
  - src/rs/vertebra-core/src/ipc.rs
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-core/src/platform/macos.rs
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-http/src/router.rs
---

# Shared-Memory Rings & Cross-Platform IPC Gotchas

## 1. `SlotView::release()` & `Ordering::SeqCst` Futex Handshake (`channel.rs`)

- **Unreleased `SlotView`**: [`SpscRing::try_recv_view()`](../../../../src/rs/vertebra-core/src/channel.rs) borrows the SHM slot in place without advancing `read_idx`. Dropping a `SlotView` without calling `slot.release()` leaves `read_idx` unchanged, causing the next read to re-consume the same message and eventually wedging the ring.
- **Store-Load Reordering on `send_waiters` / `recv_waiters`**: In `SlotView::release()`, `header.wait_read_ptr().fetch_add(1, Ordering::SeqCst)` **must** execute unconditionally before loading `header.send_waiters.load(Ordering::SeqCst)`. Using `Acquire`/`Release` alone allows the CPU to reorder the store to `read_idx` past the load of `send_waiters`, causing missed `futex_wake` notifications and deadlocks under heavy contention.

## 2. `MpmcRing::has_capacity()` Sequence Check (`ipc.rs`)

- Never use SPSC capacity arithmetic (`write_idx - read_idx < capacity`) on [`MpmcRing`](../../../../src/rs/vertebra-core/src/ipc.rs). In Vyukov's bounded MPMC queue, a slot at `tail = write_idx` is available for writing if and only if `(slot.sequence.load(Acquire) as i64) - (tail as i64) >= 0`.

## 3. macOS XNU `shm_open`, `pipe` FDs & `__ulock_wait` (`shm.rs`, `macos.rs`, `ring_pair.rs`)

1. **31-Byte `PSHMNAMLEN` Limit**: macOS XNU rejects `shm_open` names longer than 31 bytes (including the leading `/`) with `EINVAL`. [`shm.rs`](../../../../src/rs/vertebra-core/src/shm.rs) formats names as `/vtb-{pid:x}-{seq:x}-{attempt:x}` ($\le 28$ bytes).
2. **Inherited Pipe Write FDs**: On non-Linux Unix (`pipe(2)`), child workers must inherit **both** `read_fd` (`VERTEBRA_REQ_NOTIFY_FD`) and `write_fd` (`VERTEBRA_REQ_NOTIFY_WRITE_FD`) via [`Notifier::from_raw_fds`](../../../../src/rs/vertebra-core/src/platform/macos.rs). Allocating a dummy `write_fd = -1` breaks `notify()` in child workers.
3. **`__ulock_wait` with `ULF_NO_ERRNO` (`0x0100_0000`)**: Returns `rc >= 0` (remaining waiters) on success and `-errno` (`rc < 0`, e.g. `-ETIMEDOUT = -60`, `-ENOENT = -2`, `-EFAULT = -14`) on failure. Checking `rc == 0` instead of `rc >= 0` treats waking when other threads remain queued as an error.

## 4. `matchit 0.9` Catch-All Syntax (`router.rs`)

- `matchit 0.9` uses `{param}` for single segments and `{*rest}` for catch-all parameters (not `:param` or `*rest`). Always normalize route paths through [`normalize_typed_path()`](../../../../src/rs/vertebra-http/src/router.rs) before `matchit::Router::insert`.
