# Rust IPC Core Architecture

## Overview

The Rust IPC core (`src/rs/vertebra-core`) provides shared-memory primitives,
lock-free channels, and synchronization helpers that power zero-copy data
exchange between processes.

## Modules

- `shm.rs`: Shared memory region management (memfd/shm_open on Unix, CreateFileMapping on Windows).
- `buffer.rs`: Shared memory header layout, ring layout helpers, and buffer pool.
- `channel.rs`: SPSC ring channel (shared memory) + MPMC ring (in-memory) + buffer channel.
- `sync.rs`: Wait/wake primitives and shared mutex/rwlock/condvar abstractions.
- `platform/`: OS-specific wait/wake implementations.

## Shared Memory Layout

Each shared memory region begins with a fixed `ShmHeader` followed by the ring
buffer payload region. The header includes:

- `magic`, `abi_version` for compatibility validation
- `state`, `generation`, `creator_pid`, `creator_epoch` for lifecycle/recovery
- cacheline-aligned read/write heads and wait words

Slot payloads use a fixed-size prefix (`SlotHeader`) with `len + flags`.

## Channels

### SPSC

`SpscRing` provides single-producer/single-consumer messaging on shared memory
using cacheline-aligned head/tail atomics. Backpressure is applied via
`wait_on_address` primitives.

**Receive Methods:**

| Method | Latency | Use Case |
|--------|---------|----------|
| `try_recv()` | ~100ns | Non-blocking poll |
| `recv()` | ~3-5µs | Simple blocking |
| `recv_timeout()` | ~3-5µs | Blocking with timeout |
| `recv_hybrid()` | **~0.8µs** | Spin-then-block for lowest latency |

`recv_hybrid(out, spin_us, timeout)` implements a three-phase strategy:
1. **Immediate**: Check if data is available (atomic load)
2. **Spin**: Busy-wait checking `has_data()` for `spin_us` microseconds
3. **Park**: Fall back to futex `wait_on_address` if data doesn't arrive

The spin phase uses `has_data()` which performs 2x `Relaxed` atomic loads (~2ns/iteration), allowing ~2500 iterations in a 5µs spin window without any syscalls.

### MPMC

`MpmcRing` implements a bounded multi-producer/multi-consumer queue using
per-slot sequence numbers (epoch-style ABA mitigation). It is used for the
**Request Channel** (Broker → Worker) to allow lock-free concurrent dispatching
from multiple broker tasks.

### AsyncRing

`AsyncRing` wraps `SpscRing` with an eventfd-based `Notifier` for asyncio integration.
The eventfd provides a pollable file descriptor that can be added to Python's asyncio
event loop via `loop.add_reader()`.

### AsyncMpmcRing

`AsyncMpmcRing` wraps `MpmcRing` with a `Notifier`. It is used by Python workers
to receive requests from the MPMC request channel.

**Receive Methods:**

| Method | Latency | Use Case |
|--------|---------|----------|
| `try_recv()` | ~100ns | Non-blocking poll |
| `recv()` | ~5-10µs | Blocking with eventfd wait |
| `recv_hybrid()` | **~1.6µs** | Spin-then-eventfd for asyncio |

`recv_hybrid()` uses the same three-phase strategy as `SpscRing::recv_hybrid()`,
but falls back to eventfd polling instead of futex. This is slightly slower (~1.6µs
vs ~0.8µs) but integrates with asyncio's event loop.

**When to use which:**
- **SpscRing**: Response channel (Worker -> Broker), 1:1, max throughput
- **MpmcRing**: Request channel (Broker -> Worker), N:1, lock-free concurrency
- **AsyncRing / AsyncMpmcRing**: Python-side wrappers for asyncio integration

### Buffer Channel

`BufferChannel` sends buffer metadata (index + length + flags) over an SPSC ring.
Buffers live in a shared `BufferPool`, enabling zero-copy transfers.

## Notification

`notify.rs` provides `Notifier`, an eventfd-based notification mechanism for
cross-process wakeups. Key methods:

- `notify()`: Signal the notifier (write to eventfd)
- `wait(timeout_ms)`: Block until signaled (poll on eventfd)
- `clear()`: Consume pending notification (read from eventfd)
- `spin_wait_or_park(spin_us, timeout_ms)`: Hybrid spin-then-park using `poll(0)`
- `spin_wait_or_park_with(spin_us, timeout_ms, has_data)`: Hybrid with custom check

The `spin_wait_or_park_with()` variant takes a closure for data-availability checking,
which is critical for performance. Using a ring-buffer's `head != tail` check
(~2ns/iteration) instead of `poll(fd, 0)` (~300ns/iteration) provides 150x faster
spin loops.

## Synchronization

`sync.rs` wraps platform wait/wake primitives:

- Linux: futex
- macOS: ulock (opt-in) + spin/sleep fallback
- Windows: WaitOnAddress/WakeByAddress

Shared mutex/rwlock/condvar types are built atop these primitives, with a
file-lock fallback for environments lacking wait/wake support.

### OS-API Shim (Timed Waits)

Timeout-based waits route through the OS shim to avoid busy-yield loops:

- Linux: `futex(FUTEX_WAIT)` with `timespec`.
- macOS: `__ulock_wait` with timeout (feature `macos-ulock`), otherwise a
  bounded spin + nanosleep loop.
- Windows: `WaitOnAddress` with millisecond timeout.

SPSC `send_timeout` / `recv_timeout` use `wait_on_address_timeout`, so the
data path stays lock-free while backpressure waits are handled by the OS.

## FFI Surface

Bindings are exposed through:

- `vertebra-py` (PyO3) for Python integration
- `vertebra-js` (napi-rs) for Bun/Node integration

## Cross-Platform Notes

Platform-specific code is isolated under `platform/`, with shared interfaces
in `sync.rs` and `shm.rs`. The design avoids Linux-only assumptions to preserve
macOS/Windows parity.
