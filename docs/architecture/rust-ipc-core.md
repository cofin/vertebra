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

### MPMC

`MpmcRing` implements a bounded multi-producer/multi-consumer queue using
per-slot sequence numbers (epoch-style ABA mitigation).

### Buffer Channel

`BufferChannel` sends buffer metadata (index + length + flags) over an SPSC ring.
Buffers live in a shared `BufferPool`, enabling zero-copy transfers.

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
