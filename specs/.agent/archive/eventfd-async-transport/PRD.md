# PRD-10: Eventfd Async Transport

## 1. Overview

### 1.1 Document Information

| Field | Value |
|-------|-------|
| PRD ID | PRD-10 |
| Title | Eventfd Async Transport |
| Status | Draft |
| Author | Claude Code |
| Created | 2026-01-19 |
| Last Updated | 2026-01-19 |

### 1.2 Executive Summary

This PRD defines the implementation of a high-performance async transport layer for Vertebra's shared-memory IPC. The current architecture wraps blocking ring buffer operations in `asyncio.to_thread()`, adding 50-130μs overhead per call. This PRD specifies a replacement architecture using eventfd-based signaling that integrates natively with Python's asyncio event loop, achieving sub-100μs latency and eliminating thread pool overhead entirely.

**Key Deliverables**:
1. Native asyncio integration via eventfd/loop.add_reader()
2. Pre-allocated buffer pool to eliminate allocation churn
3. Complete removal of CFFI bindings (replaced by PyO3)
4. Migration of all call sites from asyncio.to_thread to native async
5. Cross-platform support (Linux optimized, macOS/Windows via polling fallback)

### 1.3 Goals and Non-Goals

**Goals**:
- Achieve sub-100μs IPC round-trip latency (currently ~180μs)
- Eliminate thread pool overhead for IPC operations
- Reduce GC pressure from buffer allocations by 100x
- Maintain cross-platform compatibility (Linux primary, macOS/Windows supported)
- Clean migration with no backwards compatibility burden
- Support Python 3.12+ asyncio

**Non-Goals**:
- Maintaining backwards compatibility with CFFI bindings
- Supporting Python < 3.12
- Implementing Windows IOCP (pipe fallback is acceptable)
- Changing the MessagePack/Arrow serialization layer

### 1.4 Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| IPC round-trip latency | 180μs | <100μs | Benchmark suite |
| Buffer allocation rate | 650MB/s | <1MB/s | memory_profiler |
| Max sustainable throughput | 5K req/s | 50K req/s | Load test |
| Thread pool calls per request | 2 | 0 | Code inspection |
| Test coverage | 87 tests | 87+ tests | pytest |

## 2. Background

### 2.1 Current Architecture

Vertebra's Python-to-Bun IPC uses CFFI bindings to a Rust C ABI for shared-memory ring buffers. The flow:

```
Python async code
    → asyncio.to_thread(ring.recv, timeout_ms)
        → Thread pool worker
            → CFFI call to Rust C ABI
                → Blocking futex wait (holds GIL!)
            ← Data returned
        ← Thread returns
    ← asyncio.Future resolves
```

### 2.2 Problem Analysis

**Problem 1: Thread Pool Overhead**

Each asyncio.to_thread() call incurs:
- Thread pool task submission: 10-20μs
- Context switch to worker: 20-40μs
- GIL release/reacquisition: 10-30μs
- Context switch back: 20-40μs
- **Total: 50-130μs per call**

For request-response (send + recv): 100-260μs overhead.

**Problem 2: Buffer Allocation Churn**

CFFI bindings allocate 3 buffers per recv():
```python
out_len = ffi.new("size_t*")        # 8 bytes
out_flags = ffi.new("uint32_t*")    # 4 bytes
buffer = ffi.new("uint8_t[]", 65536)  # 64KB
```

At 10K req/s: **650MB/s** of allocations becoming immediate garbage.

**Problem 3: GIL Contention**

CFFI does NOT release the GIL during blocking calls. PyO3 uses `allow_threads()` which helps, but still requires threading.

### 2.3 Prior Art

| Project | Approach | Latency | Notes |
|---------|----------|---------|-------|
| uvloop | epoll + Cython | ~10μs | Native FD integration |
| trio | eventfd + asyncio | ~15μs | Similar pattern |
| asyncio.Queue | threading + conditions | ~50μs | Thread-based |
| Vertebra (current) | asyncio.to_thread | ~180μs | Thread pool overhead |

### 2.4 Related Specifications

- `specs/active/shared-memory-broker/` - Broker architecture (uses this transport)
- `docs/prd/PRD-4-bun-drivers.md` - Driver layer (depends on this transport)
- `docs/prd/PRD-0-foundation.md` - GIL bypass strategy (this implements it)

## 3. Technical Design

### 3.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Python asyncio Event Loop                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  loop.add_reader(eventfd, callback)                       │   │
│  │       ↓                                                    │   │
│  │  epoll_wait([eventfd, ...])                               │   │
│  │       ↓                                                    │   │
│  │  callback() → ring.try_recv() → resolve Future            │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ↓ (eventfd signal)
┌─────────────────────────────────────────────────────────────────┐
│                      Rust Ring Buffer                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  send(data)                                               │   │
│  │    → write to ring buffer                                 │   │
│  │    → write(eventfd, 1)  // Signal Python                  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Design

#### 3.2.1 Rust Notification Primitive

**File**: `src/rs/vertebra-core/src/notify.rs`

```rust
//! Cross-platform notification primitives for async IPC.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

/// Platform-independent notification handle.
pub struct Notifier {
    #[cfg(target_os = "linux")]
    fd: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    read_fd: OwnedFd,
    #[cfg(not(target_os = "linux"))]
    write_fd: OwnedFd,
}

impl Notifier {
    /// Create a new notification handle.
    pub fn new() -> std::io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
            if fd < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self { fd: unsafe { OwnedFd::from_raw_fd(fd) } })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut fds = [0i32; 2];
            if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Set non-blocking
            for &fd in &fds {
                let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
            }
            Ok(Self {
                read_fd: unsafe { OwnedFd::from_raw_fd(fds[0]) },
                write_fd: unsafe { OwnedFd::from_raw_fd(fds[1]) },
            })
        }
    }

    /// Get the file descriptor for polling (read end).
    pub fn as_raw_fd(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        { self.fd.as_raw_fd() }
        #[cfg(not(target_os = "linux"))]
        { self.read_fd.as_raw_fd() }
    }

    /// Signal that data is available.
    pub fn notify(&self) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let val: u64 = 1;
            let ret = unsafe {
                libc::write(self.fd.as_raw_fd(), &val as *const _ as *const _, 8)
            };
            if ret < 0 { Err(std::io::Error::last_os_error()) } else { Ok(()) }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let val: u8 = 1;
            let ret = unsafe {
                libc::write(self.write_fd.as_raw_fd(), &val as *const _ as *const _, 1)
            };
            if ret < 0 { Err(std::io::Error::last_os_error()) } else { Ok(()) }
        }
    }

    /// Clear pending notifications (call after handling).
    pub fn clear(&self) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let mut val: u64 = 0;
            let _ = unsafe {
                libc::read(self.fd.as_raw_fd(), &mut val as *mut _ as *mut _, 8)
            };
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        {
            let mut buf = [0u8; 64];
            loop {
                let ret = unsafe {
                    libc::read(self.read_fd.as_raw_fd(), buf.as_mut_ptr() as *mut _, 64)
                };
                if ret <= 0 { break; }
            }
            Ok(())
        }
    }
}
```

#### 3.2.2 Async Ring Buffer

**File**: `src/rs/vertebra-core/src/async_ring.rs`

```rust
//! Async-capable ring buffer with notification support.

use crate::channel::SpscRing;
use crate::notify::Notifier;

pub struct AsyncRing {
    ring: SpscRing,
    notifier: Notifier,
}

impl AsyncRing {
    pub fn new(ring: SpscRing) -> std::io::Result<Self> {
        Ok(Self {
            ring,
            notifier: Notifier::new()?,
        })
    }

    /// Get the notification file descriptor for asyncio integration.
    pub fn notifier_fd(&self) -> i32 {
        self.notifier.as_raw_fd()
    }

    /// Send data and notify waiters.
    pub fn send(&self, data: &[u8], flags: u32) -> Result<(), crate::channel::ChannelError> {
        self.ring.send(data, flags)?;
        let _ = self.notifier.notify();
        Ok(())
    }

    /// Try to receive without blocking.
    pub fn try_recv(&self, buffer: &mut [u8]) -> Result<Option<usize>, crate::channel::ChannelError> {
        match self.ring.try_recv(buffer) {
            Ok(len) => Ok(Some(len)),
            Err(crate::channel::ChannelError::Empty) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Clear notification (call after successful try_recv).
    pub fn clear_notification(&self) {
        let _ = self.notifier.clear();
    }
}
```

#### 3.2.3 Buffer Pool

**File**: `src/rs/vertebra-core/src/buffer_pool.rs`

```rust
//! Pre-allocated buffer pool for zero-allocation recv operations.

use std::sync::Mutex;

pub struct BufferPool {
    buffers: Mutex<Vec<Option<Box<[u8]>>>>,
    slot_size: usize,
}

impl BufferPool {
    pub fn new(capacity: usize, slot_size: usize) -> Self {
        let buffers = (0..capacity)
            .map(|_| Some(vec![0u8; slot_size].into_boxed_slice()))
            .collect();
        Self {
            buffers: Mutex::new(buffers),
            slot_size,
        }
    }

    pub fn acquire(&self) -> Option<Box<[u8]>> {
        let mut buffers = self.buffers.lock().unwrap();
        buffers.iter_mut().find_map(|slot| slot.take())
    }

    pub fn release(&self, buf: Box<[u8]>) {
        let mut buffers = self.buffers.lock().unwrap();
        if let Some(slot) = buffers.iter_mut().find(|s| s.is_none()) {
            *slot = Some(buf);
        }
        // If pool full, buffer is dropped
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }
}
```

#### 3.2.4 PyO3 Bindings

**File**: `src/rs/vertebra-py/src/async_ring.rs`

```rust
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use vertebra_core::{AsyncRing, BufferPool};

#[pyclass]
pub struct PyAsyncRing {
    ring: AsyncRing,
    pool: BufferPool,
}

#[pymethods]
impl PyAsyncRing {
    #[new]
    pub fn new(name: &str, slot_size: usize, slots: usize, pool_size: usize) -> PyResult<Self> {
        let ring = AsyncRing::create(name, slot_size, slots)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let pool = BufferPool::new(pool_size, slot_size);
        Ok(Self { ring, pool })
    }

    /// Get the notification file descriptor for asyncio.
    pub fn notifier_fd(&self) -> i32 {
        self.ring.notifier_fd()
    }

    /// Send data and notify.
    pub fn send(&self, data: &[u8]) -> PyResult<()> {
        self.ring.send(data, 0)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Try non-blocking receive with pooled buffer.
    pub fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let mut buffer = self.pool.acquire()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("buffer pool exhausted"))?;

        match self.ring.try_recv(&mut buffer) {
            Ok(Some(len)) => {
                let result = PyBytes::new(py, &buffer[..len]);
                self.pool.release(buffer);
                self.ring.clear_notification();
                Ok(Some(result))
            }
            Ok(None) => {
                self.pool.release(buffer);
                Ok(None)
            }
            Err(e) => {
                self.pool.release(buffer);
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
            }
        }
    }

    /// Clear notification (for Python-side handling).
    pub fn clear_notification(&self) {
        self.ring.clear_notification();
    }
}
```

#### 3.2.5 Python Async Wrapper

**File**: `src/py/vertebra/_internal/async_transport.py`

```python
"""Native asyncio transport for shared-memory IPC."""

from __future__ import annotations

import asyncio
import os
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vertebra_py import PyAsyncRing

class AsyncShmTransport:
    """Async wrapper for shared-memory ring buffer using eventfd.

    This class integrates with asyncio's event loop using loop.add_reader()
    to wait for data without blocking any threads.
    """

    __slots__ = ("_ring", "_fd", "_waiters", "_registered")

    def __init__(self, ring: PyAsyncRing) -> None:
        self._ring = ring
        self._fd = ring.notifier_fd()
        self._waiters: deque[asyncio.Future[bytes]] = deque()
        self._registered = False

    async def send(self, data: bytes) -> None:
        """Send data to the ring buffer."""
        self._ring.send(data)

    async def recv(self) -> bytes:
        """Receive data from the ring buffer.

        This method:
        1. Tries non-blocking recv first
        2. If empty, registers with event loop and waits
        3. When notified, tries recv again
        """
        # Fast path: data already available
        data = self._ring.try_recv()
        if data is not None:
            return data

        # Slow path: wait for notification
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._waiters.append(future)

        if not self._registered:
            loop.add_reader(self._fd, self._on_readable)
            self._registered = True

        return await future

    def _on_readable(self) -> None:
        """Called when eventfd is readable (data available)."""
        # Clear the notification
        self._ring.clear_notification()

        # Try to satisfy waiters
        while self._waiters:
            data = self._ring.try_recv()
            if data is None:
                break
            future = self._waiters.popleft()
            if not future.done():
                future.set_result(data)

        # Unregister if no more waiters
        if not self._waiters and self._registered:
            loop = asyncio.get_running_loop()
            loop.remove_reader(self._fd)
            self._registered = False

    def close(self) -> None:
        """Close the transport and clean up."""
        if self._registered:
            try:
                loop = asyncio.get_running_loop()
                loop.remove_reader(self._fd)
            except RuntimeError:
                pass  # No running loop
            self._registered = False

        # Cancel pending waiters
        for future in self._waiters:
            if not future.done():
                future.cancel()
        self._waiters.clear()

    def __del__(self) -> None:
        self.close()
```

### 3.3 Migration Strategy

#### 3.3.1 Files to Delete

These files will be completely removed:

| File | Reason |
|------|--------|
| `src/py/vertebra/_internal/shm.py` | CFFI bindings replaced by PyO3 |

#### 3.3.2 Files to Modify

| File | Changes |
|------|---------|
| `src/py/vertebra/_internal/_typing.py` | Update type stubs for new PyO3 API |
| `src/py/vertebra/resources/broker.py` | Replace asyncio.to_thread with AsyncShmTransport |
| `src/py/vertebra/ipc/channel.py` | Replace asyncio.to_thread with AsyncShmTransport |
| `src/py/vertebra/server/worker.py` | Replace asyncio.to_thread with AsyncShmTransport |
| `src/py/vertebra/storage/store.py` | Replace asyncio.to_thread with AsyncShmTransport |
| `src/py/vertebra/extensions/sqlspec/provider.py` | Replace asyncio.to_thread |
| All test files | Update to new async API |

#### 3.3.3 Files to Add

| File | Purpose |
|------|---------|
| `src/rs/vertebra-core/src/notify.rs` | Cross-platform notification primitives |
| `src/rs/vertebra-core/src/async_ring.rs` | Async-capable ring buffer |
| `src/rs/vertebra-core/src/buffer_pool.rs` | Pre-allocated buffer pool |
| `src/rs/vertebra-py/src/async_ring.rs` | PyO3 bindings for async ring |
| `src/py/vertebra/_internal/async_transport.py` | Python async wrapper |

### 3.4 API Changes

#### 3.4.1 Before (Current API)

```python
# broker.py - current pattern
async def _receive_response(self) -> dict:
    data, _flags = await asyncio.to_thread(
        self._response.recv,
        self._config.timeout_ms
    )
    return self._decoder.decode(data)
```

#### 3.4.2 After (New API)

```python
# broker.py - new pattern
async def _receive_response(self) -> dict:
    data = await self._transport.recv()
    return self._decoder.decode(data)
```

### 3.5 Fallback Strategy

For platforms without eventfd (macOS, Windows) or when eventfd fails:

```python
class PollingTransport:
    """Fallback transport using a single polling thread."""

    def __init__(self, ring: PyAsyncRing) -> None:
        self._ring = ring
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._running = True
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread.start()

    async def recv(self) -> bytes:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return await self._queue.get()

    def _poll_loop(self) -> None:
        while self._running:
            data = self._ring.try_recv()
            if data is not None and self._loop is not None:
                self._loop.call_soon_threadsafe(
                    self._queue.put_nowait, data
                )
            else:
                time.sleep(0.0001)  # 100μs polling interval
```

## 4. Implementation Plan

### 4.1 Phase 1: Rust Core (Week 1)

**Tasks**:
1. Implement `notify.rs` with eventfd (Linux) and pipe (macOS/Windows)
2. Implement `async_ring.rs` with notification integration
3. Implement `buffer_pool.rs` for zero-allocation recv
4. Add unit tests for Rust components
5. Run `cargo test` - all tests must pass

**Deliverables**:
- `src/rs/vertebra-core/src/notify.rs`
- `src/rs/vertebra-core/src/async_ring.rs`
- `src/rs/vertebra-core/src/buffer_pool.rs`
- Updated `src/rs/vertebra-core/src/lib.rs`

### 4.2 Phase 2: PyO3 Bindings (Week 1-2)

**Tasks**:
1. Add `PyAsyncRing` class to vertebra-py
2. Expose `notifier_fd()`, `send()`, `try_recv()`, `clear_notification()`
3. Add buffer pool integration
4. Build and test with `maturin develop`

**Deliverables**:
- `src/rs/vertebra-py/src/async_ring.rs`
- Updated `src/rs/vertebra-py/src/lib.rs`

### 4.3 Phase 3: Python Async Layer (Week 2)

**Tasks**:
1. Implement `AsyncShmTransport` using `loop.add_reader()`
2. Implement `PollingTransport` as fallback
3. Add transport factory that selects appropriate implementation
4. Write Python unit tests

**Deliverables**:
- `src/py/vertebra/_internal/async_transport.py`
- Test files for new transport

### 4.4 Phase 4: Migration (Week 2-3)

**Tasks**:
1. Update `broker.py` to use new transport
2. Update `channel.py` to use new transport
3. Update `worker.py` to use new transport
4. Update `store.py` to use new transport
5. Update `provider.py` to use new transport
6. Update all affected test files

**Deliverables**:
- Modified source files
- Updated test files
- All 87+ tests passing

### 4.5 Phase 5: Cleanup (Week 3)

**Tasks**:
1. Delete `shm.py` (CFFI bindings)
2. Update `_typing.py` type stubs
3. Remove any dead code
4. Run full test suite
5. Run benchmarks to verify performance targets

**Deliverables**:
- Deleted CFFI code
- Clean codebase
- Benchmark results

### 4.6 Phase 6: Documentation (Week 3)

**Tasks**:
1. Update CLAUDE.md if needed
2. Update progress.md
3. Add inline documentation
4. Create migration notes

## 5. Testing Strategy

### 5.1 Unit Tests

| Component | Test File | Coverage Target |
|-----------|-----------|-----------------|
| Notifier | `tests/test_notify.rs` | 100% |
| AsyncRing | `tests/test_async_ring.rs` | 100% |
| BufferPool | `tests/test_buffer_pool.rs` | 100% |
| PyAsyncRing | `test_async_ring.py` | 100% |
| AsyncShmTransport | `test_async_transport.py` | 100% |

### 5.2 Integration Tests

| Test | Description |
|------|-------------|
| `test_async_roundtrip` | Full send/recv cycle with new transport |
| `test_broker_async` | BrokerClient with new transport |
| `test_worker_async` | AsgiWorker with new transport |
| `test_fallback_polling` | PollingTransport on non-Linux |

### 5.3 Performance Tests

| Benchmark | Target | Measurement |
|-----------|--------|-------------|
| `bench_ipc_latency` | <100μs | pytest-benchmark |
| `bench_throughput` | >50K req/s | Custom load test |
| `bench_allocation` | <1MB/s | memory_profiler |

## 6. Rollout Plan

### 6.1 Development

1. All changes on feature branch
2. CI must pass (Linux, macOS, Windows)
3. All 87+ tests passing
4. Benchmarks meet targets

### 6.2 Review

1. Code review by maintainer
2. Performance review of benchmarks
3. API review for ergonomics

### 6.3 Merge

1. Squash merge to main branch
2. Tag release if applicable
3. Update changelog

## 7. Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| eventfd not available on old Linux | Fallback to polling | Low | Runtime detection, polling fallback |
| Buffer pool exhaustion | Stalls | Medium | Monitor pool, warn when low, grow on demand |
| PyO3 build issues | Blocked users | Low | Good CI, clear build instructions |
| Performance regression on macOS/Windows | User complaints | Medium | Document Linux optimization, polling acceptable |
| Test failures during migration | Delayed release | Medium | Incremental migration, thorough testing |

## 8. Open Questions

1. **Pool growth policy**: Should buffer pool grow on demand or have fixed size?
   - Recommendation: Fixed size with warning, configurable via env var

2. **Timeout handling**: How to handle timeouts in new async model?
   - Recommendation: asyncio.wait_for() wrapper in high-level API

3. **Multiple rings per transport**: Should one transport handle multiple rings?
   - Recommendation: One transport per ring, simpler implementation

## 9. Appendix

### 9.1 Glossary

| Term | Definition |
|------|------------|
| eventfd | Linux-specific file descriptor for event notification |
| SPSC | Single-Producer Single-Consumer (ring buffer type) |
| GIL | Global Interpreter Lock (Python threading limitation) |
| CFFI | C Foreign Function Interface (Python → C bindings) |
| PyO3 | Rust → Python bindings library |

### 9.2 References

- [Linux eventfd(2) man page](https://man7.org/linux/man-pages/man2/eventfd.2.html)
- [Python asyncio.loop.add_reader](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.add_reader)
- [PyO3 User Guide](https://pyo3.rs/)
- [Vertebra Architecture](../../../docs/arch.md)
