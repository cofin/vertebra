# Rust Fast Path Research Plan

## Executive Summary

This document details the research and design for implementing a Rust fast path to achieve sub-10 microsecond IPC latency in Python. The current implementation achieves approximately 55 microseconds p99 latency, primarily due to Python GIL overhead, object allocation, and asyncio.to_thread() costs. Multi-model consensus from Gemini 3 Pro (both FOR and AGAINST stances) confirms the approach is architecturally sound with high confidence (8.5/10 average) that sync path targets are achievable.

## Problem Statement

### Current Performance Characteristics

The Vertebra IPC system uses shared memory ring buffers for zero-copy message passing between Python workers and the Bun broker. While the underlying Rust primitives achieve sub-microsecond latency, the Python wrapper introduces significant overhead:

1. **GIL Acquisition/Release**: Every PyO3 call requires acquiring and releasing the Global Interpreter Lock. Each acquisition costs 1-5 microseconds depending on contention. A typical request/response cycle requires 4-6 GIL operations.

2. **Python Object Allocation**: The current implementation creates a new PyBytes object for every received message using `PyBytes::new()`. This triggers Python's memory allocator and can cause GC pressure, adding 2-5 microseconds per message with non-deterministic jitter affecting p99 latency.

3. **asyncio.to_thread() Overhead**: The async path currently wraps synchronous Rust calls in `asyncio.to_thread()`, which dispatches work to a thread pool. This adds approximately 20 microseconds due to thread scheduling, context switching, and GIL contention.

4. **Python Spin-Wait**: The synchronous path uses `time.sleep(0.001)` for polling, which has 1ms granularity and wastes CPU cycles.

### Measured Baseline Performance

From benchmarks/ipc_performance.py:
- p99 latency: 53-58 microseconds
- Throughput: 32-34K messages/second
- Allocations: 1.8 bytes/message (near-zero, good)

From benchmarks/ipc_performance.ts (TypeScript comparison):
- Protocol layer p99: 7.64 microseconds
- ShmRing p99: 9.99 microseconds
- This proves the underlying transport CAN achieve sub-10us latency

## Research Methodology

### Multi-Model Consensus Process

We consulted Gemini 3 Pro with both FOR and AGAINST stances to validate the proposed approach:

**FOR Stance (9/10 confidence):**
- Confirmed that eliminating PyBytes::new() is critical for deterministic p99
- Recommended hybrid spin-then-park strategy (spin 2-5us before eventfd block)
- Identified Phase 3 (Rust-native multiplexing) as highest complexity
- Mandated avoiding asyncio.to_thread() - use add_reader with eventfd directly

**AGAINST Stance (8/10 confidence):**
- Agreed on approach but more conservative on targets
- Expects realistic floor of 15-25us for async path
- Recommends memoryview over buffer indices for usability
- Warns about Python runtime overhead being hard floor

### Codebase Analysis

Examined the following files to understand current implementation:

1. **src/rs/vertebra-py/src/lib.rs** (969 lines)
   - PyChannel class with send/recv methods
   - PyBuffer with buffer protocol support
   - Pattern for __getbuffer__/__releasebuffer__

2. **src/rs/vertebra-core/src/notify.rs** (705 lines)
   - InheritableNotifier with eventfd (Linux) / pipe (other) support
   - wait() method for blocking with timeout
   - has_pending() for non-blocking check

3. **src/py/vertebra/ipc/channel.py** (429 lines)
   - BufferView class returning memoryview
   - encode_request/decode_request with protocol headers
   - send_sync/receive_sync using asyncio.to_thread

4. **src/rs/vertebra-py/src/supervisor.rs** (243 lines)
   - NativeSupervisor creates InheritableNotifier before fork
   - Passes VERTEBRA_NOTIFY_FD to children
   - Pattern for FD inheritance

## Technical Design

### Architecture Overview

```
CURRENT ARCHITECTURE (55us):

Python                          Rust
+------------------+           +------------------+
| async send()     |           |                  |
|   to_thread() -->|-- GIL -->-| send_request()   |
|                  |           |   ring.send()    |
+------------------+           +------------------+
        |                              |
        v                              v
+------------------+           +------------------+
| async recv()     |           |                  |
|   to_thread() <--|-- GIL --<-| recv_request()   |
|   PyBytes::new() |           |   ring.recv()    |
+------------------+           +------------------+


OPTIMIZED ARCHITECTURE (<10us):

Python                          Rust
+------------------+           +------------------+
| fast_send()      |           |                  |
|   memcpy -->-----|-- GIL -->-| send_from_slot() |
|                  |           |   ring.send()    |
+------------------+           +------------------+
        |                              |
        v                              v
+------------------+           +------------------+
| fast_recv()      |           |                  |
|   memoryview <---|-- GIL --<-| recv_to_slot()   |
|   (no alloc)     |           |   spin_wait()    |
+------------------+           +------------------+
```

### Phase 1: Hybrid Spin-Wait (Foundation)

The first optimization is replacing Python's `time.sleep()` polling with a Rust-native hybrid spin-then-park strategy. This technique is proven in high-performance systems like LMAX Disruptor and Intel DPDK.

**Algorithm:**
```
fn spin_wait_or_park(spin_us: u64, timeout_ms: i32) -> bool:
    start = Instant::now()
    spin_deadline = start + Duration::from_micros(spin_us)

    // Spin phase: busy-wait checking for data
    while Instant::now() < spin_deadline:
        if ring.has_data():
            return true
        std::hint::spin_loop()  // CPU hint for spin-wait

    // Park phase: block on eventfd
    return eventfd.wait(timeout_ms)
```

This approach captures immediate responses (common in IPC) without syscall overhead, while still being CPU-efficient for longer waits.

**Implementation Location:**
- Add `spin_wait_or_park()` to `InheritableNotifier` in `notify.rs`
- Add `recv_hybrid()` to `SpscRing` in `channel.rs`
- Expose as `recv_request_hybrid()` in `PyChannel`

### Phase 2: Zero-Copy SlotView

The second optimization eliminates the `PyBytes::new()` allocation by returning a view directly into the ring buffer slot. This requires careful lifetime management to prevent Python from holding a view after the slot is reused.

**Design:**
```rust
struct SlotView {
    ring: Arc<SpscRing>,
    slot_index: usize,
    length: usize,
    guard: SlotGuard,  // Prevents ring advance while view exists
}

impl SlotView {
    fn as_slice(&self) -> &[u8] {
        &self.ring.slot_data(self.slot_index)[..self.length]
    }
}

impl Drop for SlotView {
    fn drop(&mut self) {
        self.ring.release_slot(self.slot_index);
    }
}
```

**Python Integration:**
```rust
#[pyclass]
struct PySlotView {
    inner: Option<SlotView>,
    exports: AtomicUsize,
}

#[pymethods]
impl PySlotView {
    unsafe fn __getbuffer__(...) {
        // Return view into slot data
    }

    unsafe fn __releasebuffer__(...) {
        // Decrement export count
    }
}
```

### Phase 3: asyncio.add_reader Integration

The third optimization replaces `asyncio.to_thread()` with native event loop integration using the inherited eventfd file descriptor.

**Current (slow):**
```python
async def recv(self):
    return await asyncio.to_thread(self._channel.recv_sync)
```

**Optimized (fast):**
```python
async def recv_fast(self):
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def on_readable():
        loop.remove_reader(self._notify_fd)
        try:
            result = self._channel.try_recv_to_slot()
            if result is not None:
                future.set_result(result)
            else:
                # Spurious wakeup, re-register
                loop.add_reader(self._notify_fd, on_readable)
        except Exception as e:
            future.set_exception(e)

    loop.add_reader(self._notify_fd, on_readable)
    return await future
```

This approach:
- Avoids thread pool overhead entirely
- Uses kernel-level notification (epoll/kqueue)
- Compatible with uvloop for additional performance

### Phase 4: Rust-Native Correlation Tracking

The final optimization moves correlation ID tracking from Python to Rust, achieving a single GIL acquisition per complete request/response cycle.

**Design:**
```rust
struct FastChannel {
    request_ring: SpscRing,
    response_ring: SpscRing,
    pending: DashMap<u64, PendingRequest>,
    next_id: AtomicU64,
    notifier: InheritableNotifier,
}

struct PendingRequest {
    slot: Option<SlotView>,
    waker: Option<std::task::Waker>,
}

impl FastChannel {
    fn request_response_fast(
        &self,
        request_data: &[u8],
        spin_us: u64,
        timeout_ms: u64,
    ) -> Result<SlotView, ChannelError> {
        // 1. Allocate correlation ID
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // 2. Send request with header
        let mut header = [0u8; 16];
        encode_header(&mut header, id, request_data.len());
        self.request_ring.send_with_header(&header, request_data)?;

        // 3. Notify receiver
        self.notifier.notify()?;

        // 4. Wait for response (spin then park)
        loop {
            // Try to receive
            if let Some(view) = self.try_recv_response(id)? {
                return Ok(view);
            }

            // Spin-wait or park
            if !self.notifier.spin_wait_or_park(spin_us, timeout_ms as i32)? {
                return Err(ChannelError::Timeout);
            }
        }
    }

    fn try_recv_response(&self, expected_id: u64) -> Result<Option<SlotView>, ChannelError> {
        if let Some(view) = self.response_ring.try_recv_to_slot()? {
            let header = parse_header(view.as_slice());
            if header.id == expected_id {
                return Ok(Some(view));
            }
            // Store for another waiter
            self.pending.insert(header.id, PendingRequest { slot: Some(view), waker: None });
        }

        // Check pending map
        if let Some((_, req)) = self.pending.remove(&expected_id) {
            return Ok(req.slot);
        }

        Ok(None)
    }
}
```

## Performance Validation

### Benchmark Suite

Create `benchmarks/ipc_fast_path.py`:

```python
def benchmark_sync_immediate():
    """Measure latency when data is already available."""
    # Pre-send messages, then measure recv time
    pass

def benchmark_sync_blocked():
    """Measure latency with blocking wait."""
    # Send in background thread, measure full round-trip
    pass

def benchmark_async_add_reader():
    """Measure async latency with add_reader."""
    # Use asyncio event loop with native FD integration
    pass

def benchmark_multiplexed():
    """Measure latency with 64 concurrent requests."""
    # Test Rust-side correlation tracking
    pass
```

### Expected Results

| Path | Current | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| Sync immediate | ~55us | ~25us | ~15us | ~15us | <10us |
| Sync blocked | ~55us | ~30us | ~20us | ~20us | <15us |
| Async | ~55us | ~55us | ~45us | <25us | <20us |
| 64x concurrent | N/A | N/A | N/A | N/A | <20us |

### Validation Commands

```bash
# Phase 1 validation
uv run python benchmarks/ipc_performance.py --spin-us=5

# Phase 4 validation
uv run python benchmarks/ipc_fast_path.py --target-p99-us=10

# Rust core validation
cd src/rs && cargo bench --package vertebra-core -- spin_wait

# Full test suite
make test-all
```

## Risk Mitigation

### Risk 1: SlotView Lifetime Management

**Problem:** Python could hold a memoryview after the ring buffer slot is reused, causing data corruption.

**Mitigation:**
1. SlotView holds an `Arc<SlotGuard>` that blocks ring advance
2. Ring tracks "in-flight" slots and refuses to overwrite
3. Configurable timeout for stuck views (copy-on-timeout fallback)
4. Python __del__ releases guard immediately

**Fallback:** If guard contention exceeds threshold, automatically copy to Python memory and release slot.

### Risk 2: asyncio.add_reader Compatibility

**Problem:** Not all event loops support add_reader (trio, curio).

**Mitigation:**
1. Detect event loop type at runtime
2. Prefer add_reader for asyncio and uvloop
3. Fall back to to_thread for unsupported loops
4. Document supported configurations

### Risk 3: Cross-Platform Eventfd

**Problem:** eventfd is Linux-only; macOS/Windows use different mechanisms.

**Mitigation:**
- Already handled in `notify.rs` with platform-specific code
- Linux: eventfd
- macOS/BSD: pipe pair
- Windows: Named events (future)
- CI validates on Linux and macOS

### Risk 4: GIL Contention in High-Concurrency

**Problem:** Many Python threads competing for GIL during response dispatch.

**Mitigation:**
1. Batch response collection: single GIL acquisition for multiple responses
2. Rust-side pending map reduces Python involvement
3. Use `py.allow_threads()` during spin/wait phases
4. Benchmark with 64 concurrent requests to validate

## Dependencies and Sequencing

```
Phase 1: Hybrid Spin-Wait
    |
    +---> Phase 2: Zero-Copy SlotView
    |         |
    |         +---> Phase 4: Rust Correlation
    |         |
    +---> Phase 3: asyncio.add_reader
              |
              +---> Phase 4: Rust Correlation
```

- Phase 1 is the foundation; all others depend on it
- Phase 2 and Phase 3 can proceed in parallel after Phase 1
- Phase 4 requires both Phase 2 (SlotView) and Phase 3 (add_reader)

## Success Criteria

1. `make test-all` passes with no regressions
2. `make lint-all` passes
3. Sync path benchmark: p99 < 10 microseconds
4. Async path benchmark: p99 < 20 microseconds
5. Zero per-message allocations in hot path (validated by tracemalloc)
6. 64 concurrent requests achieves same latency (no head-of-line blocking)
7. Documentation updated with new fast-path API

## Industry Comparison and Prior Art

### High-Performance IPC Systems

The proposed approach draws from several proven high-performance systems:

**LMAX Disruptor (Java)**
The Disruptor pattern, developed by LMAX Exchange for high-frequency trading, achieves sub-microsecond latency using:
- Pre-allocated ring buffers with power-of-two sizing
- Memory barriers instead of locks
- Single-writer principle (one producer per ring)
- Busy-spin wait strategies

Our Phase 1 hybrid spin-wait directly implements the Disruptor's BusySpinWaitStrategy with a fallback to BlockingWaitStrategy. The key insight is that for latency-critical paths, brief CPU spinning (2-5 microseconds) is cheaper than a syscall to the kernel scheduler.

**Intel DPDK (C)**
The Data Plane Development Kit achieves millions of packets per second by:
- Bypassing the kernel entirely (userspace networking)
- Pre-allocating memory pools (mbufs)
- Per-core lockless queues
- Busy-polling on NIC descriptors

While Vertebra cannot bypass Python's GIL entirely, we apply DPDK's principles by minimizing kernel crossings (using eventfd only when spinning fails) and pre-allocating buffers (SlotView eliminates per-message allocation).

**uvloop (Python)**
The fastest Python event loop, uvloop achieves 2-4x asyncio performance by:
- Using libuv (C library) for I/O
- Minimizing Python object creation in hot paths
- Tight integration with epoll/kqueue
- Avoiding Python's default selector implementation

Our Phase 3 directly replicates uvloop's pattern of using `add_reader()` for FD-based wakeups. When combined with inherited eventfd, this achieves native kernel notification without Python's selector overhead.

**pydantic v2 (Rust/Python)**
The pydantic validation library achieved 5-50x speedups by:
- Moving core logic to Rust
- Using PyO3 buffer protocol for zero-copy
- Minimizing GIL acquisitions
- Pre-computing schema lookups in Rust

Our Phase 2 and Phase 4 apply these patterns: SlotView uses the buffer protocol for zero-copy memoryview, and FastChannel keeps correlation tracking in Rust to minimize GIL crossings.

### Latency Budgets

Understanding where time is spent is critical for optimization:

| Operation | Typical Latency | Notes |
|-----------|-----------------|-------|
| L1 cache access | 1ns | Reference point |
| L2 cache access | 4ns | Reference point |
| Main memory | 100ns | Reference point |
| Mutex lock/unlock | 25ns | Uncontended |
| Syscall (minimal) | 100ns | getpid() |
| Syscall (epoll_wait) | 1-5us | With immediate return |
| eventfd write | 200-500ns | Notify |
| eventfd read | 200-500ns | Clear |
| epoll_wait (blocked) | 3-10us | Wake from sleep |
| Python GIL acquire | 1-5us | Depends on contention |
| PyBytes::new(1KB) | 2-5us | Includes allocator |
| asyncio.to_thread | 20-30us | Thread scheduling |
| time.sleep(0.001) | 1000us | 1ms minimum |

For a 10us budget, we can afford approximately:
- 1-2 GIL operations (2-4us)
- 1 eventfd notification (0.5us)
- 1 spin check or blocked wake (0-5us)
- Serialization/deserialization (remaining budget)

This budget analysis confirms that eliminating `asyncio.to_thread()` (20us) and `PyBytes::new()` (2-5us) are the highest-impact optimizations.

### Memory Layout Considerations

Cache-line optimization is critical for sub-10us latency:

**Cache Line Size**: 64 bytes on modern x86/ARM
**False Sharing**: When two cores modify data on the same cache line, the cache line "ping-pongs" between cores, adding 10-100 cycles per access.

The existing `SpscRing` in Vertebra uses cache-padded atomics:
```rust
pub struct SpscRing<T> {
    head: CachePadded<AtomicU64>,  // Producer-owned
    tail: CachePadded<AtomicU64>,  // Consumer-owned
    // ...
}
```

This ensures producer and consumer indices don't share a cache line, eliminating false sharing.

For SlotView, we must ensure:
1. Guard counters are cache-aligned
2. Slot data starts on cache-line boundary (64-byte aligned)
3. Metadata is separated from payload data

### Python Memory Model

Understanding CPython's memory management informs our zero-copy design:

**Object Allocation**: CPython uses a private heap with three layers:
- Small objects (< 512 bytes): pooled allocator
- Large objects: system malloc
- Very large objects (> 256KB): mmap

For IPC payloads (typically 1-64KB), each `PyBytes::new()` calls the pooled allocator, which involves:
1. Check arena for available pools
2. Scan pool bitmap for free block
3. Initialize object header (refcount, type pointer)
4. Copy payload data
5. Return pointer

This process takes 2-5 microseconds and can trigger GC.

**Buffer Protocol**: Python's buffer protocol (`__getbuffer__`) allows zero-copy access to memory:
1. C extension fills a `Py_buffer` struct with pointer, length, format
2. Python's `memoryview()` wraps the buffer
3. No data copying occurs
4. Extension is notified when memoryview is released (`__releasebuffer__`)

By implementing the buffer protocol in `PySlotView`, we eliminate allocation entirely. The memoryview points directly into the ring buffer slot.

**GIL Implications**: The GIL protects Python's internal structures, but:
- C extensions can release GIL during long operations (`Py_BEGIN_ALLOW_THREADS`)
- PyO3 provides `py.allow_threads()` for the same purpose
- While GIL is released, Python cannot call back into the extension

Our fast path design uses `py.allow_threads()` during spin-wait and eventfd blocking, minimizing GIL hold time.

## Implementation Examples from Codebase

### Existing Buffer Protocol Implementation

The codebase already has a working buffer protocol implementation in `lib.rs`:

```rust
// From src/rs/vertebra-py/src/lib.rs:576-599
unsafe fn __getbuffer__(
    slf: Bound<'_, Self>,
    view: *mut ffi::Py_buffer,
    flags: c_int,
) -> PyResult<()> {
    if view.is_null() {
        return Err(PyBufferError::new_err("view is null"));
    }
    let mut buffer = slf.borrow_mut();
    let inner = buffer.inner_mut()?;
    fill_view_from_data(view, flags, inner.as_mut_slice(), slf.into_any(), true)?;
    buffer.exports.fetch_add(1, AtomicOrdering::Relaxed);
    Ok(())
}
```

This pattern will be replicated for `PySlotView`.

### Existing Notifier Implementation

The `InheritableNotifier` in `notify.rs` provides the foundation:

```rust
// From src/rs/vertebra-core/src/notify.rs:541-561
pub fn wait(&self, timeout_ms: i32) -> io::Result<bool> {
    let mut pfd = libc::pollfd {
        fd: self.fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    if ret == 0 {
        return Ok(false); // Timeout
    }
    Ok((pfd.revents & libc::POLLIN) != 0)
}
```

The new `spin_wait_or_park()` method will add a spin phase before calling `wait()`.

### Existing FD Inheritance Pattern

The supervisor already demonstrates FD inheritance:

```rust
// From src/rs/vertebra-py/src/supervisor.rs:67-91
let notifier = InheritableNotifier::new().map_err(|e| {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
        "Failed to create notifier: {}",
        e
    ))
})?;
let notify_fd = notifier.as_raw_fd();

// Store notifier and FD numbers for later access.
{
    let mut guard = notifier_arc.lock().unwrap();
    *guard = Some(notifier);
}
{
    let mut fds = fds_arc.lock().unwrap();
    fds.notify_fd = Some(notify_fd);
}
```

This pattern extends naturally to `PyChannel` for exposing `notify_fd`.

## Conclusion

The Rust fast path implementation is architecturally sound and builds on existing infrastructure in the Vertebra codebase. Multi-model consensus confirms high confidence (8.5/10) that sync path targets are achievable, with more conservative expectations for async paths due to Python runtime overhead.

The four-phase approach provides incremental validation at each stage, clear rollback paths if issues are discovered, and reuses proven patterns from high-performance Python libraries like uvloop and pydantic. Industry precedents from LMAX Disruptor, Intel DPDK, and modern Rust/Python hybrids validate the technical approach.

Recommendation: **PROCEED** with implementation.
