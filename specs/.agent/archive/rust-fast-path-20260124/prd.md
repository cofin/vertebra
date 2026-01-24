# PRD: Rust Fast Path for Sub-10us Python IPC

## Intelligence Context

### Complexity Assessment

- **Classification**: High
- **Estimated Phases**: 4
- **Risk Level**: Medium (careful lifetime management required)
- **Breaking Changes**: None (additive API, existing methods preserved)

### Similar Features Referenced

1. `src/rs/vertebra-core/src/notify.rs` - InheritableNotifier with eventfd (extend with spin_wait_or_park)
2. `src/rs/vertebra-py/src/lib.rs:258-613` - PyBuffer with buffer protocol (pattern for PySlotView)
3. `src/py/vertebra/ipc/channel.py:87-165` - BufferView returning memoryview (Python-side pattern)
4. `src/rs/vertebra-py/src/supervisor.rs` - FD inheritance via environment variables

### Patterns to Follow

- **Hybrid Spin-Wait**: Spin for configurable microseconds, then block on eventfd
- **Buffer Protocol**: Use `__getbuffer__`/`__releasebuffer__` for zero-copy memoryview
- **FD Inheritance**: Pass eventfd via `VERTEBRA_NOTIFY_FD` environment variable
- **Event Loop Integration**: Use `asyncio.add_reader()` for non-blocking async

### Multi-Model Consensus

- **Gemini 3 Pro (FOR)**: 9/10 confidence, sub-10us achievable with hybrid spinning
- **Gemini 3 Pro (AGAINST)**: 8/10 confidence, realistic floor 15-25us for async
- **Synthesized**: 8.5/10 confidence, sync <10us achievable, async <20us realistic

### Detected Stack and Commands

- **Languages**: Python 3.12+, Rust, TypeScript (Bun)
- **Test Commands**: `make test`, `make rs-test`, `make test-all`
- **Lint Command**: `make lint-all`
- **Build Command**: `make install`

---

## Problem Statement

### User Problem Being Solved

The current Python IPC implementation achieves approximately 55 microseconds p99 latency, which falls short of the world-class target of less than 10 microseconds established in the IPC Performance Optimization PRD. The bottleneck is not the underlying Rust transport (which achieves sub-microsecond latency) but rather:

1. **GIL overhead**: 4-6 GIL acquisitions per request/response cycle
2. **Object allocation**: `PyBytes::new()` on every recv (2-5us + GC jitter)
3. **asyncio.to_thread()**: Thread pool dispatch adds 20us per async call
4. **Python spin-wait**: `time.sleep(0.001)` has 1ms granularity

TypeScript benchmarks prove the transport CAN achieve sub-10us (7.64us p99), demonstrating this is a Python integration problem, not an architectural limitation.

### Business Value

1. **World-Class Performance**: Transform from 55us to <10us, matching or exceeding competitors
2. **Real-Time Use Cases**: Enable HFT, robotics, and interactive AI inference in Python
3. **Developer Productivity**: Python developers get Rust-level performance without learning Rust
4. **Competitive Advantage**: Only Python framework achieving sub-10us IPC latency

### Success Criteria

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Sync p99 (immediate) | ~55us | <10us | `ipc_fast_path` benchmark |
| Sync p99 (blocked) | ~55us | <15us | `ipc_fast_path` benchmark |
| Async p99 | ~55us | <20us | `ipc_fast_path` benchmark |
| Hot path allocations | 1.8 bytes/msg | 0 | tracemalloc |
| 64x concurrent p99 | N/A | <20us | multiplexing benchmark |

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Hybrid Spin-Wait in Rust

- [ ] Add `spin_wait_or_park(spin_us, timeout_ms)` to `InheritableNotifier`
- [ ] Spin phase uses `std::hint::spin_loop()` for CPU efficiency
- [ ] Configurable spin duration via `VERTEBRA_SPIN_US` (default: 5)
- [ ] Fall back to eventfd block after spin timeout
- [ ] Works on Linux (eventfd) and macOS (pipe)

#### FR-2: Zero-Copy SlotView API

- [ ] Add `SlotView` struct wrapping ring buffer slot with lifetime tracking
- [ ] Add `recv_to_slot()` method returning `SlotView` instead of copying
- [ ] `PySlotView` implements Python buffer protocol
- [ ] Python can use `memoryview()` on returned object
- [ ] Slot guard prevents ring advance while view held
- [ ] Automatic release when Python releases memoryview

#### FR-3: asyncio.add_reader Integration

- [ ] Add `notify_fd` property to `PyChannel` for eventfd access
- [ ] Add `recv_fast()` async method using `add_reader`
- [ ] No `asyncio.to_thread()` in hot path
- [ ] Compatible with default asyncio and uvloop
- [ ] Fallback to `to_thread` for unsupported event loops

#### FR-4: Rust-Native Correlation Tracking

- [ ] Add `FastChannel` with `DashMap<u64, PendingRequest>`
- [ ] Parse protocol headers in Rust (correlation ID extraction)
- [ ] Single GIL acquisition per complete request/response cycle
- [ ] Support 64+ concurrent in-flight requests
- [ ] Out-of-order response matching via correlation ID

### Non-Functional Requirements

#### NFR-1: Performance Targets

- [ ] Sync immediate path: p99 < 10 microseconds
- [ ] Sync blocked path: p99 < 15 microseconds
- [ ] Async path: p99 < 20 microseconds
- [ ] Zero per-message allocations in hot path
- [ ] 64 concurrent requests same latency as single

#### NFR-2: Backwards Compatibility

- [ ] Existing `Channel` API unchanged
- [ ] Existing `send_sync`/`recv_sync` methods still work
- [ ] New fast-path methods are additive
- [ ] No breaking changes to public interfaces

#### NFR-3: Platform Support

- [ ] Linux: Full optimization (eventfd + hybrid spin)
- [ ] macOS: Good performance (pipe + hybrid spin)
- [ ] Windows: Future work (not in scope)

---

## Technical Approach

### Architecture Overview

```
CURRENT (55us p99):
+--------+  to_thread  +--------+  GIL  +--------+  GIL  +--------+
| Python |------------>| Thread |------>| PyO3   |------>| Rust   |
| async  |  (20us)     | Pool   |(2us)  | recv() |(3us)  | ring   |
+--------+             +--------+       +--------+       +--------+
                                            |
                                     PyBytes::new (3us)
                                            |
                                     GIL release (2us)


OPTIMIZED (<10us p99):
+--------+  add_reader +--------+       +--------+
| Python |------------>| eventfd|------>| Rust   |
| async  |  (0us)      | wake   |(1us)  | fast   |
+--------+             +--------+       | path   |
    |                                   +--------+
    |                                       |
    +--- memoryview (0 alloc) <-------------+
```

### Phase 1: Hybrid Spin-Wait

**Files to modify:**
- `src/rs/vertebra-core/src/notify.rs`
- `src/rs/vertebra-core/src/channel.rs`
- `src/rs/vertebra-py/src/lib.rs`

**New methods:**
```rust
impl InheritableNotifier {
    /// Spin for up to spin_us microseconds, then park on eventfd.
    /// Returns true if notification received, false on timeout.
    pub fn spin_wait_or_park(&self, spin_us: u64, timeout_ms: i32) -> io::Result<bool> {
        let start = std::time::Instant::now();
        let spin_deadline = start + std::time::Duration::from_micros(spin_us);

        // Spin phase
        while std::time::Instant::now() < spin_deadline {
            if self.has_pending() {
                return Ok(true);
            }
            std::hint::spin_loop();
        }

        // Park phase
        self.wait(timeout_ms)
    }
}
```

### Phase 2: Zero-Copy SlotView

**Files to modify:**
- `src/rs/vertebra-core/src/channel.rs`
- `src/rs/vertebra-py/src/lib.rs`

**New struct:**
```rust
/// Zero-copy view into a ring buffer slot.
/// Prevents slot reuse while view exists.
pub struct SlotView {
    ring: Arc<SpscRing>,
    slot_index: usize,
    length: usize,
    _guard: SlotGuard,
}

impl AsRef<[u8]> for SlotView {
    fn as_ref(&self) -> &[u8] {
        unsafe { self.ring.slot_data(self.slot_index) }
    }
}
```

**Python wrapper:**
```rust
#[pyclass(name = "SlotView")]
struct PySlotView {
    inner: Option<SlotView>,
    exports: AtomicUsize,
}

unsafe impl pyo3::buffer::Element for PySlotView {}

#[pymethods]
impl PySlotView {
    fn as_memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        // Implementation follows PyBuffer pattern from lib.rs:576-599
    }
}
```

### Phase 3: asyncio.add_reader Integration

**Files to modify:**
- `src/py/vertebra/ipc/channel.py`
- `src/py/vertebra/resources/broker.py`

**New async method:**
```python
async def recv_fast(
    self,
    *,
    timeout_ms: int | None = None,
    params_type: type | None = None,
) -> tuple[str, Any]:
    """Receive using native event loop integration.

    Uses asyncio.add_reader with inherited eventfd for instant wakeups.
    Falls back to to_thread for unsupported event loops.
    """
    loop = asyncio.get_running_loop()

    # Check for add_reader support (asyncio, uvloop)
    if not hasattr(loop, 'add_reader'):
        return await self.receive(timeout_ms=timeout_ms, params_type=params_type)

    fd = self._handle.notify_fd()
    if fd is None:
        return await self.receive(timeout_ms=timeout_ms, params_type=params_type)

    future: asyncio.Future[tuple[str, Any]] = loop.create_future()

    def on_readable() -> None:
        loop.remove_reader(fd)
        try:
            result = self.receive_sync(timeout_ms=0, params_type=params_type)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

    loop.add_reader(fd, on_readable)

    try:
        if timeout_ms is not None:
            return await asyncio.wait_for(future, timeout=timeout_ms / 1000.0)
        return await future
    except asyncio.CancelledError:
        loop.remove_reader(fd)
        raise
```

### Phase 4: Rust-Native Correlation Tracking

**Files to modify:**
- `src/rs/vertebra-py/src/lib.rs`
- `src/rs/vertebra-core/src/protocol/mod.rs`

**New class:**
```rust
#[pyclass(name = "FastChannel")]
struct PyFastChannel {
    request_ring: SpscRing,
    response_ring: SpscRing,
    notifier: InheritableNotifier,
    pending: DashMap<u64, PendingSlot>,
    next_id: AtomicU64,
}

#[pymethods]
impl PyFastChannel {
    /// Send request and wait for response with minimal GIL interaction.
    fn request_response<'py>(
        &self,
        py: Python<'py>,
        procedure_id: &str,
        payload: &[u8],
        spin_us: u64,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PySlotView>> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        // Encode and send (GIL held briefly)
        let message = encode_request(procedure_id, payload, id);

        // Release GIL during wait
        py.allow_threads(|| {
            self.request_ring.send(&message, 0)?;
            self.notifier.notify()?;

            // Wait for matching response
            loop {
                if let Some(view) = self.try_recv_response(id)? {
                    return Ok(view);
                }
                if !self.notifier.spin_wait_or_park(spin_us, timeout_ms as i32)? {
                    return Err(ChannelError::Timeout);
                }
            }
        })?;

        // Wrap in Python object (GIL reacquired)
        Ok(PySlotView::new(view))
    }
}
```

### Database Changes

None. This PRD focuses on IPC performance optimization.

### API Changes

All changes are **additive**. Existing methods are preserved for backwards compatibility.

**New Python methods:**
- `Channel.recv_fast()` - async recv using add_reader
- `Channel.recv_to_view()` - sync recv returning SlotView

**New Rust methods:**
- `InheritableNotifier::spin_wait_or_park()`
- `SpscRing::recv_to_slot()`
- `PyFastChannel::request_response()`

### Documentation Updates Required

- `docs/architecture/python-ipc-guide.md` - Add fast-path API documentation
- `docs/architecture/performance-tuning.md` - Add spin-wait configuration

---

## Testing Strategy

### Unit Tests

**Rust (`src/rs/vertebra-core/src/notify.rs`):**
```rust
#[test]
fn test_spin_wait_immediate() {
    let notifier = InheritableNotifier::new().unwrap();
    notifier.notify().unwrap();
    let result = notifier.spin_wait_or_park(5, 100).unwrap();
    assert!(result, "should return immediately when data pending");
}

#[test]
fn test_spin_wait_timeout() {
    let notifier = InheritableNotifier::new().unwrap();
    let start = Instant::now();
    let result = notifier.spin_wait_or_park(5, 10).unwrap();
    let elapsed = start.elapsed();
    assert!(!result, "should timeout");
    assert!(elapsed.as_millis() >= 10, "should wait at least 10ms");
}
```

**Python (`src/py/tests/unit/ipc/test_fast_path.py`):**
```python
def test_slot_view_memoryview():
    """SlotView should support memoryview protocol."""
    client, server = Channel.create_pair()
    server.send_sync("test", {"key": "value"})
    view = client.recv_to_view()
    mv = memoryview(view)
    assert len(mv) > 0
    view.release()

async def test_recv_fast_with_add_reader():
    """recv_fast should use add_reader, not to_thread."""
    client, server = Channel.create_pair()
    server.send_sync("test", {"key": "value"})
    result = await client.recv_fast()
    assert result[0] == "test"
```

### Integration Tests

```bash
# Test hybrid spin-wait with real IPC
uv run pytest src/py/tests/integration/ipc/test_spin_wait.py -v

# Test slot view lifecycle
uv run pytest src/py/tests/integration/ipc/test_slot_view.py -v

# Test add_reader with uvloop
uv run pytest src/py/tests/integration/ipc/test_uvloop.py -v
```

### Benchmark Tests

```bash
# Validate sync targets
uv run python benchmarks/ipc_fast_path.py --target-p99-us=10

# Validate async targets
uv run python benchmarks/ipc_fast_path.py --async --target-p99-us=20

# Validate multiplexing
uv run python benchmarks/ipc_fast_path.py --concurrent=64 --target-p99-us=20

# Validate zero allocations
uv run python benchmarks/ipc_fast_path.py --check-allocs
```

### Coverage Requirements

- Unit tests: 90%+ coverage for new code
- All critical paths tested (spin, block, timeout)
- Platform parity verified (Linux + macOS)

---

## Implementation Notes

### Pattern Deviations

1. **Spin-wait before blocking**: Unlike typical blocking implementations, we spin first to capture immediate responses. This trades a small amount of CPU for significantly lower latency.

2. **SlotView with guard**: Unlike typical recv() that copies data, SlotView holds a reference that blocks ring advance. This is safe because we control the ring buffer implementation.

3. **Header parsing in Rust**: Duplicates logic from Python `channel.py`, but required for single-GIL-acquisition optimization.

### Dependencies

**New Rust dependencies:**
- `dashmap = "5.5"` - Thread-safe concurrent HashMap for pending requests

**No new Python dependencies.**

### Migration Considerations

No migration required. This is an additive optimization. Users can opt-in to fast-path methods when ready.

### Rollback Strategy

1. Fast-path methods can be disabled via environment variable
2. Existing methods remain unchanged
3. Full rollback via version downgrade if needed

---

## Verification Checklist

### Functional Verification

- [ ] `make test` passes
- [ ] `make rs-test` passes
- [ ] `make test-all` passes
- [ ] `make lint-all` passes

### Performance Verification

- [ ] Sync immediate p99 < 10 microseconds
- [ ] Sync blocked p99 < 15 microseconds
- [ ] Async p99 < 20 microseconds
- [ ] Zero allocations in hot path (tracemalloc)
- [ ] 64 concurrent requests same latency

### Platform Verification

- [ ] Linux: Full performance achieved
- [ ] macOS: Acceptable performance (pipe fallback)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| SlotView held too long | Medium | High | Guard with timeout, copy fallback |
| add_reader incompatible | Low | Medium | Detect + to_thread fallback |
| Spin-wait CPU waste | Low | Low | Configurable spin_us, default 5 |
| DashMap contention | Low | Medium | Sharded map, low contention design |

---

## Detailed Technical Specifications

### Wire Format

The IPC wire format uses 16-byte headers for zero-parse-cost multiplexing:

```
Request Wire Format:
+----------------+----------------+----------------+----------------+
|   id (u64)     |   flags (u32)  | payload_len(u32)|
+----------------+----------------+----------------+----------------+
|   proc_id_len (u16)   |   procedure_id (variable)   |   payload     |
+----------------+----------------+----------------+----------------+

Response Wire Format:
+----------------+----------------+----------------+----------------+
|   id (u64)     |   flags (u32)  | payload_len(u32)|   payload     |
+----------------+----------------+----------------+----------------+
```

All fields are little-endian. The `id` field carries the correlation ID for request/response matching.

### Flag Definitions

```rust
pub mod Flags {
    pub const NONE: u32 = 0x0000;
    pub const PRIORITY: u32 = 0x0001;      // High-priority message
    pub const COMPRESSED: u32 = 0x0002;    // Payload is compressed
    pub const FIRE_AND_FORGET: u32 = 0x0004; // No response expected
    pub const BATCH: u32 = 0x0008;         // Part of a batch
    pub const OK: u32 = 0x0100;            // Response: success
    pub const ERROR: u32 = 0x0200;         // Response: error
}
```

### SlotView Lifetime Management

The SlotView struct manages ring buffer slot lifetimes safely:

```rust
/// Guard that blocks ring advance while slot is in use.
/// Dropped when all references (including Python memoryviews) are released.
struct SlotGuard {
    ring: Arc<SpscRing>,
    slot_index: usize,
    released: AtomicBool,
}

impl SlotGuard {
    fn new(ring: Arc<SpscRing>, slot_index: usize) -> Self {
        ring.acquire_slot(slot_index);  // Mark slot as in-use
        Self {
            ring,
            slot_index,
            released: AtomicBool::new(false),
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.ring.release_slot(self.slot_index);
        }
    }
}

/// Zero-copy view into a ring buffer slot.
pub struct SlotView {
    guard: Arc<SlotGuard>,
    length: usize,
}

impl SlotView {
    /// Get the slot data as a byte slice.
    /// Safe because guard prevents slot reuse.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: SlotGuard ensures slot is not reused while view exists
        unsafe {
            let ring = &self.guard.ring;
            &ring.slot_data(self.guard.slot_index)[..self.length]
        }
    }
}
```

### SpscRing Slot Management

New methods for slot-based recv:

```rust
impl SpscRing {
    /// Receive to a slot view instead of copying.
    /// Returns None if ring is empty.
    pub fn try_recv_to_slot(&self) -> Result<Option<SlotView>, ChannelError> {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        if tail == head {
            return Ok(None);  // Empty
        }

        let slot_index = (tail % self.capacity) as usize;
        let length = self.read_slot_length(slot_index)?;

        // Create guard before advancing tail
        let guard = Arc::new(SlotGuard::new(
            Arc::clone(&self.region),
            slot_index,
        ));

        // Advance tail (consumer side)
        self.tail.store(tail + 1, Ordering::Release);

        Ok(Some(SlotView { guard, length }))
    }

    /// Mark a slot as acquired (in-use).
    fn acquire_slot(&self, index: usize) {
        // Set in-flight flag for this slot
        let flags = self.slot_flags(index);
        flags.fetch_or(SLOT_IN_FLIGHT, Ordering::AcqRel);
    }

    /// Mark a slot as released (available for reuse).
    fn release_slot(&self, index: usize) {
        let flags = self.slot_flags(index);
        flags.fetch_and(!SLOT_IN_FLIGHT, Ordering::AcqRel);
    }
}
```

### Event Loop Detection

For Phase 3, we detect the event loop type to choose the optimal strategy:

```python
def _get_recv_strategy(self) -> str:
    """Detect event loop type and return optimal recv strategy."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return "sync"  # No event loop

    loop_name = type(loop).__name__

    # uvloop provides add_reader
    if loop_name == "Loop" and "uvloop" in type(loop).__module__:
        return "add_reader"

    # Default asyncio SelectorEventLoop provides add_reader
    if hasattr(loop, "add_reader"):
        return "add_reader"

    # ProactorEventLoop (Windows) does not support add_reader
    if loop_name == "ProactorEventLoop":
        return "to_thread"

    # trio, curio: use their native primitives (future work)
    return "to_thread"
```

### DashMap Configuration

For Phase 4 correlation tracking, we configure DashMap for optimal performance:

```rust
use dashmap::DashMap;

impl PyFastChannel {
    fn new(/* ... */) -> Self {
        // Pre-size the map for expected concurrency
        // Shards default to num_cpus * 4, which is good for most cases
        let pending: DashMap<u64, PendingSlot> = DashMap::with_capacity(128);

        Self {
            // ...
            pending,
            next_id: AtomicU64::new(0),
        }
    }
}
```

DashMap uses sharded locking internally, so concurrent access from multiple threads has minimal contention.

### Benchmark Implementation

The benchmark suite validates all performance targets:

```python
# benchmarks/ipc_fast_path.py

import asyncio
import statistics
import time
import tracemalloc
from vertebra.ipc.channel import Channel

def benchmark_sync_immediate(iterations: int = 10000) -> dict:
    """Benchmark sync recv when data is already available."""
    client, server = Channel.create_pair()

    # Pre-populate with messages
    for i in range(iterations):
        server.respond_sync("test", {"i": i})

    # Measure recv time
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter_ns()
        client.recv_to_view()  # Fast path
        elapsed = time.perf_counter_ns() - start
        latencies.append(elapsed / 1000)  # Convert to us

    return {
        "p50": statistics.median(latencies),
        "p99": statistics.quantiles(latencies, n=100)[98],
        "p999": statistics.quantiles(latencies, n=1000)[998],
        "mean": statistics.mean(latencies),
    }

async def benchmark_async_add_reader(iterations: int = 10000) -> dict:
    """Benchmark async recv using add_reader."""
    client, server = Channel.create_pair()

    async def producer():
        for i in range(iterations):
            server.respond_sync("test", {"i": i})
            await asyncio.sleep(0)  # Yield to consumer

    async def consumer():
        latencies = []
        for _ in range(iterations):
            start = time.perf_counter_ns()
            await client.recv_fast()  # add_reader path
            elapsed = time.perf_counter_ns() - start
            latencies.append(elapsed / 1000)
        return latencies

    producer_task = asyncio.create_task(producer())
    latencies = await consumer()
    await producer_task

    return {
        "p50": statistics.median(latencies),
        "p99": statistics.quantiles(latencies, n=100)[98],
        "p999": statistics.quantiles(latencies, n=1000)[998],
        "mean": statistics.mean(latencies),
    }

def benchmark_allocations(iterations: int = 10000) -> dict:
    """Verify zero allocations in hot path."""
    client, server = Channel.create_pair()

    # Warmup
    for i in range(100):
        server.respond_sync("test", {"i": i})
        client.recv_to_view()

    # Pre-populate
    for i in range(iterations):
        server.respond_sync("test", {"i": i})

    # Measure allocations
    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()

    for _ in range(iterations):
        view = client.recv_to_view()
        view.release()

    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    diff = snapshot_after.compare_to(snapshot_before, "lineno")
    total_alloc = sum(stat.size_diff for stat in diff if stat.size_diff > 0)

    return {
        "total_bytes": total_alloc,
        "bytes_per_msg": total_alloc / iterations,
        "target_met": total_alloc / iterations < 1.0,
    }
```

---

## Consensus Sources

- **Gemini 3 Pro (FOR)**: 9/10 confidence - Hybrid spinning mandatory, eliminate to_thread
- **Gemini 3 Pro (AGAINST)**: 8/10 confidence - Realistic floor 15-25us for async
- **Synthesized**: 8.5/10 confidence - Sync <10us achievable, async <20us realistic

---

## Implementation Phases

### Phase 1: Hybrid Spin-Wait (Foundation)

**Goal:** Add configurable spin-then-park to InheritableNotifier and SpscRing.

**Tasks:**
1. Add `spin_wait_or_park()` to `InheritableNotifier` in `notify.rs`
2. Add `recv_hybrid()` to `SpscRing` in `channel.rs`
3. Expose `recv_request_hybrid()` in `PyChannel`
4. Add `VERTEBRA_SPIN_US` environment variable
5. Create Rust benchmark for spin-wait validation

**Validation:** Sync immediate < 25us (2x improvement)

### Phase 2: Zero-Copy SlotView

**Goal:** Return memoryview instead of PyBytes to eliminate allocation.

**Tasks:**
1. Add `SlotView` struct with lifetime tracking to `channel.rs`
2. Add `recv_to_slot()` returning `SlotView`
3. Add `PySlotView` with buffer protocol to `lib.rs`
4. Update Python `Channel` to use slot views optionally

**Validation:** Zero allocations in hot path (tracemalloc)

### Phase 3: asyncio.add_reader Integration

**Goal:** Replace asyncio.to_thread with native FD polling.

**Tasks:**
1. Add `notify_fd` property to `PyChannel`
2. Add `recv_fast()` async method using `add_reader`
3. Update `AsyncBrokerClient` to use fast path
4. Test with uvloop

**Validation:** Async < 30us (2x improvement)

### Phase 4: Rust-Native Correlation Tracking

**Goal:** Single GIL acquisition per request/response cycle.

**Tasks:**
1. Add protocol header parsing to Rust
2. Add `FastChannel` with `DashMap<u64, PendingSlot>`
3. Add `request_response()` method
4. Benchmark with 64 concurrent requests

**Validation:** Sync < 10us, Async < 20us (final targets)
