# Rust Fast Path Tasks

## Overview

Total: 18 tasks across 4 phases
Dependencies: Phase 1 required for all; Phase 2 and 3 parallel; Phase 4 requires both

```
Phase 1 (Foundation)
    |
    +---> Phase 2 (Zero-Copy) ----+
    |                              |
    +---> Phase 3 (add_reader) ---+--> Phase 4 (Correlation)
```

---

## Phase 1: Hybrid Spin-Wait (5 tasks)

### Task 1.1: Add spin_wait_or_park to InheritableNotifier

**File:** `src/rs/vertebra-core/src/notify.rs`

**Description:**
Add a method that busy-loops checking `has_pending()` for `spin_us` microseconds before falling back to `wait()`.

**Implementation:**
```rust
pub fn spin_wait_or_park(&self, spin_us: u64, timeout_ms: i32) -> io::Result<bool> {
    let start = std::time::Instant::now();
    let spin_deadline = start + std::time::Duration::from_micros(spin_us);

    while std::time::Instant::now() < spin_deadline {
        if self.has_pending() {
            return Ok(true);
        }
        std::hint::spin_loop();
    }

    self.wait(timeout_ms)
}
```

**Tests:**
- [ ] test_spin_wait_immediate - returns true when data pending
- [ ] test_spin_wait_timeout - returns false after timeout
- [ ] test_spin_wait_park - falls back to wait after spin

---

### Task 1.2: Add recv_hybrid to SpscRing

**File:** `src/rs/vertebra-core/src/channel.rs`

**Description:**
Add a recv method that uses spin_wait_or_park for hybrid waiting.

**Implementation:**
```rust
pub fn recv_hybrid(
    &self,
    buf: &mut [u8],
    notifier: &InheritableNotifier,
    spin_us: u64,
    timeout_ms: i32,
) -> Result<usize, ChannelError> {
    loop {
        match self.try_recv(buf) {
            Ok(len) => return Ok(len),
            Err(ChannelError::Empty) => {
                if !notifier.spin_wait_or_park(spin_us, timeout_ms)? {
                    return Err(ChannelError::Timeout);
                }
                notifier.clear()?;
            }
            Err(e) => return Err(e),
        }
    }
}
```

---

### Task 1.3: Expose hybrid recv in PyChannel

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Add `recv_request_hybrid` method that releases GIL during wait.

**Implementation:**
```rust
fn recv_request_hybrid<'py>(
    &self,
    py: Python<'py>,
    max_len: usize,
    spin_us: u64,
    timeout_ms: u64,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut buffer = vec![0u8; max_len];
    let len = py.allow_threads(|| {
        self.recv_ring
            .recv_hybrid(&mut buffer, &self.notifier, spin_us, timeout_ms as i32)
            .map_err(map_err)
    })?;
    Ok(PyBytes::new(py, &buffer[..len]))
}
```

---

### Task 1.4: Add VERTEBRA_SPIN_US configuration

**Files:**
- `src/rs/vertebra-py/src/supervisor.rs`
- `src/py/vertebra/ipc/channel.py`

**Description:**
Read `VERTEBRA_SPIN_US` environment variable (default: 5) and use for hybrid wait.

---

### Task 1.5: Benchmark validation

**File:** `benchmarks/rust_spin_wait.rs` (new)

**Description:**
Create Rust benchmark validating:
- Immediate data: < 5us
- Blocked wake: < 10us

---

## Phase 2: Zero-Copy SlotView (4 tasks)

### Task 2.1: Add SlotView struct to channel.rs

**File:** `src/rs/vertebra-core/src/channel.rs`

**Description:**
Create SlotView wrapping ring buffer slot with lifetime tracking.

**Implementation:**
```rust
pub struct SlotView {
    ring: Arc<SpscRing>,
    slot_index: usize,
    length: usize,
    _guard: SlotGuard,
}

struct SlotGuard {
    ring: Arc<SpscRing>,
    slot_index: usize,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.ring.release_slot(self.slot_index);
    }
}

impl AsRef<[u8]> for SlotView {
    fn as_ref(&self) -> &[u8] {
        unsafe { &self.ring.slot_data(self.slot_index)[..self.length] }
    }
}
```

---

### Task 2.2: Add recv_to_slot() returning SlotView

**File:** `src/rs/vertebra-core/src/channel.rs`

**Description:**
Add method that returns view into ring buffer instead of copying.

---

### Task 2.3: Expose PySlotView with buffer protocol

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Create Python class implementing `__getbuffer__`/`__releasebuffer__`.

**Implementation:**
```rust
#[pyclass(name = "SlotView")]
struct PySlotView {
    inner: Option<SlotView>,
    exports: AtomicUsize,
}

#[pymethods]
impl PySlotView {
    fn as_memoryview<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyMemoryView>> {
        PyMemoryView::from(slf.as_any())
    }

    fn release(&mut self) -> PyResult<()> {
        if self.exports.load(Ordering::Acquire) > 0 {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "slot has active views",
            ));
        }
        self.inner.take();
        Ok(())
    }

    unsafe fn __getbuffer__(/* ... */) { /* ... */ }
    unsafe fn __releasebuffer__(/* ... */) { /* ... */ }
}
```

---

### Task 2.4: Update Python Channel to use slot views

**File:** `src/py/vertebra/ipc/channel.py`

**Description:**
Add `recv_to_view()` method returning BufferView wrapping PySlotView.

---

## Phase 3: asyncio.add_reader Integration (4 tasks)

### Task 3.1: Add notify_fd property to PyChannel

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Expose eventfd file descriptor number to Python.

**Implementation:**
```rust
#[getter]
fn notify_fd(&self) -> Option<i32> {
    self.notifier.as_ref().map(|n| n.as_raw_fd())
}
```

---

### Task 3.2: Add async recv using add_reader

**File:** `src/py/vertebra/ipc/channel.py`

**Description:**
Create `recv_fast()` async method using `add_reader` pattern.

**Implementation:**
```python
async def recv_fast(self, *, timeout_ms: int | None = None) -> tuple[str, Any]:
    """Receive using native event loop integration."""
    loop = asyncio.get_running_loop()
    fd = self._handle.notify_fd

    if fd is None or not hasattr(loop, 'add_reader'):
        return await self.receive(timeout_ms=timeout_ms)

    future: asyncio.Future[tuple[str, Any]] = loop.create_future()

    def on_readable() -> None:
        loop.remove_reader(fd)
        try:
            result = self.receive_sync(timeout_ms=0)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

    loop.add_reader(fd, on_readable)
    try:
        if timeout_ms is not None:
            return await asyncio.wait_for(future, timeout=timeout_ms / 1000.0)
        return await future
    finally:
        if not future.done():
            loop.remove_reader(fd)
```

---

### Task 3.3: Update AsyncBrokerClient

**File:** `src/py/vertebra/resources/broker.py`

**Description:**
Use eventfd-based waiting instead of `to_thread` in response loop.

---

### Task 3.4: Validate with uvloop

**File:** `src/py/tests/integration/ipc/test_uvloop.py` (new)

**Description:**
Test that `add_reader` works correctly with uvloop installed.

---

## Phase 4: Rust-Native Correlation Tracking (5 tasks)

### Task 4.1: Add protocol parsing to Rust

**File:** `src/rs/vertebra-core/src/protocol/mod.rs`

**Description:**
Add functions to parse request/response headers from bytes.

**Implementation:**
```rust
pub fn parse_request_header(data: &[u8]) -> Result<RequestHeader, ProtocolError> {
    if data.len() < HEADER_SIZE {
        return Err(ProtocolError::TooShort);
    }
    Ok(RequestHeader {
        id: u64::from_le_bytes(data[0..8].try_into().unwrap()),
        flags: u32::from_le_bytes(data[8..12].try_into().unwrap()),
        payload_len: u32::from_le_bytes(data[12..16].try_into().unwrap()),
    })
}
```

---

### Task 4.2: Add PendingMap to PyFastChannel

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Create `FastChannel` with thread-safe pending request map.

**Dependencies:**
- Add `dashmap = "5.5"` to Cargo.toml

**Implementation:**
```rust
#[pyclass(name = "FastChannel")]
struct PyFastChannel {
    request_ring: SpscRing,
    response_ring: SpscRing,
    notifier: InheritableNotifier,
    pending: DashMap<u64, PendingSlot>,
    next_id: AtomicU64,
}

struct PendingSlot {
    view: Option<SlotView>,
}
```

---

### Task 4.3: Add send_request_fast()

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Send request and register correlation ID in Rust pending map.

---

### Task 4.4: Add poll_response_fast()

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Check for matching response by correlation ID.

---

### Task 4.5: Add request_response_fast() combining both

**File:** `src/rs/vertebra-py/src/lib.rs`

**Description:**
Single method with single GIL acquisition for entire cycle.

**Implementation:**
```rust
fn request_response_fast<'py>(
    &self,
    py: Python<'py>,
    procedure_id: &str,
    payload: &[u8],
    spin_us: u64,
    timeout_ms: u64,
) -> PyResult<Bound<'py, PySlotView>> {
    let id = self.next_id.fetch_add(1, Ordering::Relaxed);

    // Build message in Rust
    let message = encode_request(procedure_id, payload, id);

    // Release GIL during send and wait
    let view = py.allow_threads(|| {
        self.request_ring.send(&message, 0)?;
        self.notifier.notify()?;

        loop {
            if let Some(v) = self.try_recv_matching(id)? {
                return Ok(v);
            }
            if !self.notifier.spin_wait_or_park(spin_us, timeout_ms as i32)? {
                return Err(ChannelError::Timeout);
            }
        }
    }).map_err(map_err)?;

    // Wrap in Python (GIL reacquired automatically)
    Py::new(py, PySlotView { inner: Some(view), exports: AtomicUsize::new(0) })
}
```

---

## Validation Tasks

### Benchmark Suite

- [ ] Create `benchmarks/ipc_fast_path.py`
- [ ] Sync immediate benchmark (target: <10us p99)
- [ ] Sync blocked benchmark (target: <15us p99)
- [ ] Async benchmark (target: <20us p99)
- [ ] 64 concurrent benchmark (target: <20us p99)
- [ ] Allocation check (target: 0 bytes/msg)

### Documentation

- [ ] Update `docs/architecture/python-ipc-guide.md`
- [ ] Update `docs/architecture/performance-tuning.md`
- [ ] Add VERTEBRA_SPIN_US to configuration docs

---

## Summary

| Phase | Tasks | Key Deliverable |
|-------|-------|-----------------|
| 1 | 5 | Hybrid spin-wait in Rust |
| 2 | 4 | Zero-copy SlotView API |
| 3 | 4 | asyncio.add_reader integration |
| 4 | 5 | Rust-native correlation tracking |
| **Total** | **18** | **Sub-10us sync, sub-20us async** |
