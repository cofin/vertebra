# Task Breakdown: 100K-200K RPS Performance Target

## Phase Overview

| Phase | Focus | Expected Gain | Status |
|-------|-------|---------------|--------|
| P0 | Syscall + GIL | 3-5x (40-60K RPS) | ✅ Complete |
| P1 | Batching + Ordering | 2x (80-120K RPS) | Pending |
| P2 | Allocation + Polish | 1.5x (120-200K RPS) | Pending |

---

## Phase 0: Critical Path (Target: 40-60K RPS)

### Task 0.1: Add Waiter Count to Channel
**Priority:** P0-CRITICAL
**Effort:** Medium
**Risk:** High (concurrency bugs)

**Description:**
Add `wait_count: AtomicU32` to `SpscChannel` and `MpmcChannel` structs. This tracks how many threads are parked waiting for data, enabling conditional wake.

**File:** `src/rs/vertebra-core/src/channel.rs`

**Acceptance Criteria:**
- [x] `wait_count` field added to channel structs
- [x] Initial value is 0
- [x] Field is cache-line aligned (prevent false sharing)
- [x] Unit test verifies counter increments/decrements correctly

**Status:** ✅ Complete - Added `send_waiters` and `recv_waiters` to `ShmHeader` in `buffer.rs`

**Implementation Notes:**
```rust
#[repr(C, align(64))]  // Cache line alignment
pub struct ChannelHeader {
    // ... existing fields ...
    wait_count: AtomicU32,  // NEW: waiter tracking
}
```

---

### Task 0.2: Implement Conditional Wake for Send
**Priority:** P0-CRITICAL
**Effort:** Medium
**Risk:** High (lost wakeup bug)

**Description:**
Modify `send()` to only call `wake_one()` if `wait_count > 0`. This eliminates the 8µs syscall overhead when receiver is actively polling.

**File:** `src/rs/vertebra-core/src/channel.rs` (lines 280-290)

**Acceptance Criteria:**
- [x] `wake_one()` called conditionally
- [x] Memory ordering is correct (Acquire load of wait_count)
- [x] Proptest with 1M send/recv cycles shows no lost messages
- [ ] Benchmark shows syscall reduction

**Status:** ✅ Complete - Conditional wake added to `try_send` and `try_recv`

---

### Task 0.3: Implement Double-Check Pattern for Recv
**Priority:** P0-CRITICAL
**Effort:** High
**Risk:** High (most complex change)

**Description:**
Modify blocking `recv()` to increment `wait_count` BEFORE parking, then double-check for data. This prevents the lost wakeup race condition.

**File:** `src/rs/vertebra-core/src/channel.rs` (lines 360-390)

**Acceptance Criteria:**
- [x] wait_count incremented with Release ordering
- [x] Double-check for data after increment
- [x] wait_count decremented after wake (Relaxed OK)
- [x] Stress test: 100 threads, 1M messages, zero lost

**Status:** ✅ Complete - Double-check pattern implemented in all blocking methods

**Implementation Notes:**
```rust
fn recv(&self, buf: &mut [u8]) -> Result<usize, ChannelError> {
    loop {
        // Fast path
        if let Ok(len) = self.try_recv(buf) {
            return Ok(len);
        }

        // CRITICAL: Increment BEFORE parking
        self.wait_count.fetch_add(1, Ordering::Release);

        // CRITICAL: Re-check after increment
        if let Ok(len) = self.try_recv(buf) {
            self.wait_count.fetch_sub(1, Ordering::Relaxed);
            return Ok(len);
        }

        // Safe to park
        self.notifier().wait()?;
        self.wait_count.fetch_sub(1, Ordering::Relaxed);
    }
}
```

---

### Task 0.4: Implement Optimistic GIL for try_recv
**Priority:** P0-CRITICAL
**Effort:** Medium
**Risk:** Medium

**Description:**
Modify PyO3 bindings to attempt `try_recv` with GIL held. Only release GIL if operation would block. This eliminates 2-5µs toggle overhead.

**File:** `src/rs/vertebra-py/src/lib.rs` (lines 610-640)

**Acceptance Criteria:**
- [x] Fast path: try_recv with GIL, return immediately if data available
- [x] Slow path: allow_threads() for blocking recv
- [x] Python tests pass
- [ ] Microbenchmark shows 2-3µs improvement per recv

**Status:** ✅ Complete - Optimistic GIL pattern implemented for all recv/send methods

**Implementation Notes:**
```rust
fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
    // FAST PATH: Try with GIL held
    let mut buffer = vec![0u8; self.buffer_size];
    match self.ring.try_recv(&mut buffer) {
        Ok(len) => return Ok(PyBytes::new_bound(py, &buffer[..len])),
        Err(ChannelError::Empty) => {}  // Continue to slow path
        Err(e) => return Err(e.into()),
    }

    // SLOW PATH: Release GIL for blocking
    let len = py.allow_threads(|| self.ring.recv(&mut buffer))?;
    Ok(PyBytes::new_bound(py, &buffer[..len]))
}
```

---

### Task 0.5: Implement Optimistic GIL for try_send
**Priority:** P0-HIGH
**Effort:** Medium
**Risk:** Low

**Description:**
Apply same optimistic GIL pattern to send operations.

**File:** `src/rs/vertebra-py/src/lib.rs` (lines 580-610)

**Acceptance Criteria:**
- [x] Fast path: try_send with GIL
- [x] Slow path: allow_threads() for blocking
- [x] Python tests pass

**Status:** ✅ Complete - Same optimistic pattern applied to all send methods

---

### Task 0.6: Remove Unnecessary Dict Copies
**Priority:** P0-HIGH
**Effort:** Low
**Risk:** Low

**Description:**
Remove `payload = dict(payload)` copies in broker request methods.

**File:** `src/py/vertebra/messaging/broker.py` (lines 166, 469)

**Acceptance Criteria:**
- [x] Dict copy removed or made conditional
- [x] ID field added in-place or via new dict literal
- [x] Existing tests pass

**Status:** ✅ Complete - Dict copies replaced with spread operator pattern

**Implementation Notes:**
```python
# BEFORE
payload = dict(payload)
payload["id"] = self._next_id()

# AFTER
payload = {"id": self._next_id(), **payload}
```

---

### Task 0.7: Benchmark P0 Changes
**Priority:** P0-HIGH
**Effort:** Low
**Risk:** None

**Description:**
Run comprehensive benchmarks after P0 changes to validate 3-5x improvement.

**Commands:**
```bash
# Baseline (run before P0 changes)
make bench-servers --servers vertebra,granian --scenarios hello,json > baseline_p0.json

# After P0
make bench-servers --servers vertebra,granian --scenarios hello,json > after_p0.json

# Compare
python scripts/compare_benchmarks.py baseline_p0.json after_p0.json
```

**Acceptance Criteria:**
- [ ] `hello` endpoint: 40,000+ RPS (3.3x improvement)
- [ ] `json` endpoint: 30,000+ RPS (3x improvement)
- [ ] p99 latency: <10ms

---

## Phase 1: Batching & Ordering (Target: 80-120K RPS)

### Task 1.1: Implement recv_batch in Rust
**Priority:** P1-HIGH
**Effort:** Medium
**Risk:** Low

**Description:**
Add `recv_batch(max: usize)` method to drain multiple messages per call.

**File:** `src/rs/vertebra-core/src/channel.rs`

**Acceptance Criteria:**
- [ ] Returns Vec of (request_id, payload) tuples
- [ ] Drains up to `max` messages without blocking
- [ ] Returns empty Vec if ring is empty
- [ ] Unit tests for partial drain, full drain, empty ring

**Implementation Notes:**
```rust
pub fn recv_batch(&self, max: usize) -> Result<Vec<(u64, Vec<u8>)>, ChannelError> {
    let mut batch = Vec::with_capacity(max.min(128));
    while batch.len() < max {
        match self.try_recv_with_header() {
            Ok((header, data)) => batch.push((header.id, data.to_vec())),
            Err(ChannelError::Empty) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(batch)
}
```

---

### Task 1.2: Expose recv_batch to Python
**Priority:** P1-HIGH
**Effort:** Medium
**Risk:** Low

**Description:**
Add PyO3 binding for recv_batch.

**File:** `src/rs/vertebra-py/src/lib.rs`

**Acceptance Criteria:**
- [ ] `recv_batch(max=100)` method available
- [ ] Returns list of (int, bytes) tuples
- [ ] Uses allow_threads() for potential blocking
- [ ] Python tests pass

---

### Task 1.3: Update Worker to Use Batch Processing
**Priority:** P1-HIGH
**Effort:** High
**Risk:** Medium (behavioral change)

**Description:**
Modify worker to drain batch of requests and process concurrently with asyncio.gather().

**File:** `src/py/vertebra/core/worker.py`

**Acceptance Criteria:**
- [ ] `on_control_readable()` uses recv_batch
- [ ] Batch processed via asyncio.gather()
- [ ] Request ordering preserved via correlation IDs
- [ ] Integration tests pass

**Implementation Notes:**
```python
def on_control_readable(self) -> None:
    # Drain batch
    batch = self._ring.recv_batch(max=100)
    if not batch:
        return

    # Process concurrently
    tasks = [
        asyncio.create_task(self._handle_request(req_id, payload))
        for req_id, payload in batch
    ]
    asyncio.gather(*tasks)
```

---

### Task 1.4: Relax Memory Ordering for Fast-Path Checks
**Priority:** P1-MEDIUM
**Effort:** Medium
**Risk:** Medium (correctness critical)

**Description:**
Change `has_data()` and fast-path head/tail loads to use Relaxed ordering. Add explicit Acquire fence before payload access.

**File:** `src/rs/vertebra-core/src/channel.rs` (lines 245-250, 273-274, 342-343)

**Acceptance Criteria:**
- [ ] `has_data()` uses Relaxed loads
- [ ] `try_recv` fast-path check uses Relaxed
- [ ] Acquire fence added before `memcpy` of payload
- [ ] MIRI passes with no data races
- [ ] ThreadSanitizer clean

**Implementation Notes:**
```rust
pub fn has_data(&self) -> bool {
    let head = self.read_head().load(Ordering::Relaxed);
    let tail = self.write_head().load(Ordering::Relaxed);
    head != tail
}

pub fn try_recv(&self, buf: &mut [u8]) -> Result<usize, ChannelError> {
    let head = self.read_head().load(Ordering::Relaxed);
    let tail = self.write_head().load(Ordering::Relaxed);

    if head == tail {
        return Err(ChannelError::Empty);
    }

    // CRITICAL: Acquire fence before reading payload
    std::sync::atomic::fence(Ordering::Acquire);

    // Now safe to read payload
    // ...
}
```

---

### Task 1.5: Pre-Create msgspec Decoders
**Priority:** P1-MEDIUM
**Effort:** Low
**Risk:** Low

**Description:**
Pre-create msgspec Decoder instances for known types at module load time.

**File:** `src/py/vertebra/core/serializers/codecs.py`

**Acceptance Criteria:**
- [ ] Decoders for AsgiRequest, dict, list pre-created
- [ ] Class-level cache, not instance-level
- [ ] Module load time increased by <10ms

**Implementation Notes:**
```python
# Module-level pre-created decoders
_DECODERS: dict[type, msgspec.msgpack.Decoder] = {
    dict: msgspec.msgpack.Decoder(dict, dec_hook=default_decoder),
    AsgiRequest: msgspec.msgpack.Decoder(AsgiRequest, dec_hook=default_decoder),
}

def decode(data: bytes, type_hint: type | None = None) -> Any:
    if type_hint in _DECODERS:
        return _DECODERS[type_hint].decode(data)
    # Fallback to dynamic creation
    ...
```

---

### Task 1.6: Reduce Spin Duration
**Priority:** P1-LOW
**Effort:** Low
**Risk:** Low

**Description:**
Reduce DEFAULT_SPIN_US from 5 to 2 microseconds. The 5µs spin is too long given target 10µs budget.

**File:** `src/py/vertebra/messaging/transport/async_transport.py`

**Acceptance Criteria:**
- [ ] DEFAULT_SPIN_US = 2
- [ ] Configurable via environment variable
- [ ] Benchmark verifies no latency regression

---

### Task 1.7: Benchmark P1 Changes
**Priority:** P1-HIGH
**Effort:** Low
**Risk:** None

**Description:**
Run benchmarks after P1 changes.

**Acceptance Criteria:**
- [ ] `hello` endpoint: 80,000+ RPS
- [ ] `json` endpoint: 60,000+ RPS
- [ ] p99 latency: <5ms

---

## Phase 2: Allocation & Polish (Target: 120-200K RPS)

### Task 2.1: Header Pre-Allocation in Worker
**Priority:** P2-MEDIUM
**Effort:** Medium
**Risk:** Low

**Description:**
Pre-allocate header buffers instead of creating new list[tuple] for each request.

**File:** `src/py/vertebra/core/worker.py` (line 913)

**Acceptance Criteria:**
- [ ] Header buffer pool (reusable lists)
- [ ] Or: lazy header access (only decode on access)
- [ ] GC pressure reduced measurably

---

### Task 2.2: Zero-Copy Header View (Stretch)
**Priority:** P2-LOW
**Effort:** High
**Risk:** Medium

**Description:**
Create PyO3-backed header view that accesses headers directly from SHM.

**File:** `src/rs/vertebra-py/src/lib.rs` (new type)

**Acceptance Criteria:**
- [ ] `HeaderView` class exposes headers without Python allocation
- [ ] ASGI apps can access headers via index or iteration
- [ ] Benchmark shows measurable improvement

---

### Task 2.3: Lazy Notification Batching
**Priority:** P2-MEDIUM
**Effort:** Medium
**Risk:** Low

**Description:**
Only notify when ring transitions empty→non-empty, not on every send.

**File:** `src/rs/vertebra-core/src/channel.rs`

**Acceptance Criteria:**
- [ ] Track `was_empty` before send
- [ ] Only wake if ring was empty AND now has data
- [ ] Reduces syscalls from O(N) to O(batches)

**Implementation Notes:**
```rust
fn send(&self, data: &[u8]) -> Result<(), ChannelError> {
    let was_empty = !self.has_data();

    self.try_send(data)?;

    // Only notify on empty->non-empty transition
    if was_empty && self.wait_count.load(Ordering::Acquire) > 0 {
        self.notifier().wake_one();
    }
    Ok(())
}
```

---

### Task 2.4: Response Writer Flush Pattern
**Priority:** P2-MEDIUM
**Effort:** Medium
**Risk:** Low

**Description:**
Allow worker to process multiple requests before triggering response notification.

**File:** `src/py/vertebra/core/worker.py` (ResponseWriter class)

**Acceptance Criteria:**
- [ ] `flush()` method triggers notification
- [ ] Auto-flush after batch processing
- [ ] Reduces response-side syscalls

---

### Task 2.5: CPU Affinity Option
**Priority:** P2-LOW
**Effort:** Low
**Risk:** Low

**Description:**
Add optional CPU pinning for worker processes.

**File:** `src/rs/vertebra-server/src/worker.rs`

**Acceptance Criteria:**
- [ ] `--cpu-affinity` CLI flag
- [ ] Workers pinned to specific cores
- [ ] Benchmark shows improvement on NUMA systems

---

### Task 2.6: Final Benchmark & Documentation
**Priority:** P2-HIGH
**Effort:** Medium
**Risk:** None

**Description:**
Run final benchmarks, document results, update performance tuning guide.

**Acceptance Criteria:**
- [ ] `hello` endpoint: 100,000+ RPS
- [ ] `json` endpoint: 80,000+ RPS
- [ ] Stretch: 200,000 RPS achieved
- [ ] `docs/architecture/performance-tuning.md` updated
- [ ] `.agent/patterns.md` updated with learnings

---

## Summary Table

| Task ID | Description | Priority | Effort | Risk |
|---------|-------------|----------|--------|------|
| 0.1 | Add wait_count to channel | P0-CRITICAL | Medium | High |
| 0.2 | Conditional wake for send | P0-CRITICAL | Medium | High |
| 0.3 | Double-check pattern for recv | P0-CRITICAL | High | High |
| 0.4 | Optimistic GIL for try_recv | P0-CRITICAL | Medium | Medium |
| 0.5 | Optimistic GIL for try_send | P0-HIGH | Medium | Low |
| 0.6 | Remove dict copies | P0-HIGH | Low | Low |
| 0.7 | Benchmark P0 | P0-HIGH | Low | None |
| 1.1 | recv_batch in Rust | P1-HIGH | Medium | Low |
| 1.2 | recv_batch Python binding | P1-HIGH | Medium | Low |
| 1.3 | Worker batch processing | P1-HIGH | High | Medium |
| 1.4 | Relax memory ordering | P1-MEDIUM | Medium | Medium |
| 1.5 | Pre-create decoders | P1-MEDIUM | Low | Low |
| 1.6 | Reduce spin duration | P1-LOW | Low | Low |
| 1.7 | Benchmark P1 | P1-HIGH | Low | None |
| 2.1 | Header pre-allocation | P2-MEDIUM | Medium | Low |
| 2.2 | Zero-copy header view | P2-LOW | High | Medium |
| 2.3 | Lazy notification batching | P2-MEDIUM | Medium | Low |
| 2.4 | Response flush pattern | P2-MEDIUM | Medium | Low |
| 2.5 | CPU affinity option | P2-LOW | Low | Low |
| 2.6 | Final benchmark & docs | P2-HIGH | Medium | None |

**Total Tasks:** 20
**P0 Tasks:** 7 (critical path)
**P1 Tasks:** 7 (major gains)
**P2 Tasks:** 6 (polish/stretch)
