# PRD: Performance Optimization - 100K-200K RPS Target

**Version:** 1.0
**Status:** Ready for Implementation
**Created:** 2026-01-28
**Epic:** `performance-100k-rps`
**Parent:** async-ipc-pipelining, vertebra-v2-rewrite

---

## 1. Intelligence Context

### 1.1 Complexity Assessment

**Complexity Level:** Complex (10+ checkpoints)

**Rationale:**
- Touches 4 critical subsystems (channel, bindings, worker, serialization)
- Requires memory ordering expertise
- High risk of subtle concurrency bugs
- Performance validation requires extensive benchmarking

### 1.2 Similar Features Referenced

| Reference | Location | Pattern |
|-----------|----------|---------|
| Granian GIL management | External | Optimistic try-with-GIL |
| io_uring lazy wake | Linux kernel | Conditional notification |
| LMAX Disruptor | External | Batch processing |
| async-ipc-pipelining | .agent/specs/ | Correlation protocol |

### 1.3 Patterns to Follow

- **Conditional Wake:** Only syscall if peer is actually parked
- **Optimistic GIL:** Try operation with GIL, release only if blocking
- **Batch Amortization:** Process N items per FFI/syscall crossing
- **Relaxed Fast-Path:** Use Relaxed ordering for checks, Acquire before data access

### 1.4 Detected Stack and Commands

| Component | Technology | Test Command |
|-----------|------------|--------------|
| IPC Core | Rust | `cargo test --package vertebra-core` |
| Bindings | PyO3 | `cargo test --package vertebra-py` |
| Worker | Python/asyncio | `uv run pytest src/py/tests` |
| Benchmark | oha | `make bench-servers` |

---

## 2. Problem Statement

### 2.1 User Problem

Vertebra currently achieves ~12,000 RPS in benchmarks, which is below competitors like Granian (60-80K RPS). Users deploying high-traffic applications cannot rely on Vertebra for production workloads requiring >20K RPS.

### 2.2 Business Value

- **Market Position:** Enables Vertebra to compete with Granian/Uvicorn
- **User Adoption:** Removes primary blocker for production deployments
- **Architecture Validation:** Proves shared-memory IPC approach is viable

### 2.3 Success Criteria

| Metric | Baseline | Target (P0) | Stretch (P1) |
|--------|----------|-------------|--------------|
| RPS (hello endpoint) | 12,000 | 100,000 | 200,000 |
| RPS (json endpoint) | 10,000 | 80,000 | 160,000 |
| p99 latency | 2ms | <5ms | <2ms |
| CPU efficiency | 8% per 1K RPS | 1% per 1K RPS | 0.5% per 1K RPS |

---

## 3. Acceptance Criteria

### 3.1 Functional Requirements

1. **F1:** Ring buffer send/recv operations only trigger syscalls when peer is actually waiting
2. **F2:** PyO3 bindings attempt non-blocking operations with GIL held before releasing
3. **F3:** Batch API allows draining multiple messages per FFI crossing
4. **F4:** Memory ordering uses Relaxed for fast-path checks, Acquire before payload access
5. **F5:** Existing tests continue to pass (no API changes)

### 3.2 Performance Requirements

1. **P1:** `hello` endpoint achieves 100,000 RPS on benchmark hardware (AMD Ryzen 7, single worker)
2. **P2:** p99 latency stays under 5ms at 80% capacity
3. **P3:** No memory leaks under sustained load (24-hour soak test)
4. **P4:** CPU usage scales linearly with RPS (no runaway spinning)

### 3.3 Edge Cases

1. **E1:** Lost wakeup prevention - receiver must not miss messages when transitioning to wait
2. **E2:** Thundering herd prevention - wake_one, not wake_all
3. **E3:** Graceful degradation - system remains stable if backpressure occurs
4. **E4:** Cross-platform - works on Linux (eventfd), macOS (pipe), Windows (pipe)

### 3.4 Error Scenarios

1. **ERR1:** Ring buffer full - return backpressure error, do not block indefinitely
2. **ERR2:** Worker crash - pending requests receive timeout, not hang
3. **ERR3:** Memory corruption - detect via CRC or magic bytes (optional)

---

## 4. Technical Approach

### 4.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    PERFORMANCE OPTIMIZATION LAYERS               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Layer 1: Syscall Reduction (P0)                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ channel.rs: Conditional wake_one()                       │   │
│  │ - Add wait_count: AtomicU32                             │   │
│  │ - Only wake if wait_count > 0                           │   │
│  │ - Careful ordering to prevent lost wakeups              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Layer 2: GIL Optimization (P0)                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ lib.rs: Optimistic GIL pattern                          │   │
│  │ - try_* with GIL held (fast path)                       │   │
│  │ - allow_threads() only for blocking operations          │   │
│  │ - Eliminates 2-5µs toggle overhead                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Layer 3: Batch Processing (P1)                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ channel.rs + lib.rs: recv_batch(max=N)                  │   │
│  │ - Drain up to N messages per FFI call                   │   │
│  │ - Amortize FFI overhead from O(N) to O(1)               │   │
│  │ - Worker processes batch in asyncio.gather()            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Layer 4: Memory Ordering (P1)                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ channel.rs: Relaxed fast-path                           │   │
│  │ - Relaxed load for has_data() checks                    │   │
│  │ - Acquire barrier before payload access                 │   │
│  │ - Eliminates unnecessary fences                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Layer 5: Allocation Reduction (P2)                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ worker.py: Header optimization                          │   │
│  │ - Pre-allocated header buffer                           │   │
│  │ - Memoryview instead of string decode                   │   │
│  │ - Reduces GC pressure at high RPS                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 File Structure Plan

**Modified Files:**

| File | Changes |
|------|---------|
| `src/rs/vertebra-core/src/channel.rs` | Add wait_count, conditional wake, Relaxed fast-path |
| `src/rs/vertebra-core/src/sync.rs` | Expose wait_count primitives |
| `src/rs/vertebra-py/src/lib.rs` | Optimistic GIL, batch API |
| `src/py/vertebra/core/worker.py` | Batch processing support |
| `src/py/vertebra/core/serializers/codecs.py` | Pre-create decoders |

**New Files:**

| File | Purpose |
|------|---------|
| `src/rs/vertebra-core/benches/throughput.rs` | Throughput benchmark |
| `benchmarks/ipc_batch.py` | Batch API benchmark |

### 4.3 Database Changes

None required.

### 4.4 API Endpoints

No new HTTP endpoints. Internal API additions:

```rust
// New Rust API
impl SpscRing {
    fn recv_batch(&self, max: usize) -> Result<Vec<(u64, Vec<u8>)>, ChannelError>;
}

// New Python API
class AsyncRing:
    def recv_batch(self, max: int = 100) -> list[tuple[int, bytes]]: ...
```

### 4.5 Documentation Updates

| Document | Update |
|----------|--------|
| `docs/architecture/performance-tuning.md` | Add batch API section |
| `CLAUDE.md` | Update performance targets |
| `.agent/patterns.md` | Add conditional wake pattern |

---

## 5. Testing Strategy

### 5.1 Unit Tests (90%+ Coverage)

**Rust Tests:**
```rust
// src/rs/vertebra-core/src/channel.rs
#[test]
fn test_conditional_wake_no_waiters() {
    // Send should not trigger syscall when no waiters
}

#[test]
fn test_conditional_wake_with_waiter() {
    // Send should wake when receiver is parked
}

#[test]
fn test_no_lost_wakeup() {
    // Stress test: receiver never misses message
}

#[test]
fn test_batch_recv_drains_multiple() {
    // recv_batch returns up to N messages
}
```

**Python Tests:**
```python
# src/py/tests/unit/ipc/test_batch.py
def test_batch_recv_empty():
    """recv_batch returns empty list on empty ring"""

def test_batch_recv_partial():
    """recv_batch returns available messages up to max"""

def test_batch_recv_concurrent():
    """recv_batch works under concurrent send"""
```

### 5.2 Integration Tests

```python
# src/py/tests/integration/ipc/test_throughput.py
@pytest.mark.integration
async def test_100k_rps_target():
    """Verify 100K RPS is achievable with optimizations"""

@pytest.mark.integration
async def test_no_lost_messages_under_load():
    """All messages delivered under sustained 100K RPS"""
```

### 5.3 Benchmark Validation

```bash
# Baseline (before changes)
make bench-servers --servers vertebra --scenarios hello,json > baseline.json

# After P0
make bench-servers --servers vertebra --scenarios hello,json > p0.json

# Compare
python scripts/compare_benchmarks.py baseline.json p0.json
# Expected: 3-5x improvement
```

### 5.4 Stress Tests

```bash
# 24-hour soak test
oha -z 24h -c 256 http://localhost:8000/hello

# Memory leak check
valgrind --leak-check=full cargo test --package vertebra-core

# Lost message detection
uv run python benchmarks/stress_lost_message.py --duration 3600
```

---

## 6. Implementation Notes

### 6.1 Conditional Wake Implementation

**Critical ordering to prevent lost wakeups:**

```rust
// CORRECT: Double-check after incrementing wait_count
fn recv_blocking(&self, buf: &mut [u8]) -> Result<usize> {
    loop {
        // Fast path: try without waiting
        if let Ok(len) = self.try_recv(buf) {
            return Ok(len);
        }

        // Increment wait count with Release ordering
        // This synchronizes with sender's Acquire load
        self.wait_count.fetch_add(1, Ordering::Release);

        // CRITICAL: Re-check after incrementing
        // Prevents: sender checks wait_count (0) -> we increment -> sender sends (no wake)
        if let Ok(len) = self.try_recv(buf) {
            self.wait_count.fetch_sub(1, Ordering::Relaxed);
            return Ok(len);
        }

        // Safe to park now - sender will see wait_count > 0
        self.notifier.wait();
        self.wait_count.fetch_sub(1, Ordering::Relaxed);
    }
}
```

### 6.2 Optimistic GIL Pattern

```rust
// CORRECT: Try with GIL, only release if blocking needed
fn try_recv_optimistic<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
    // Fast path: try with GIL held
    if let Ok(len) = self.ring.try_recv(&mut self.buffer) {
        return Ok(PyBytes::new_bound(py, &self.buffer[..len]));
    }

    // Slow path: release GIL for blocking wait
    let len = py.allow_threads(|| self.ring.recv_blocking(&mut self.buffer))?;
    Ok(PyBytes::new_bound(py, &self.buffer[..len]))
}
```

### 6.3 Batch Processing Pattern

```rust
fn recv_batch(&self, max: usize) -> Result<Vec<(u64, Vec<u8>)>, ChannelError> {
    let mut batch = Vec::with_capacity(max.min(64)); // Cap allocation

    // Drain available messages without blocking
    while batch.len() < max {
        match self.try_recv_with_header() {
            Ok((header, data)) => batch.push((header.id, data)),
            Err(ChannelError::Empty) => break, // No more available
            Err(e) => return Err(e),
        }
    }

    Ok(batch)
}
```

### 6.4 Memory Ordering Rationale

| Operation | Current | New | Rationale |
|-----------|---------|-----|-----------|
| has_data() check | Acquire | Relaxed | Only checking, not accessing data |
| try_recv head load | Acquire | Relaxed | Fast-path check |
| try_recv before data access | N/A | Acquire | Ensure payload visibility |
| wait_count increment | N/A | Release | Synchronize with sender |
| wait_count check (sender) | N/A | Acquire | See receiver's increment |

### 6.5 Dependencies

This PRD depends on:
- `async-ipc-pipelining` Chapter 2 (correlation-protocol) for request ID handling
- `vertebra-v2-rewrite` Chapter 1 (server-foundation) for Rust server

This PRD is independent of:
- `lock-free-dispatch` (can be applied in either order)
- `python-concurrent` (additive improvement)

### 6.6 Migration Considerations

**Backward Compatibility:**
- All changes are internal implementation details
- Public API unchanged
- Existing benchmarks continue to work

**Feature Flags:**
```rust
// Optional: compile-time flag for gradual rollout
#[cfg(feature = "conditional-wake")]
fn wake_if_needed(&self) {
    if self.wait_count.load(Ordering::Acquire) > 0 {
        self.notifier.wake_one();
    }
}

#[cfg(not(feature = "conditional-wake"))]
fn wake_if_needed(&self) {
    self.notifier.wake_one();
}
```

---

## 7. Risk Mitigation

### 7.1 Lost Wakeup Bug

**Risk:** Receiver parks but never wakes because sender checked wait_count before increment.

**Mitigation:**
1. Double-check pattern after incrementing wait_count
2. Proptest with millions of send/recv combinations
3. MIRI for data race detection
4. Code review by concurrency expert

### 7.2 Memory Ordering Bug

**Risk:** Payload not visible after successful recv due to missing barrier.

**Mitigation:**
1. Add explicit Acquire fence before payload access
2. Document ordering requirements in code comments
3. Run under ThreadSanitizer

### 7.3 Performance Regression

**Risk:** Changes introduce unexpected slowdown in some scenarios.

**Mitigation:**
1. Benchmark before/after each PR
2. A/B test with feature flag
3. Maintain baseline benchmarks in CI

---

## 8. Detailed Bottleneck Analysis

### 8.1 Syscall Overhead Breakdown

The current implementation triggers syscalls on every IPC operation. A complete request-response cycle involves:

1. **Rust server sends request to Python worker:**
   - `SpscSender::send()` writes to ring buffer
   - `wake_one()` triggers futex syscall (~5-8µs)

2. **Python worker receives request:**
   - `AsyncRing.try_recv()` reads from ring buffer
   - eventfd read to clear notification (~1-2µs)

3. **Python worker sends response:**
   - `AsyncRing.send()` writes to ring buffer
   - eventfd write to notify Rust (~3-5µs)

4. **Rust server receives response:**
   - Response router calls `wait_readable().await`
   - eventfd poll + read (~2-3µs)
   - `SpscReceiver::try_recv()` reads from ring buffer

**Total syscall overhead per request:** 15-25µs (18-30% of 83µs budget)

With conditional wake, syscalls only occur when the peer is actually parked:
- High load (peer spinning): ~0µs syscall overhead
- Low load (peer parked): ~8µs syscall overhead
- **Expected average:** 2-5µs (80-90% reduction)

### 8.2 GIL Toggle Overhead Analysis

Python's Global Interpreter Lock (GIL) prevents true parallelism but allows C extensions to release it during blocking operations. The current PyO3 bindings release GIL for ALL IPC operations:

```rust
// Current pattern (lib.rs:613)
fn recv<'py>(&self, py: Python<'py>) -> PyResult<...> {
    let result = py.detach(|| {  // Release GIL
        self.ring.recv(&mut buffer)  // May complete instantly
    })?;  // Reacquire GIL
    Ok(...)
}
```

**GIL toggle cost breakdown:**
- Release: Save thread state, unlock (~1µs)
- Reacquire: Wait for GIL, restore state (~1-3µs)
- Scheduler overhead: Python may run other threads (~2-5µs worst case)
- **Total:** 2-5µs per operation

At 100K RPS with 2 IPC operations per request:
- 200,000 GIL toggles per second
- 400ms-1s of wasted time per second

The optimistic GIL pattern eliminates this for non-blocking operations (estimated 90%+ of operations under load):
- Fast path (data available): 0µs GIL overhead
- Slow path (blocking needed): 2-5µs GIL overhead
- **Expected average:** 0.2-0.5µs (90% reduction)

### 8.3 Python Allocation Analysis

At 200K RPS, Python object creation becomes a significant bottleneck due to:

1. **Header list comprehension (worker.py:913):**
   ```python
   headers = [(k.decode(), v.decode()) for k, v in raw_headers]
   ```
   - Creates 2N string objects per request (N = number of headers)
   - Typical request: 10 headers = 20 string allocations
   - 200K RPS × 20 strings = 4M allocations/second

2. **Scope dict creation:**
   ```python
   scope = {
       "type": "http",
       "method": meta.method,  # String copy
       "path": meta.path,      # String copy
       ...
   }
   ```
   - ~20 dict entries per request
   - 200K RPS × 20 entries = 4M dict operations/second

3. **MessagePack decode:**
   - Creates intermediate Python objects
   - GC pressure from short-lived objects

**Mitigation strategies:**
- Pre-allocated header buffers (reuse across requests)
- Lazy header access (only decode on first access)
- Zero-copy header view via PyO3 (access SHM directly)

### 8.4 Memory Ordering Deep Dive

The Rust channel uses atomic operations for thread-safe coordination. Current ordering:

```rust
// Current: Acquire on both loads (channel.rs:273-274)
let head = self.read_head().load(Ordering::Acquire);
let tail = self.write_head().load(Ordering::Acquire);
```

**Why Acquire is expensive:**
- Prevents CPU reordering of subsequent loads
- Forces memory fence on x86 (implicit) and ARM (explicit `dmb` instruction)
- ~5-15ns per operation on multi-core systems

**Why Relaxed is sufficient for checks:**
- We're only comparing head vs tail, not accessing payload
- Stale values are acceptable (just retry)
- Acquire is only needed before reading the actual data

**Correct pattern:**
```rust
// Fast path: Relaxed for comparison
let head = self.read_head().load(Ordering::Relaxed);
let tail = self.write_head().load(Ordering::Relaxed);
if head == tail { return Err(Empty); }

// Before data access: Acquire fence
std::sync::atomic::fence(Ordering::Acquire);

// Now safe to read payload
let data = &self.slots[slot_index];
```

This reduces per-operation overhead from ~15ns to ~3ns (80% reduction).

### 8.5 Batch Processing Impact

FFI (Foreign Function Interface) crossing has fixed overhead:

| Operation | Overhead |
|-----------|----------|
| PyO3 function call dispatch | ~50ns |
| GIL state management | ~100ns |
| Python exception handling setup | ~50ns |
| Return value conversion | ~100ns |
| **Total per FFI call** | ~300ns |

At 100K RPS with 1 FFI call per message:
- 100,000 FFI calls × 300ns = 30ms/second

With batching (100 messages per FFI call):
- 1,000 FFI calls × 500ns = 0.5ms/second (60x improvement)

The batch API amortizes this overhead:
```python
# Before: O(N) FFI calls
for _ in range(N):
    msg = ring.recv()  # FFI call
    process(msg)

# After: O(1) FFI calls
batch = ring.recv_batch(N)  # Single FFI call
for msg in batch:
    process(msg)
```

---

## 9. Appendix

### 8.1 Benchmark Hardware

**Reference Configuration:**
- CPU: AMD Ryzen 7 5700X (8 cores, 16 threads)
- RAM: 32GB DDR4-3200
- OS: Linux 6.12 (Ubuntu 24.04)
- Kernel: `CONFIG_HZ=1000`

### 8.2 Related PRDs

| PRD | Relationship |
|-----|--------------|
| async-ipc-pipelining | Prerequisite (correlation protocol) |
| lock-free-dispatch | Complementary (can apply independently) |
| vertebra-v2-rewrite | Parent (server foundation) |

### 8.3 References

- [Rust Atomics and Locks](https://marabos.nl/atomics/) - Memory ordering
- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) - Batch processing
- [Linux futex(2)](https://man7.org/linux/man-pages/man2/futex.2.html) - Conditional wake
- [PyO3 GIL Management](https://pyo3.rs/v0.20.0/parallelism.html) - Optimistic pattern

---

**Total PRD Word Count:** 3,400+ words
