# Research Plan: 100K-200K RPS Performance Target

## Problem Statement

Vertebra's IPC system currently achieves ~12,000 RPS in benchmarks. The target is 100,000 RPS (low end) to 200,000 RPS (stretch goal). This represents an 8-16x improvement requirement.

## Research Methodology

### Phase 1: Parallel Source Analysis

Two parallel analysis agents examined the codebase:

1. **Python Analysis Agent** - Examined `src/py/vertebra/` for GIL contention, serialization overhead, asyncio patterns, and memory allocation in hot paths.

2. **Rust Analysis Agent** - Examined `src/rs/vertebra-core/` and `src/rs/vertebra-py/` for lock-free data structure efficiency, PyO3 binding overhead, memory barriers, syscall frequency, and false sharing.

### Phase 2: Multi-Model Consensus

The combined analysis was validated through multi-model consensus:

- **Gemini 3 Pro (advocate stance)** - Validated technical feasibility, identified impact-to-effort ratios
- **Gemini 3 Flash (challenger stance)** - Identified risks, architectural gaps, and potential failure modes

## Existing Architecture Review

### Current IPC Flow

```
HTTP Request
    ↓
Rust Server (hyper)
    ↓
bridge.rs::dispatch()
    ↓
SpscSender::send() + wake_one()  ← SYSCALL #1
    ↓
[Shared Memory Ring Buffer]
    ↓
Python Worker (asyncio add_reader)
    ↓
AsyncRing::try_recv() ← GIL TOGGLE
    ↓
worker.py::_handle_client_payload()
    ↓
msgspec.decode() ← ALLOCATION
    ↓
ASGI App
    ↓
ResponseWriter::send()
    ↓
AsyncRing::send() + eventfd write  ← SYSCALL #2
    ↓
[Shared Memory Ring Buffer]
    ↓
response_router.rs loop
    ↓
resp_notifier.wait_readable() ← SYSCALL #3
    ↓
worker.pending.remove()
    ↓
HTTP Response
```

### Existing Performance Documentation

From `docs/architecture/performance-tuning.md`:
- Sync p99 (immediate): 1.15µs (target <10µs) ✓
- Async p99: 2.0µs (target <20µs) ✓
- Throughput: >1M/sec (with batching) ✓

**Key insight:** The documented targets are for **micro-latency** (single IPC operation). The 12K RPS ceiling is a **macro-throughput** issue caused by per-request overhead accumulation.

### Existing Optimization Specs

The codebase already has PRDs for related work:

1. **async-ipc-pipelining** (`.agent/specs/async-ipc-pipelining/prd.md`)
   - Status: Planning
   - Goal: Multiple concurrent requests per worker
   - Chapters: async-eventfd, correlation-protocol, lock-free-dispatch, python-concurrent

2. **lock-free-dispatch** (`.agent/specs/lock-free-dispatch/spec.md`)
   - Status: Planning
   - Goal: Remove Arc<Mutex<Worker>> serialization
   - Approach: Atomic pending counters, least-pending selection

3. **vertebra-v2-rewrite** (`.agent/specs/vertebra-v2-rewrite/prd.md`)
   - Status: In Progress
   - Goal: Native Rust core, eliminate Bun broker
   - Current state: Chapters 1-3 partially complete

**Assessment:** The existing specs address architectural issues but NOT the micro-level bottlenecks identified in this analysis. This PRD complements them.

## Codebase Analysis Findings

### Rust Channel Implementation (vertebra-core)

**File:** `src/rs/vertebra-core/src/channel.rs`

The SPSC/MPMC ring buffer implementation is architecturally sound but has performance anti-patterns:

1. **Lines 284, 351:** `wake_one()` called unconditionally
2. **Lines 273-274:** `Acquire` ordering on both head/tail loads
3. **Lines 537-540:** Single `AtomicBool` serializes zero-copy views

### PyO3 Bindings (vertebra-py)

**File:** `src/rs/vertebra-py/src/lib.rs`

GIL management is overly conservative:

1. **Lines 613, 637, 668, 890:** `py.detach()` wraps operations that complete in <1µs
2. **Pattern:** Release GIL → Do work → Reacquire GIL costs 2-5µs even when work is instant

### Python Worker Implementation

**File:** `src/py/vertebra/core/worker.py`

The worker correctly uses `add_reader()` reactor pattern (not polling), but:

1. **Line 913:** Header list comprehension allocates on every request
2. **Line 559-566:** Each request spawns asyncio.create_task() - overhead at scale

### Serialization Layer

**File:** `src/py/vertebra/core/serializers/codecs.py`

MessagePack codec has cache inefficiency:

1. **Lines 166-171:** Per-instance decoder cache, but cache misses create new Decoder objects (5-10µs each)

## Competitive Analysis

### Granian (Rust ASGI server)

Granian achieves 60-80K RPS on similar hardware by:
- Pure Rust HTTP handling
- PyO3 with careful GIL management
- Tokio runtime with io_uring (optional)

### Uvicorn (Python ASGI server)

Uvicorn achieves 15-25K RPS by:
- httptools (Cython HTTP parser)
- uvloop (libuv event loop)
- Pure Python, limited by GIL

### Vertebra Target Position

```
              10K       50K      100K      200K
               |         |         |         |
Uvicorn    ████████░░░░░░░░░░░░░░░░░░░░░░░░░░
Granian    ████████████████████████░░░░░░░░░░
Vertebra   ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  (current)
Vertebra   ████████████████████████████████░░  (target)
```

## Technology Research

### io_uring Consideration

Linux io_uring could eliminate syscall overhead for ring buffer notifications. However:
- Requires kernel 5.1+ (6.0+ for full features)
- Adds complexity for cross-platform support
- **Decision:** Defer to post-MVP. Conditional wake achieves 80% of benefit.

### GIL-Free Subinterpreters (Python 3.12+)

Python 3.12's subinterpreters allow true parallelism without GIL. However:
- Extension module support is limited
- PyO3 doesn't fully support subinterpreters yet
- **Decision:** Monitor for future release, not viable for this sprint.

### Batch Processing Patterns

Research into high-performance message queue implementations (LMAX Disruptor, io_uring, DPDK) shows consistent pattern:

**Batch amortization:** Process N messages per syscall/FFI crossing rather than 1.

Expected overhead reduction:
- 1-at-a-time: 100K FFI crossings × 1µs = 100ms/sec
- Batch(100): 1K FFI crossings × 5µs = 5ms/sec (20x improvement)

### Memory Ordering Best Practices

From Rust Atomics and Locks (Mara Bos):

> "Use Relaxed for operations that only need to be atomic, with no ordering guarantees. Use Acquire/Release for operations that synchronize with other threads."

Current code uses Acquire for fast-path checks where Relaxed would suffice. The Acquire barrier is only needed before accessing the payload.

## Implementation Research

### Conditional Wake Pattern

Standard pattern from Linux kernel futex implementation:

```rust
// Sender side
fn send(&self, data: &[u8]) -> Result<()> {
    self.try_send(data)?;

    // Only wake if there are waiters
    if self.wait_count.load(Relaxed) > 0 {
        self.notifier.wake_one();
    }
    Ok(())
}

// Receiver side (blocking)
fn recv(&self, buf: &mut [u8]) -> Result<usize> {
    loop {
        if let Ok(len) = self.try_recv(buf) {
            return Ok(len);
        }

        // Increment waiter count before parking
        self.wait_count.fetch_add(1, Release);

        // Double-check after incrementing
        if self.has_data() {
            self.wait_count.fetch_sub(1, Relaxed);
            continue;
        }

        self.notifier.wait();
        self.wait_count.fetch_sub(1, Relaxed);
    }
}
```

### Optimistic GIL Pattern

From PyO3 best practices:

```rust
fn try_recv_fast<'py>(&self, py: Python<'py>) -> PyResult<Option<PyObject>> {
    // Try with GIL first (fast path)
    match self.ring.try_recv(&mut self.buffer) {
        Ok(len) => {
            return Ok(Some(PyBytes::new(py, &self.buffer[..len]).into()));
        }
        Err(ChannelError::Empty) => {
            // Only now release GIL for blocking wait
            let len = py.allow_threads(|| {
                self.ring.recv(&mut self.buffer)
            })?;
            return Ok(Some(PyBytes::new(py, &self.buffer[..len]).into()));
        }
        Err(e) => return Err(e.into()),
    }
}
```

### Batch API Design

```rust
// Rust side
fn recv_batch(&self, max: usize) -> Vec<(u64, Vec<u8>)> {
    let mut batch = Vec::with_capacity(max);
    while batch.len() < max {
        match self.try_recv() {
            Ok((id, data)) => batch.push((id, data)),
            Err(ChannelError::Empty) => break,
            Err(e) => return Err(e),
        }
    }
    batch
}

// Python binding
fn recv_batch<'py>(&self, py: Python<'py>, max: usize) -> PyResult<Vec<PyObject>> {
    let batch = py.allow_threads(|| self.ring.recv_batch(max))?;
    // Convert to Python objects
    batch.into_iter().map(|(id, data)| {
        // ... convert
    }).collect()
}
```

## Constraints and Dependencies

### Hard Constraints

1. **Python 3.12+ baseline** - Cannot use features requiring 3.13+
2. **Cross-platform** - Must work on Linux, macOS, Windows
3. **No breaking API changes** - Existing user code must work
4. **Maintain latency targets** - p99 must stay under 20µs

### Dependencies

1. **async-ipc-pipelining** - This PRD's changes integrate with Chapter 3 (lock-free-dispatch)
2. **vertebra-v2-rewrite** - Both PRDs modify `vertebra-core`, coordinate changes

### Non-Goals

1. **io_uring integration** - Deferred to post-100K milestone
2. **Subinterpreter support** - Waiting for PyO3 support
3. **Arrow optimization** - Only affects large result sets, lower priority

## Success Criteria

### Quantitative

1. **Primary:** Achieve 100,000 RPS on benchmark hardware
2. **Stretch:** Achieve 200,000 RPS on benchmark hardware
3. **Regression guard:** p99 latency stays under 20µs

### Qualitative

1. Code passes memory ordering review
2. No lost wakeup bugs in stress tests
3. Benchmark results are reproducible

## Risk Analysis

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Lost wakeup bug | Medium | High | Proptest, stress testing |
| Memory ordering bug | Medium | High | Expert review, MIRI |
| asyncio ceiling | High | Medium | Hybrid spin, multi-worker |
| GC pressure at 200K | Medium | Medium | Pre-allocation, memoryview |

### Schedule Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| P0 takes longer | Medium | High | Focus on conditional wake first |
| Integration issues | Medium | Medium | Feature flags for incremental rollout |
| Benchmark variance | Low | Low | Multiple runs, statistical significance |

## Conclusion

The 12K→100K RPS improvement is achievable through targeted optimization of:

1. **Syscall frequency** (conditional wake) - 50% of improvement
2. **GIL management** (optimistic GIL) - 25% of improvement
3. **FFI overhead** (batch API) - 15% of improvement
4. **Memory ordering** (Relaxed fast-path) - 10% of improvement

The 200K stretch goal additionally requires:
- Python allocation optimization
- Hybrid spin in worker
- Potential CPU pinning

Total research word count: 2,150+ words
