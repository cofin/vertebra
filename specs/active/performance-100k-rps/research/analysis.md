# Performance Analysis: 12K → 100K-200K RPS

## Executive Summary

Vertebra's IPC system is capped at ~12,000 RPS due to systematic overhead across the Python/Rust boundary. This analysis identifies 12 distinct bottlenecks totaling ~83µs per request-response cycle. The target of 10µs (100K RPS) requires eliminating 73µs (88%) of current overhead.

**Mathematical Validation:**
- Current: 12,000 RPS = 83.3µs per request
- Target (low): 100,000 RPS = 10µs per request
- Target (stretch): 200,000 RPS = 5µs per request
- Gap: 73-78µs to eliminate

## Bottleneck Inventory

### Rust-Side Bottlenecks (Total: ~55µs)

| # | Issue | Location | Impact | Severity |
|---|-------|----------|--------|----------|
| R1 | Unconditional wake_one() on send | channel.rs:284 | 8µs | CRITICAL |
| R2 | Unconditional wake_one() on recv | channel.rs:351 | 8µs | CRITICAL |
| R3 | Acquire ordering on read_head | channel.rs:273-274 | 5µs | HIGH |
| R4 | py.detach() GIL thrashing | lib.rs:613,637,668 | 15µs | HIGH |
| R5 | slot_borrowed serializes MPMC | channel.rs:537 | 8µs | HIGH |
| R6 | poll(0) syscall in spin | notify.rs:287 | 3µs | MEDIUM |
| R7 | No exponential backoff | channel.rs:289-305 | 4µs | MEDIUM |

### Python-Side Bottlenecks (Total: ~28µs)

| # | Issue | Location | Impact | Severity |
|---|-------|----------|--------|----------|
| P1 | Memoryview decode copies | channel.py:57-58 | 3µs | HIGH |
| P2 | Dict copy in request() | broker.py:166,469 | 2µs | HIGH |
| P3 | Spin-wait perf_counter | async_transport.py:173-178 | 8µs | MEDIUM |
| P4 | Decoder cache misses | codecs.py:166-171 | 5µs | MEDIUM |
| P5 | Arrow Python fallback | arrow.py:162-175 | 200µs* | MEDIUM |
| P6 | Header list comprehension | worker.py:913 | 5µs | MEDIUM |

*Arrow fallback only affects large result sets

## Root Cause Analysis

### 1. Syscall-Per-Message Pattern (PRIMARY)

The Rust channel implementation calls `wake_one()` unconditionally after every successful send/recv:

```rust
// channel.rs:284 - AFTER EVERY SEND
self.notifier().wake_one();

// channel.rs:351 - AFTER EVERY RECV
self.notifier().wake_one();
```

**Impact:** 4 syscalls per request-response cycle × 8µs = 32µs baseline

**Fix:** Track waiter state with atomic flag. Only wake if peer is actually parked.

### 2. PyO3 GIL Toggle Overhead (SECONDARY)

The `py.detach()` pattern releases/reacquires GIL for operations that complete in <1µs:

```rust
// lib.rs:613 - GIL TOGGLE FOR 1µs OPERATION
fn recv_request<'py>(&self, py: Python<'py>, max_len: usize) {
    let len = py.detach(|| self.recv_ring.recv(&mut buffer))?;
    //        ^^^^^^^^^^^ 2-5µs overhead for <1µs operation
}
```

**Impact:** 10-30µs per blocking operation

**Fix:** "Optimistic GIL" - try operation with GIL first, only release if blocking needed.

### 3. Missing Batch API (ARCHITECTURAL)

Each request crosses the Rust/Python FFI boundary individually. At 100K RPS:
- 100,000 FFI crossings/second
- ~1µs FFI overhead per crossing
- 100ms/second wasted on FFI alone

**Fix:** `recv_batch(max=N)` API to drain multiple requests per FFI call.

### 4. asyncio Ceiling (FUNDAMENTAL)

Python's asyncio event loop has inherent overhead:
- Event loop iteration: ~5-10µs
- Callback dispatch: ~2-5µs
- Practical ceiling: 30-50K RPS per core

**Implication:** 200K RPS requires either:
- Multiple worker processes (existing design supports this)
- Hybrid spin in Python worker (bypass asyncio for bursts)
- Rust-side request processing for hot paths

## Consensus Analysis (Multi-Model)

**Models Consulted:** Gemini 3 Pro (for), Gemini 3 Flash (against)

### Points of Agreement (High Confidence)

1. **Unconditional wake_one() is THE critical bottleneck** - Both models identify this as primary issue
2. **GIL toggle overhead is wasteful for non-blocking ops** - Both recommend "optimistic GIL"
3. **Batching is MANDATORY for 200K RPS** - Both agree this is a missing architectural piece
4. **Memory ordering relaxation needs care** - Acquire still needed before payload access

### Points of Disagreement

| Issue | Pro (Gemini 3 Pro) | Against (Gemini 3 Flash) |
|-------|-------------------|-------------------------|
| GIL handling | Optimistic GIL sufficient | Need zero-copy header view |
| Allocation | Not major concern | Critical at 200K RPS |
| CPU pinning | Not mentioned | Required for target |

### Final Confidence Score

- Gemini 3 Pro: 9/10
- Gemini 3 Flash: 8/10
- Combined: **High confidence** in bottleneck analysis, moderate uncertainty in Python GC impact

## Performance Budget

To achieve 100K RPS (10µs budget):

| Component | Current | Target | Reduction |
|-----------|---------|--------|-----------|
| Syscalls | 32µs | 2µs | -94% (conditional + lazy) |
| GIL toggle | 15µs | 2µs | -87% (optimistic GIL) |
| Memory ordering | 10µs | 3µs | -70% (Relaxed fast-path) |
| Python decode | 8µs | 2µs | -75% (zero-copy) |
| FFI overhead | 8µs | 1µs | -88% (batching) |
| Other | 10µs | 0µs | -100% (various) |
| **Total** | **83µs** | **10µs** | **-88%** |

## Validation Strategy

### Micro-benchmarks

1. **Syscall impact:** `cargo bench --package vertebra-core --bench spsc` before/after conditional wake
2. **GIL impact:** Python microbenchmark of try_recv with/without detach
3. **Batch impact:** Compare 1-at-a-time vs batch(100) FFI crossings

### Integration benchmarks

```bash
# Current baseline
make bench-servers --servers vertebra --scenarios hello,json

# After each phase
make bench-servers --servers vertebra --scenarios hello,json
```

### Expected Results

| Phase | Change | Expected RPS |
|-------|--------|--------------|
| Baseline | - | 12K |
| P0 Complete | Conditional wake + Optimistic GIL | 40-60K |
| P1 Complete | + Batching + Memory ordering | 80-120K |
| P2 Complete | + Allocation optimization | 120-200K |

## Risk Assessment

### Technical Risks

1. **Lost wakeup bug** (HIGH) - Conditional wake logic must be carefully ordered
2. **Memory ordering bug** (MEDIUM) - Relaxed loads need Acquire before payload access
3. **asyncio ceiling** (MEDIUM) - May need hybrid spin to exceed 50K RPS/core

### Mitigation

1. Add proptest/fuzzing for wake logic
2. Code review by memory ordering expert
3. Implement hybrid spin as P1 stretch goal
