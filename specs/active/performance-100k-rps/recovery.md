# Recovery Guide: 100K-200K RPS Performance Target

## Quick Resume

**Last phase:** P0 Implementation Complete
**Current status:** Ready for P1 or Benchmarking
**PRD location:** `specs/active/performance-100k-rps/`

## Intelligence Context

**Complexity:** Complex (10+ checkpoints)

**Stack:**
- Rust: vertebra-core (channel.rs, sync.rs, notify.rs)
- Rust: vertebra-py (PyO3 bindings)
- Python: vertebra.core.worker, vertebra.messaging.broker

**Commands:**
- Test Rust: `cargo test --package vertebra-core`
- Test Python: `uv run pytest src/py/tests`
- Benchmark: `make bench-servers --servers vertebra --scenarios hello,json`
- Build Rust: `cd src/rs && cargo build --release`
- Rebuild PyO3: `cd src/rs/vertebra-py && maturin develop`

**Documentation:**
- System: Sphinx
- Location: `docs/`
- Skills: `~/.claude/skills/vertebra/`

**Similar implementations:**
- Granian (external) - GIL optimization
- io_uring (Linux kernel) - conditional wake
- LMAX Disruptor - batch processing

**Patterns being followed:**
- Conditional wake (only syscall if waiters exist)
- Optimistic GIL (try with GIL, release only if blocking)
- Batch amortization (N items per FFI crossing)
- Relaxed fast-path (Acquire only before payload access)

## State Summary

### Completed
- [x] Python source analysis (bottlenecks identified)
- [x] Rust source analysis (bottlenecks identified)
- [x] Multi-model consensus (Gemini 3 Pro + Flash)
- [x] PRD written (3,400+ words)
- [x] Research documented (2,150+ words)
- [x] Tasks broken down (20 tasks, 3 phases)
- [x] **P0 implementation (Tasks 0.1-0.6)**
  - Added `send_waiters` and `recv_waiters` to `ShmHeader`
  - Implemented conditional wake in `try_send` and `try_recv`
  - Implemented double-check pattern in all blocking recv/send methods
  - Implemented optimistic GIL pattern in PyO3 bindings
  - Removed dict copies in Python broker

### Pending
- [x] P0 benchmarking (Task 0.7) - See findings below
- [ ] P1 implementation (Tasks 1.1-1.7)
- [ ] P2 implementation (Tasks 2.1-2.6)
- [ ] Final validation

### P0 Benchmark Findings

**Micro-benchmarks (vertebra-core):**
- `spsc_ping_pong`: **17ns** (excellent)
- `spsc_recv_hybrid_immediate`: **18ns** (fast path working)
- `spsc_throughput`: **3.3µs** per batch

**Server benchmarks:**
- Vertebra: **11,237 RPS** (unchanged from baseline)
- Granian reference: **62,387 RPS**

**Root Cause of Limited Server Improvement:**
The `vertebra-server` crate has redundant notification in `WorkerConnection::send_request()`:
```rust
// src/rs/vertebra-server/src/worker.rs:89-91
self.req_notifier.notify()  // <-- Unconditional eventfd write!
```

This bypasses the conditional wake optimization in `vertebra-core`. The server uses a separate `AsyncNotifier` (eventfd) to signal Python workers, which fires **unconditionally** after every send.

**Action Required:**
Add conditional notification to vertebra-server:
```rust
// Only notify if Python worker is waiting
if self.python_waiting.load(Ordering::Acquire) > 0 {
    self.req_notifier.notify()?;
}
```

This should be added as a new P0 task or incorporated into P1.

### Files Modified (P0)
- `src/rs/vertebra-core/src/buffer.rs` - Added waiter count fields
- `src/rs/vertebra-core/src/channel.rs` - Conditional wake + double-check pattern
- `src/rs/vertebra-py/src/lib.rs` - Optimistic GIL pattern
- `src/py/vertebra/messaging/broker.py` - Dict copy optimization

## Key Files to Modify

| Priority | File | Change Summary |
|----------|------|----------------|
| P0 | `src/rs/vertebra-core/src/channel.rs` | wait_count, conditional wake |
| P0 | `src/rs/vertebra-py/src/lib.rs` | Optimistic GIL pattern |
| P0 | `src/py/vertebra/messaging/broker.py` | Remove dict copies |
| P1 | `src/rs/vertebra-core/src/channel.rs` | recv_batch, Relaxed ordering |
| P1 | `src/py/vertebra/core/worker.py` | Batch processing |
| P1 | `src/py/vertebra/core/serializers/codecs.py` | Pre-create decoders |

## Critical Implementation Notes

### Conditional Wake (MUST NOT MISS)

The double-check pattern is CRITICAL to prevent lost wakeups:

```rust
// CORRECT ORDER
1. try_recv() fast path
2. wait_count.fetch_add(1, Release)  // Increment FIRST
3. try_recv() again  // Double-check AFTER increment
4. notifier.wait()  // Only if still empty
5. wait_count.fetch_sub(1, Relaxed)
```

If this order is wrong, messages WILL be lost under load.

### Memory Ordering (MUST NOT MISS)

When relaxing to `Relaxed`, MUST add Acquire fence before payload access:

```rust
// WRONG - Data race!
let tail = self.write_head().load(Ordering::Relaxed);
let data = &buffer[..];  // May see stale data!

// CORRECT
let tail = self.write_head().load(Ordering::Relaxed);
std::sync::atomic::fence(Ordering::Acquire);  // REQUIRED
let data = &buffer[..];  // Now safe
```

### PyO3 GIL Pattern (MUST NOT MISS)

Fast path MUST try with GIL first:

```rust
// WRONG - Always toggles GIL
let len = py.allow_threads(|| self.ring.recv(&mut buf))?;

// CORRECT - Only toggles if needed
match self.ring.try_recv(&mut buf) {
    Ok(len) => return Ok(len),  // Fast path, no GIL toggle
    Err(Empty) => {
        py.allow_threads(|| self.ring.recv(&mut buf))  // Slow path
    }
}
```

## Benchmark Baselines

Before any changes, record:

```bash
make bench-servers --servers vertebra,granian --scenarios hello,json \
  > specs/active/performance-100k-rps/tmp/baseline.json
```

Expected baseline:
- Vertebra hello: ~12K RPS
- Vertebra json: ~10K RPS
- Granian hello: ~65K RPS (reference)

## Rollback Plan

If changes cause issues:

1. **Feature flag:** All P0 changes can be behind `#[cfg(feature = "perf-opt")]`
2. **Git revert:** Each task should be a separate commit
3. **Binary switch:** Keep old binary for A/B testing

## Python-Side Performance Analysis (2026-01-28)

### Executive Summary

Deep investigation identified **6 major bottleneck categories** in the Python-side code, accounting for approximately **30-50us** of the ~90us per-request overhead. The Rust IPC layer is fast (17ns ping-pong), but Python asyncio overhead dominates.

### Detailed Bottleneck Breakdown (Per Request)

| Category | Location | Current Cost | Optimized Cost | Savings |
|----------|----------|--------------|----------------|---------|
| asyncio.create_task() | worker.py:436-442 | 2-5us | 0.02-0.05us* | 98% |
| asyncio.Queue creation | worker.py:923 | 5-10us | 0.5-1us* | 90% |
| Scope dict creation | worker.py:905-921 | 2-3us | 0.5-1us | 67% |
| Header encoding | worker.py:913 | 3-5us | 0.3-0.5us | 90% |
| Spin loop timing | async_transport.py:173-178 | 1-7us | 0.5-1us | 75% |
| Future creation (slow path) | async_transport.py:214 | 1-2us | 0.1-0.2us | 90% |
| **Total Python overhead** | | **14-32us** | **1.9-3.8us** | **88%** |

*With batch size of 100

### Critical Code Paths

**1. asyncio.create_task() per request (worker.py:436-442)**
```python
# Current: Creates task for EVERY message
for client_id, client in list(self._clients.items()):
    while True:
        payload = client.request.try_recv()
        if payload is None:
            break
        asyncio.create_task(self._handle_client_payload(...))  # 2-5us each!
```

**2. asyncio.Queue per request (worker.py:923)**
```python
# Current: Creates Queue object for every HTTP request
queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)  # 5-10us!
```

**3. Header encoding (worker.py:913)**
```python
# Current: Creates 2N string objects per request (N = number of headers)
headers = [(k.lower().encode(), v.encode()) for k, v in meta.headers]  # 3-5us!
```

**4. time.perf_counter() in spin loop (async_transport.py:173-178)**
```python
# Current: ~100-300ns per call, called 10-25 times per 5us spin
deadline = time.perf_counter() + self._spin_duration
while time.perf_counter() < deadline:  # Each call costs!
    data = self._ring.try_recv()
```

### Proposed Solutions

**P0-PY.1: Request Batching**
```python
# Proposed: Drain batch, process with single gather()
batch = []
for client_id, client in list(self._clients.items()):
    while len(batch) < MAX_BATCH_SIZE:
        payload = client.request.try_recv()
        if payload is None:
            break
        batch.append((client_id, client, payload))

if batch:
    tasks = [self._handle_client_payload(cid, c, p) for cid, c, p in batch]
    await asyncio.gather(*tasks, return_exceptions=True)
```

**P0-PY.2: Queue Bypass for Simple Responses**
```python
# Proposed: Use response holder list instead of Queue
response_holder = [None, None]  # [start_frame, body_frame]

async def send(message: dict[str, Any]) -> None:
    if message["type"] == "http.response.start":
        response_holder[0] = message
    elif message["type"] == "http.response.body" and not message.get("more_body"):
        response_holder[1] = message

await self._app(scope, receive, send)
if response_holder[0] and response_holder[1]:
    await writer.send_aggregated(response_holder[0], response_holder[1])
```

**P0-PY.3: Iteration-based Spin**
```python
# Proposed: Avoid perf_counter in tight loop
SPIN_ITERATIONS = 50  # Calibrated to ~5us on target hardware

for _ in range(SPIN_ITERATIONS):
    data = self._ring.try_recv()
    if data is not None:
        return data
```

**P1-PY.1: Scope Template Pattern**
```python
# Proposed: Pre-create immutable parts
_SCOPE_TEMPLATE = types.MappingProxyType({
    "type": "http",
    "asgi": types.MappingProxyType({"version": ASGI_VERSION, "spec_version": ASGI_HTTP_SPEC_VERSION}),
})

# Per-request: overlay mutable fields
scope = {**_SCOPE_TEMPLATE, "method": meta.method, "path": meta.path, ...}
```

**P1-PY.2: Lazy Header Encoding**
```python
# Proposed: Defer encoding until iteration
class LazyHeaders:
    __slots__ = ('_raw', '_cached')

    def __init__(self, raw: list[tuple[str, str]]):
        self._raw = raw
        self._cached: list[tuple[bytes, bytes]] | None = None

    def __iter__(self):
        if self._cached is None:
            self._cached = [(k.lower().encode(), v.encode()) for k, v in self._raw]
        return iter(self._cached)
```

### Expert Validation Notes

The Gemini expert analysis confirmed:
1. `asyncio.gather` creates internal futures - consider TaskGroup (Python 3.11+) or fire-and-forget pattern
2. PyBytes::new in recv_request copies data - should return memoryview from ring buffer
3. Scope dict creation can use lazy `__getitem__` to avoid upfront allocation
4. GIL contention on send_aggregated path needs verification
5. Batching needs backpressure limits to avoid starving ResponseWriter

### Expected Combined Impact

| Phase | RPS Target | Cumulative Improvement |
|-------|------------|------------------------|
| Baseline | 11K | 1x |
| P0-Rust (done) | 11K* | 1x (server notify issue) |
| P0-Python | 22K | 2x |
| P1-Python | 33K | 3x |
| P2-Python | 43K | 4x |
| + Multi-worker | 130K+ | 12x |

*Note: P0-Rust optimization impact was limited by vertebra-server unconditional notify issue

### Beads Issues Created

See `bd list --tag=python-perf` for tracking:
- P0-PY.1: Request Batching in Worker
- P0-PY.2: Queue Bypass for Single-Message Responses
- P0-PY.3: Iteration-based Spin
- P1-PY.1: Scope Template Pattern
- P1-PY.2: Lazy Header Encoding
- P1-PY.3: recv_batch Python Bindings
- P1-PY.4: Pre-create MessagePack Decoders
- P2-PY.1: ResponseWriter Flush Batching
- P2-PY.2: Zero-Copy Header View from Rust
- P2-PY.3: Fast Path Bypass for Native Runners

## Next Steps

1. ~~Run `/implement performance-100k-rps` to begin implementation~~ Done
2. ~~Start with Task 0.1 (add wait_count)~~ Done
3. ~~Run benchmarks to validate P0 improvement~~ Done (see findings above)
4. **FIX VERTEBRA-SERVER NOTIFY** - Critical blocker for Rust P0 gains
5. Proceed to P0-Python implementation (batching)
6. Proceed to P1 implementation (scope template, lazy headers)

## Contact Points

- **PRD:** `specs/active/performance-100k-rps/prd.md`
- **Tasks:** `specs/active/performance-100k-rps/tasks.md`
- **Analysis:** `specs/active/performance-100k-rps/research/analysis.md`
- **Patterns:** `.agent/patterns.md`
- **Related PRDs:** `.agent/specs/async-ipc-pipelining/prd.md`
