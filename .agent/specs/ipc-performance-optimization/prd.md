# PRD: World-Class IPC Performance Optimization

## Intelligence Context

### Complexity Assessment

- **Classification**: High
- **Estimated Phases**: 4
- **Risk Level**: Medium (requires careful validation across three languages)
- **Breaking Changes**: Minimal (optimizing existing APIs, not removing functionality)

### Similar Features Referenced

1. `src/rs/vertebra-core/src/channel.rs` - SPSC/MPMC ring buffer implementation (follow patterns)
2. `src/rs/vertebra-core/src/notify.rs` - Current notification primitives (modify for inheritance)
3. `src/py/vertebra/ipc/async_transport.py` - Async transport with event loop integration (extend)
4. `src/ts/src/broker/server.ts` - Broker polling loop (replace with event-driven)
5. `src/rs/vertebra-py/src/supervisor.rs` - Process supervisor (add FD creation before fork)

### Patterns to Follow

- **Rust IPC**: Use `std::sync::atomic` with explicit memory ordering
- **Python Async**: Use `asyncio.add_reader()` for FD-based event notification
- **TypeScript**: Use buffer pools, avoid per-message allocations
- **Cross-Process FD**: Use process inheritance via fork(), pass FD numbers via env vars

### Detected Stack and Commands

- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **Test Commands**: `make test`, `make js-test`, `make rs-test`, `make test-all`
- **Lint Command**: `make lint-all`
- **Build Command**: `make install`

---

## Problem Statement

### User Problem Being Solved

The Vertebra shared-memory IPC architecture currently exhibits poor latency characteristics that undermine the framework's value proposition as a high-performance application backbone. Users expecting sub-millisecond response times experience 1-2ms latency floors due to polling-based implementations, making Vertebra unsuitable for latency-sensitive applications like real-time trading, game servers, or interactive AI inference.

The current implementation wastes significant CPU cycles on polling even when idle, and allocates memory on every message receive, creating garbage collection pressure that causes unpredictable latency spikes. These issues stem from implementation choices, not architectural limitations, and can be resolved without breaking API compatibility.

### Business Value

1. **Latency Reduction**: Transform Vertebra from a 1-2ms IPC system to a sub-10us system, making it competitive with industry leaders like LMAX Disruptor and Ray Plasma
2. **Throughput Increase**: Enable greater than 1M messages per second per connection through multiplexing and zero-copy optimizations
3. **CPU Efficiency**: Eliminate polling-induced CPU burn, allowing idle servers to consume minimal resources
4. **Memory Stability**: Achieve zero per-message allocations, eliminating GC-induced latency spikes
5. **Developer Experience**: Maintain existing API while delivering dramatically improved performance

### Success Criteria

| Metric | Current | Target | Measurement Method |
|--------|---------|--------|-------------------|
| p50 latency | ~1ms | less than 5us | `ipc_latency` benchmark |
| p99 latency | 1-2ms | less than 10us | `ipc_latency` benchmark |
| p999 latency | 2-5ms | less than 50us | `ipc_latency` benchmark |
| Throughput | ~100K msg/sec | greater than 1M msg/sec | `ipc_throughput` benchmark |
| Hot path allocations | 1 per message | Zero | `ipc_allocations` benchmark |
| Idle CPU usage | 1-5% per core | less than 0.1% | System monitoring |
| Sync/Async parity | N/A | Both achieve targets | `sync_async_parity` benchmark |

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Process Inheritance for FD Sharing

- [ ] `NativeSupervisor` in Rust creates eventfd (Linux) or pipe pair (other platforms) BEFORE spawning children
- [ ] Eventfd created WITHOUT `EFD_CLOEXEC` flag to allow inheritance
- [ ] FD numbers passed to children via environment variables: `VERTEBRA_NOTIFY_FD`, `VERTEBRA_SHM_FD`
- [ ] Child processes (Bun, Python workers) open inherited FDs by number, not by path
- [ ] Works on Linux kernel 4.x, 5.x, and 6.x

#### FR-2: Zero-Copy Buffer Pools

- [ ] `BufferPool` class in TypeScript pre-allocates ArrayBuffers at startup
- [ ] `ShmRing.recv()` uses pooled buffers instead of allocating per call
- [ ] Python receives `memoryview` slices instead of copying to `PyBytes`
- [ ] Buffer pool size configurable via `VERTEBRA_BUFFER_POOL_SIZE` (default: 64)
- [ ] Pool exhaustion handled gracefully (blocking wait or error)

#### FR-3: Event-Driven Wakeups (Bun)

- [ ] Remove `setInterval(poll, 1)` from `BrokerServer.start()`
- [ ] Integrate inherited eventfd with Bun event loop
- [ ] Options: `Bun.file(fd).readable` stream or NAPI ThreadSafeFunction
- [ ] Poll only when notified, not on timer

#### FR-4: Event-Driven Wakeups (Python Async)

- [ ] `AsyncShmTransport` uses `asyncio.add_reader(notify_fd, callback)`
- [ ] Remove `asyncio.sleep(0.001)` fallback from worker loop
- [ ] Clear eventfd after wakeup to prevent spurious notifications
- [ ] Works with uvloop if installed

#### FR-5: Event-Driven Wakeups (Python Sync)

- [ ] Sync methods use direct `select.select()` or `poll()` on eventfd
- [ ] No thread pool overhead (`asyncio.to_thread()` eliminated from sync path)
- [ ] Timeout support via select/poll timeout parameter
- [ ] Works correctly with multiple threads sharing same channel

#### FR-6: FlatBuffers Control Plane

- [ ] Schema defined in `src/schema/ipc.fbs`
- [ ] Rust bindings generated to `src/rs/vertebra-core/src/protocol/`
- [ ] Python bindings via `flatbuffers` package
- [ ] TypeScript bindings via `flatbuffers` npm package
- [ ] Request/Response headers use FlatBuffers (zero parsing cost)
- [ ] Dynamic payloads (user data, SQL results) remain MessagePack

#### FR-7: Request Multiplexing

- [ ] Each connection supports N concurrent in-flight requests (default N=64)
- [ ] Correlation ID in every request/response
- [ ] Responses matched to requests by correlation ID (out-of-order OK)
- [ ] Both sync and async APIs support multiplexing
- [ ] No head-of-line blocking

#### FR-8: Both Sync AND Async Achieve Targets

- [ ] Sync path: sub-10us p99 latency
- [ ] Async path: sub-10us p99 latency
- [ ] Benchmark proves parity: `ipc_sync_async_parity`
- [ ] No functionality removed from sync APIs

### Non-Functional Requirements

#### NFR-1: Platform Support

- [ ] Linux: Full optimization (eventfd, futex, world-class performance)
- [ ] macOS: Good performance (pipe pair, condition variables)
- [ ] Windows: Acceptable performance (named pipes, events)
- [ ] Graceful degradation on non-Linux platforms

#### NFR-2: Backwards Compatibility

- [ ] Existing `Channel` API unchanged
- [ ] Existing `BrokerClient` API unchanged
- [ ] No breaking changes to public interfaces
- [ ] Internal optimizations only

#### NFR-3: Observability

- [ ] Latency percentiles available via metrics
- [ ] Buffer pool utilization observable
- [ ] Event wakeup counts trackable

---

## Technical Approach

### Architecture Overview

```
BEFORE (Current - Polling):

+------------------+     +------------------+     +------------------+
|  Python Worker   |     |   Shared Memory  |     |   Bun Broker     |
|                  |     |                  |     |                  |
| asyncio.sleep()  |---->|   Ring Buffer    |<----|  setInterval()   |
|   (1ms poll)     |     |                  |     |    (1ms poll)    |
+------------------+     +------------------+     +------------------+

AFTER (Optimized - Event-Driven):

+------------------+     +------------------+     +------------------+
|  Python Worker   |     |   Shared Memory  |     |   Bun Broker     |
|                  |     |   + eventfd      |     |                  |
| add_reader(fd)   |<====|   Ring Buffer    |====>| Bun.file(fd)     |
|  (instant wake)  |     |                  |     |  (instant wake)  |
+------------------+     +------------------+     +------------------+
                              ^
                              |
                    +------------------+
                    |   Supervisor     |
                    |  (creates FDs    |
                    |   before fork)   |
                    +------------------+
```

### Pattern References from Codebase

**Ring Buffer Pattern (channel.rs:513-689):**
```rust
pub struct SpscRing<T> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    buffer: *mut T,
    capacity: usize,
}
```
Follow this pattern for new `InheritableNotifier` and cache-line alignment.

**Async Transport Pattern (async_transport.py):**
```python
class AsyncShmTransport:
    async def recv(self) -> bytes:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        loop.add_reader(self._notify_fd, lambda: self._handle_readable(future))
        # ...
```
Extend this pattern, currently uses polling fallback which must be removed.

**Buffer Pool Pattern (from industry - to implement):**
```typescript
class BufferPool {
    private readonly pool: Uint8Array[];
    private readonly available: number[];
    // Pre-allocate all buffers at construction
}
```

### File Structure Plan

**New Files:**
- `src/schema/ipc.fbs` - FlatBuffers schema
- `src/rs/vertebra-core/src/protocol/` - Generated FlatBuffers Rust code
- `src/py/vertebra/protocol/flatbuffers/` - Generated Python code
- `src/ts/src/protocol/flatbuffers/` - Generated TypeScript code

**Modified Files:**
- `src/rs/vertebra-core/src/notify.rs` - Add `InheritableNotifier`
- `src/rs/vertebra-py/src/supervisor.rs` - Create FDs before fork
- `src/py/vertebra/supervisor.py` - Pass FD numbers via env vars
- `src/ts/src/broker/server.ts` - Replace polling with event-driven
- `src/ts/src/broker/shm.ts` - Add `BufferPool` class
- `src/py/vertebra/ipc/channel.py` - Optimize sync methods
- `src/py/vertebra/ipc/async_transport.py` - Remove polling fallback
- `src/py/vertebra/server/worker.py` - Use event-driven wakeup

### API Changes

**No public API changes.** All optimizations are internal implementation details.

Internal changes:
- New `InheritableNotifier` Rust struct (internal)
- New `BufferPool` TypeScript class (internal)
- New environment variables for FD passing (documented for advanced users)

### Database Changes

None. This PRD focuses on IPC performance, not data persistence.

### Documentation Updates Required

- `docs/architecture/performance-tuning.md` - Add IPC tuning section
- `docs/architecture/python-ipc-guide.md` - Update with event-driven details
- `CLAUDE.md` - Add IPC performance notes if significant patterns emerge

---

## Testing Strategy

### Unit Tests

**Rust (src/rs/vertebra-core/src/notify.rs):**
```rust
#[test]
fn test_inheritable_notifier_roundtrip() {
    let notifier = InheritableNotifier::new().unwrap();
    assert!(!notifier.has_pending());
    notifier.notify().unwrap();
    assert!(notifier.has_pending());
    notifier.clear().unwrap();
    assert!(!notifier.has_pending());
}

#[test]
fn test_notifier_fd_inheritance() {
    // Fork and verify child can use parent's FD
}
```

**TypeScript (src/ts/src/broker/shm.test.ts):**
```typescript
describe('BufferPool', () => {
    it('should not allocate after warmup', () => {
        const pool = new BufferPool(10, 65536);
        // Acquire and release 1000 times
        // Verify no new allocations via performance API
    });

    it('should handle pool exhaustion', () => {
        const pool = new BufferPool(2, 65536);
        const b1 = pool.acquire();
        const b2 = pool.acquire();
        expect(pool.acquire()).toBeNull();
    });
});
```

**Python (src/py/tests/unit/test_ipc_performance.py):**
```python
def test_buffer_pool_zero_allocation():
    """Verify buffer pool doesn't allocate per message."""
    pool = BufferPool(size=64, capacity=65536)
    import tracemalloc
    tracemalloc.start()
    for _ in range(10000):
        buf = pool.acquire()
        pool.release(buf)
    current, peak = tracemalloc.get_traced_memory()
    assert peak < 1024  # Less than 1KB allocated

def test_sync_and_async_parity():
    """Verify sync and async achieve same latency."""
    sync_p99 = measure_sync_latency()
    async_p99 = measure_async_latency()
    assert sync_p99 < 10_000  # 10us in ns
    assert async_p99 < 10_000
```

### Integration Tests

```bash
# Run full IPC round-trip test
uv run pytest src/py/tests/integration/broker/ -v

# Verify latency requirements
uv run pytest src/py/tests/integration/broker/test_latency.py -v

# Verify FD inheritance across processes
uv run pytest src/py/tests/integration/supervisor/test_fd_inheritance.py -v

# Verify event-driven wakeups
uv run pytest src/py/tests/integration/ipc/test_event_driven.py -v
```

### Benchmark Tests

```bash
# Latency benchmark (MUST show less than 10us p99)
uv run python -m vertebra.benchmarks.ipc_latency --target-p99-us=10

# Throughput benchmark (MUST show greater than 1M msg/sec)
uv run python -m vertebra.benchmarks.ipc_throughput --target-msgs-sec=1000000

# Allocation benchmark (MUST show zero per-message allocations)
uv run python -m vertebra.benchmarks.ipc_allocations --max-allocs=0

# Sync vs Async parity benchmark
uv run python -m vertebra.benchmarks.ipc_sync_async_parity
```

### Coverage Requirements

- Unit tests: 90%+ coverage for new code
- Integration tests: All critical paths tested
- Benchmark tests: All performance targets validated

---

## Implementation Notes

### Pattern Deviations (with Rationale)

1. **Removing `EFD_CLOEXEC`**: Standard practice is to set this flag, but we need FD inheritance across fork(). This is intentional and safe because the supervisor controls all child processes.

2. **Direct syscalls for sync blocking**: Instead of using Python's `threading.Event`, we use `select.select()` directly on the eventfd. This eliminates Python overhead in the sync hot path.

3. **FlatBuffers for headers only**: Unlike typical FlatBuffers usage where entire messages are schema-driven, we use FlatBuffers only for control plane headers. Dynamic payloads (user data) remain MessagePack because their schema is not known at compile time.

### Dependencies

**New Dependencies:**

*Python:*
- `flatbuffers>=24.3.25` - FlatBuffers runtime (already used internally)

*TypeScript:*
- `flatbuffers` - FlatBuffers runtime
- No new runtime dependencies for buffer pooling

*Rust:*
- `flatbuffers` crate - FlatBuffers runtime

**Build Dependencies:**
- `flatc` - FlatBuffers compiler (for schema generation)

### Migration Considerations

No migration required. This is a performance optimization with no API changes or data format changes. Existing deployments will automatically benefit from the improvements after upgrade.

### Rollback Strategy

If issues are discovered post-deployment:
1. Set `VERTEBRA_LEGACY_POLLING=1` to re-enable polling (escape hatch)
2. Buffer pool can be disabled via `VERTEBRA_BUFFER_POOL_SIZE=0`
3. Full rollback via version downgrade if needed

---

## Verification Checklist

### Functional Verification

- [ ] `make test` passes
- [ ] `make js-test` passes
- [ ] `make rs-test` passes
- [ ] `make lint-all` passes

### Performance Verification

- [ ] p50 latency less than 5us (benchmark)
- [ ] p99 latency less than 10us (benchmark)
- [ ] p999 latency less than 50us (benchmark)
- [ ] Throughput greater than 1M msg/sec per connection (benchmark)
- [ ] Zero per-message allocations in hot path (profiler)
- [ ] Both sync AND async achieve targets (parity benchmark)
- [ ] Idle CPU usage less than 0.1% per core (system monitoring)

### Architecture Verification

- [ ] FD inheritance working (strace verification on Linux)
- [ ] Event-driven wakeups in Bun (no setInterval in server.ts)
- [ ] Event-driven wakeups in Python (no asyncio.sleep in worker.py)
- [ ] FlatBuffers used for control plane
- [ ] MessagePack only for dynamic payloads
- [ ] Buffer pool working in Bun (allocation profiling)

### Platform Verification

- [ ] Linux: Full performance achieved
- [ ] macOS: Acceptable performance (pipe fallback)
- [ ] Windows: Acceptable performance (named pipe fallback)

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| FD inheritance fails on some Linux versions | Low | High | Test on kernel 4.x, 5.x, 6.x; document requirements |
| Bun event loop integration complexity | Medium | Medium | NAPI TSFN fallback; poll with Bun.sleep(0) as last resort |
| FlatBuffers integration effort | Low | Medium | Schema-first design; generate all bindings upfront |
| Platform parity (Linux vs macOS) | Medium | Low | Linux is primary target; document platform differences |
| Sub-10us target unachievable on busy system | Medium | Medium | Adaptive spin-then-park; CPU affinity options |
| Buffer pool exhaustion under load | Low | Medium | Configurable pool size; blocking wait option |

---

## Consensus Sources

- **Gemini 3 Pro (FOR stance)**: 9/10 confidence - Architecture validated as superior to sockets, overhead is implementation debt not fundamental limitation
- **GPT 5.2 (AGAINST stance)**: 8/10 confidence - Serialization dominates for small messages, "zero-copy" only valid for large payloads
- **Gemini CLI Validation**: Confirmed 65KB allocation issue, identified critical eventfd sharing flaw, recommended Process Inheritance
- **User Feedback**: World-class targets required (p99 less than 10us), both sync AND async must be optimized, Process Inheritance is the correct approach regardless of complexity

---

## Implementation Phases

### Phase 1: Zero-Copy Foundation + Supervisor FD Inheritance

**Goal:** Establish the FD inheritance architecture and eliminate allocations.

**Detailed Tasks:**

1. **Supervisor FD Creation (Rust)**
   - File: `src/rs/vertebra-py/src/supervisor.rs`
   - Create eventfd (Linux) / pipes (other) BEFORE forking children
   - Create shared memory regions BEFORE forking
   - Store FD numbers in struct for passing to children
   - Remove `EFD_CLOEXEC` flag from eventfd creation

2. **FD Passing via Environment**
   - File: `src/py/vertebra/supervisor.py`
   - Pass FD numbers: `VERTEBRA_NOTIFY_FD`, `VERTEBRA_SHM_FD`, `VERTEBRA_RING_FD`
   - Children open inherited FDs by number, not by creating new ones
   - Validate FD numbers are valid integers before use

3. **Buffer Pool in Bun (Zero Allocation)**
   - File: `src/ts/src/broker/shm.ts`
   - Create `BufferPool` class with pre-allocated ArrayBuffer set
   - Pool size equals number of ring slots (no per-message allocation)
   - Use `Bun.ArrayBuffer.fromAddress()` for true zero-copy when possible
   - Handle pool exhaustion gracefully

4. **Python Buffer Protocol (Zero Allocation)**
   - File: `src/py/vertebra/ipc/channel.py`
   - Pre-allocate buffer slab in shared memory
   - Return `memoryview()` slices instead of copying to `PyBytes`
   - Both sync AND async paths use same zero-copy mechanism
   - Remove `_BufferProxy` fallback class

**Validation:** `make test-all`, verify no allocations in hot path via profiling

### Phase 2: Event-Driven Wakeups (Sub-10us Target)

**Goal:** Replace all polling with event-driven notifications using inherited FDs.

**Detailed Tasks:**

1. **InheritableNotifier in Rust**
   - File: `src/rs/vertebra-core/src/notify.rs`
   - Add `InheritableNotifier::create()` for supervisor use
   - Add `InheritableNotifier::from_fd(fd)` for child process use
   - Linux: eventfd with `EFD_NONBLOCK` only (no `EFD_CLOEXEC`)
   - Other platforms: pipe pair with similar inheritance model

2. **Bun Event Loop Integration**
   - File: `src/ts/src/broker/server.ts`
   - Remove `setInterval(poll, 1)` at line 53-55
   - Add `Bun.file(inheritedFd).readable` for event-driven wakeup
   - Fallback: NAPI ThreadSafeFunction if Bun.file() does not work
   - Verify no CPU burn when idle

3. **Python Async Transport**
   - File: `src/py/vertebra/ipc/async_transport.py`
   - Use `asyncio.get_event_loop().add_reader(notify_fd, callback)`
   - Remove `asyncio.sleep(0.001)` polling fallback completely
   - Clear eventfd after each wakeup to prevent spurious notifications
   - Test with both default event loop and uvloop

4. **Python Sync Transport**
   - File: `src/py/vertebra/ipc/channel.py`
   - Use `select.select([fd], [], [], timeout)` for sync blocking
   - Direct syscall, no thread pool overhead
   - Support configurable timeout
   - Thread-safe for multiple threads using same channel

5. **Worker Polling Removal**
   - File: `src/py/vertebra/server/worker.py`
   - Remove `await asyncio.sleep(0.001)` at line 232
   - Replace with event-driven wakeup on inherited notify FD
   - Ensure worker exits cleanly on shutdown signal

**Validation:** Latency benchmark showing p99 less than 10us

### Phase 3: FlatBuffers Control Plane (Zero Parse Cost)

**Goal:** Eliminate MessagePack encode/decode overhead for control messages.

**Detailed Tasks:**

1. **Define FlatBuffers Schema**
   - File: `src/schema/ipc.fbs` (new)
   - Define: `Request`, `Response`, `ControlMessage`, `Stats`
   - Schema-driven format means zero-copy reads and no parsing overhead
   - Use inline structs for fixed-size fields

2. **Rust FlatBuffers Generation**
   - File: `src/rs/vertebra-core/src/protocol/`
   - Generate Rust structs from schema using `flatc`
   - Use `flatbuffers` crate for runtime
   - Add to build.rs for automatic regeneration

3. **Python FlatBuffers**
   - Use `flatbuffers` Python package
   - Zero-copy access to fields via offset
   - Generate Python code from schema
   - Add to setup.py/pyproject.toml build process

4. **TypeScript FlatBuffers**
   - Use `flatbuffers` npm package
   - Direct buffer access, no decode step
   - Generate TypeScript code from schema
   - Add to package.json build scripts

5. **Keep MessagePack for Dynamic Payloads**
   - User data, SQL results stay MessagePack
   - Only control plane uses FlatBuffers
   - Hybrid approach: FlatBuffers header + MessagePack payload

**Validation:** p99 latency less than 10us maintained, CPU usage reduced

### Phase 4: Request Multiplexing + Polish

**Goal:** Maximize throughput with concurrent in-flight requests.

**Detailed Tasks:**

1. **Correlation ID Tracking**
   - File: `src/ts/src/ipc/client.ts`
   - Replace single `pending` with `Map<number, DeferredPromise>`
   - Allow N concurrent in-flight requests (default N=64)
   - Use atomic counter for correlation ID generation

2. **Out-of-Order Response Handling**
   - File: `src/ts/src/broker/server.ts`
   - Match responses by correlation ID
   - No head-of-line blocking
   - Handle orphaned responses gracefully

3. **Python Multiplexed Client**
   - File: `src/py/vertebra/resources/broker.py`
   - Both sync and async support concurrent requests
   - Sync uses thread-safe pending map with locks
   - Async uses asyncio.Future-based tracking

4. **Adaptive Spin-Then-Park** (Optional, Ultra-Low Latency)
   - Busy-spin for configurable iterations before parking on eventfd
   - Environment variable: `VERTEBRA_SPIN_ITERATIONS`
   - Trades CPU for sub-1us latency when traffic is bursty
   - Default: 0 (pure event-driven)

5. **Cache-Line Alignment**
   - Ensure ring buffer headers are 64-byte aligned
   - Prevent false sharing between producer/consumer
   - Use `#[repr(C, align(64))]` in Rust
   - Document memory layout for debugging

**Validation:** Throughput greater than 1M msg/sec, 64+ concurrent requests

---

## Detailed Technical Specifications

### EventFD Inheritance Protocol

The supervisor creates notification FDs using the following protocol:

```
1. Supervisor starts (Rust NativeSupervisor)
2. Create eventfd: libc::eventfd(0, EFD_NONBLOCK)
   - Note: NO EFD_CLOEXEC flag
3. Create shared memory: shm_open("/vertebra.{pid}.shm", O_CREAT|O_RDWR)
4. Initialize ring buffers in shared memory
5. Store FD numbers in environment:
   - VERTEBRA_NOTIFY_FD=5
   - VERTEBRA_SHM_FD=6
6. fork() Bun engine
   - Child inherits FD 5, FD 6
   - Child reads env vars to know FD numbers
7. fork() Python workers
   - Same inheritance pattern
8. All processes now share eventfd and shared memory
```

### Buffer Pool Specification

**TypeScript BufferPool:**
```typescript
interface BufferPoolConfig {
    count: number;      // Number of buffers (default: 64)
    capacity: number;   // Size of each buffer (default: 65536)
}

class BufferPool {
    constructor(config: BufferPoolConfig);
    acquire(): Uint8Array | null;  // Returns null if exhausted
    release(buffer: Uint8Array): void;
    stats(): { total: number; available: number; };
}
```

**Python BufferPool:**
```python
class BufferPool:
    def __init__(self, count: int = 64, capacity: int = 65536) -> None: ...
    def acquire(self) -> memoryview | None: ...
    def release(self, buffer: memoryview) -> None: ...
    @property
    def stats(self) -> BufferPoolStats: ...
```

### FlatBuffers Schema Details

The FlatBuffers schema uses fixed-size inline structs for performance-critical fields:

```flatbuffers
// Fixed header (24 bytes, fits in half a cache line)
struct RequestHeader {
    id: uint64;           // Correlation ID
    flags: uint16;        // Request flags
    procedure_len: uint16; // Length of procedure name
    payload_offset: uint32; // Offset to payload in buffer
    payload_len: uint32;  // Length of payload
}

table Request {
    header: RequestHeader (required);
    procedure: string;    // Procedure name
    // Payload is NOT in FlatBuffers - it's raw bytes at payload_offset
}
```

This design allows:
1. Fixed header can be read without any parsing
2. Procedure name lookup is single string dereference
3. Payload bytes are accessed directly at offset

---

## Appendix: Key Code Locations

### TypeScript/Bun

| File | Purpose | Changes |
|------|---------|---------|
| `src/ts/src/broker/server.ts:53-55` | Polling loop | Replace with event-driven |
| `src/ts/src/broker/shm.ts:272-292` | Ring recv | Add buffer pool |
| `src/ts/src/ipc/client.ts:154-182` | IPC client | Add multiplexing |

### Rust

| File | Purpose | Changes |
|------|---------|---------|
| `src/rs/vertebra-core/src/notify.rs` | Notifications | Add InheritableNotifier |
| `src/rs/vertebra-core/src/channel.rs` | Ring buffers | Cache-line alignment |
| `src/rs/vertebra-py/src/supervisor.rs` | Supervisor | Create FDs before fork |

### Python

| File | Purpose | Changes |
|------|---------|---------|
| `src/py/vertebra/supervisor.py` | Supervisor | Pass FD numbers |
| `src/py/vertebra/ipc/channel.py:210-349` | Channel | Optimize sync methods |
| `src/py/vertebra/ipc/async_transport.py` | Async transport | Remove polling |
| `src/py/vertebra/server/worker.py:232` | Worker | Event-driven wakeup |
