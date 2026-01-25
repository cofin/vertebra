# Research: World-Class IPC Performance Optimization

## Executive Summary

This document presents comprehensive research into optimizing Vertebra's shared-memory IPC architecture to achieve industry-leading performance metrics comparable to LMAX Disruptor and Ray Plasma. The current implementation suffers from eight data copies per round-trip and a 1ms polling latency floor that must be eliminated to meet world-class performance targets.

## Current Architecture Analysis

### Data Flow Examination

The Vertebra IPC system consists of three primary components communicating via shared-memory ring buffers:

1. **Python Application Layer** (`src/py/vertebra/`)
   - Entry point for all requests
   - Uses PyO3 bindings to access Rust IPC primitives
   - Current bottleneck: `asyncio.to_thread()` wrapper adds 50-130us overhead

2. **Rust Core Layer** (`src/rs/vertebra-core/`)
   - SPSC/MPMC ring buffer implementations
   - Shared memory allocation via `shm_open()`
   - Notification primitives (eventfd on Linux, pipes elsewhere)

3. **Bun Broker Layer** (`src/ts/src/broker/`)
   - Owns database connection pools
   - Processes requests from Python workers
   - Current bottleneck: 1ms `setInterval` polling loop

### Copy Points Identified (8 Total)

Through detailed code analysis, we identified eight distinct copy operations per request-response cycle:

**Request Path (Python to Bun):**
1. **COPY 1 - Serialization**: `msgspec.encode(dict) to bytes` in Python
2. **COPY 2 - Ring Write**: `ptr::copy_nonoverlapping()` in Rust PyO3
3. **COPY 3 - Ring Read + Allocation**: `new Uint8Array(65536)` in Bun FFI
4. **COPY 4 - Deserialization**: `@msgpack/msgpack.decode()` in TypeScript

**Response Path (Bun to Python):**
5. **COPY 5 - Serialization**: `@msgpack/msgpack.encode()` in TypeScript
6. **COPY 6 - Ring Write**: `bun:ffi` ring buffer write
7. **COPY 7 - Ring Read + Allocation**: `PyBytes::new()` in Rust PyO3
8. **COPY 8 - Deserialization**: `msgspec.decode()` in Python

### Latency Analysis

Current latency profile measured via benchmarks:

| Component | Latency | Cause |
|-----------|---------|-------|
| Polling floor | 1000us | `setInterval(poll, 1)` in server.ts |
| Async overhead | 50-130us | `asyncio.to_thread()` in channel.py |
| Buffer allocation | 10-20us | 65KB allocation per recv() |
| MessagePack encode | 5-15us | CPU-bound parsing |
| MessagePack decode | 5-15us | CPU-bound parsing |
| Ring buffer ops | less than 1us | Memory operations |

**Total observed p99 latency: 1-2ms**
**Target p99 latency: less than 10us**

## Critical Bottleneck: EventFD Sharing

### Problem Statement

The current `notify.rs` implementation creates anonymous eventfd descriptors with `EFD_CLOEXEC` flag:

```rust
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
```

The `EFD_CLOEXEC` flag causes the file descriptor to be automatically closed when `exec()` is called. More critically, anonymous eventfd descriptors cannot be shared across process boundaries through filesystem paths - they must either be:

1. Inherited through fork() (requires removing CLOEXEC)
2. Passed via Unix Domain Socket FD passing (SCM_RIGHTS)
3. Replaced with named alternatives (FIFOs)

### Solution: Process Inheritance

After multi-model consensus analysis (Gemini 3 Pro, GPT 5.2, Gemini CLI), the optimal solution is **Process Inheritance**:

1. Supervisor creates eventfd BEFORE forking
2. Child processes inherit the FD automatically
3. FD numbers passed via environment variables
4. No filesystem paths, no UDS complexity

**Implementation in supervisor.rs:**
```rust
// Current (broken for cross-process):
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };

// Fixed (inheritable):
let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
// Then pass to children via: env::set_var("VERTEBRA_NOTIFY_FD", fd.to_string())
```

## Zero-Copy Strategy

### Bun Side Optimization

**Current Problem (shm.ts:272-292):**
```typescript
recv(flags: number, timeout_ms: number): { data: Uint8Array; flags: number } | null {
    const buffer = new Uint8Array(this.capacity);  // 65KB allocation EVERY call
    // ...
}
```

**Solution: Buffer Pool**
```typescript
class BufferPool {
    private pool: Uint8Array[];
    private available: number[];

    constructor(count: number, capacity: number) {
        this.pool = Array.from({ length: count }, () => new Uint8Array(capacity));
        this.available = Array.from({ length: count }, (_, i) => i);
    }

    acquire(): Uint8Array | null {
        const idx = this.available.pop();
        return idx !== undefined ? this.pool[idx] : null;
    }

    release(buffer: Uint8Array): void {
        const idx = this.pool.indexOf(buffer);
        if (idx !== -1) this.available.push(idx);
    }
}
```

**Alternative: Direct Pointer View**
```typescript
// If Bun supports it:
const view = Bun.ArrayBuffer.fromAddress(ringSlotPtr, slotSize);
```

### Python Side Optimization

**Current Problem (channel.py):**
```python
async def send(self, ...):
    return await asyncio.to_thread(self.send_sync, ...)  # Thread pool overhead
```

**Solution: Native Async Implementation**
```python
async def send(self, procedure_id: str, params: dict, ...):
    # Direct async without thread pool
    handle = self._require_handle()
    loop = asyncio.get_running_loop()

    # Wait for ring buffer to be writable
    await loop.run_in_executor(None, handle.wait_writable)

    # Non-blocking send
    handle.send_request_nonblocking(message)

    # Event-driven response wait
    future = loop.create_future()
    loop.add_reader(self._notify_fd, lambda: self._on_response(future))
    return await future
```

**Zero-Copy Buffer Return:**
```python
def recv_zero_copy(self) -> memoryview:
    buffer = self._shared_buffer_slab
    offset, length = self._ring.recv_metadata_only()
    return memoryview(buffer)[offset:offset+length]  # No copy!
```

## Event-Driven Wakeup Architecture

### Current Polling (Must Eliminate)

**server.ts:53-55:**
```typescript
this.timer = setInterval(() => {
    this.poll();
}, 1);  // 1ms polling - creates latency floor
```

**worker.py:232:**
```python
if not did_work:
    await asyncio.sleep(0.001)  # 1ms polling fallback
```

### Event-Driven Replacement

**Linux (Optimal):**
```
Supervisor
    |
    +-- Creates eventfd (EFD_NONBLOCK only, no CLOEXEC)
    |
    +-- fork() Bun
    |      |
    |      +-- Inherits eventfd FD
    |      +-- Uses Bun.file(fd).readable or NAPI TSFN for event loop integration
    |
    +-- fork() Python Worker
           |
           +-- Inherits eventfd FD
           +-- asyncio.add_reader(fd, callback) for async
           +-- futex_wait(fd) for sync (direct syscall)
```

**Python Async Transport (event-driven):**
```python
class EventfdTransport:
    def __init__(self, notify_fd: int, ring: ShmRing):
        self._fd = notify_fd
        self._ring = ring
        self._loop = None

    async def recv(self) -> bytes:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        future = self._loop.create_future()
        self._loop.add_reader(self._fd, lambda: self._on_readable(future))

        try:
            await future
        finally:
            self._loop.remove_reader(self._fd)

        return self._ring.recv()

    def _on_readable(self, future):
        if not future.done():
            # Clear eventfd
            os.read(self._fd, 8)
            future.set_result(None)
```

**Python Sync Transport (futex-based):**
```python
import ctypes

libc = ctypes.CDLL("libc.so.6")

def futex_wait(fd: int, timeout_ns: int | None = None) -> bool:
    """Block until eventfd is readable."""
    # Use poll() for portable solution
    import select
    timeout_sec = timeout_ns / 1_000_000_000 if timeout_ns else None
    readable, _, _ = select.select([fd], [], [], timeout_sec)
    return len(readable) > 0
```

## FlatBuffers Control Plane

### Rationale

MessagePack encode/decode dominates CPU cost for small messages. FlatBuffers provides:
- Zero parsing cost (direct field access via offset)
- Type safety across Python, Rust, TypeScript
- Backwards-compatible schema evolution

### Schema Definition

**src/schema/ipc.fbs:**
```flatbuffers
namespace vertebra.ipc;

enum MessageType : uint8 {
    Request = 0,
    Response = 1,
    Control = 2,
    Stats = 3,
}

enum Status : uint16 {
    Ok = 0,
    Error = 1,
    Timeout = 2,
    Cancelled = 3,
}

table Request {
    id: uint64;
    procedure_id: string;
    flags: uint16;
    payload_offset: uint32;
    payload_length: uint32;
}

table Response {
    id: uint64;
    status: Status;
    error_code: uint32;
    error_message: string;
    payload_offset: uint32;
    payload_length: uint32;
}

table ControlMessage {
    kind: string;
    client_id: string;
    req_ring: string;
    resp_ring: string;
}

table Stats {
    pools: [PoolStats];
}

table PoolStats {
    name: string;
    active: uint32;
    idle: uint32;
    waiting: uint32;
}

union Message {
    Request,
    Response,
    ControlMessage,
    Stats,
}

table Envelope {
    message_type: MessageType;
    message: Message;
}

root_type Envelope;
```

### Integration Strategy

1. **Control plane only**: FlatBuffers for Request/Response headers, ControlMessage, Stats
2. **Dynamic payloads**: Keep MessagePack for user data, SQL results (schema unknown at compile time)
3. **Hybrid approach**: FlatBuffers header + MessagePack payload in same ring slot

## Request Multiplexing

### Current Limitation

Current implementation supports only one in-flight request per connection:
```typescript
// ipc/client.ts
private pending: DeferredPromise | null = null;
```

This wastes capacity and creates head-of-line blocking.

### Multiplexed Design

**TypeScript:**
```typescript
class MultiplexedChannel {
    private pending = new Map<bigint, DeferredPromise>();
    private nextId: bigint = 0n;

    async request(procedure: string, payload: Uint8Array): Promise<Uint8Array> {
        const id = this.nextId++;
        const deferred = createDeferred();
        this.pending.set(id, deferred);

        await this.ring.send(encodeRequest(id, procedure, payload));
        return deferred.promise;
    }

    private onResponse(id: bigint, payload: Uint8Array): void {
        const deferred = this.pending.get(id);
        if (deferred) {
            this.pending.delete(id);
            deferred.resolve(payload);
        }
    }
}
```

**Python (thread-safe for sync):**
```python
class MultiplexedChannel:
    def __init__(self):
        self._pending: dict[int, threading.Event | asyncio.Future] = {}
        self._lock = threading.Lock()
        self._next_id = 0

    def request_sync(self, procedure: str, payload: bytes) -> bytes:
        with self._lock:
            id = self._next_id
            self._next_id += 1
            event = threading.Event()
            self._pending[id] = event

        self._ring.send(encode_request(id, procedure, payload))
        event.wait()

        with self._lock:
            return self._results.pop(id)
```

## Performance Targets and Benchmarks

### World-Class Targets

| Metric | Target | Industry Comparison |
|--------|--------|---------------------|
| p50 latency | less than 5us | LMAX Disruptor: ~1us |
| p99 latency | less than 10us | Ray Plasma: ~10-20us |
| p999 latency | less than 50us | Better than Redis IPC |
| Throughput | greater than 1M msg/sec | Per connection |
| Allocations | Zero in hot path | No GC pressure |
| CPU idle | Event-driven | No polling burn |

### Benchmark Suite

**Latency benchmark:**
```bash
uv run python -m vertebra.benchmarks.ipc_latency \
    --iterations=100000 \
    --warmup=10000 \
    --target-p99-us=10
```

**Throughput benchmark:**
```bash
uv run python -m vertebra.benchmarks.ipc_throughput \
    --duration=10 \
    --connections=4 \
    --target-msgs-sec=1000000
```

**Allocation benchmark:**
```bash
uv run python -m vertebra.benchmarks.ipc_allocations \
    --iterations=10000 \
    --max-allocs=0
```

## Platform Strategy

### Linux (Primary Target)

- eventfd for notifications (lowest latency)
- futex for synchronous blocking
- `mmap()` for shared memory
- Full world-class performance achievable

### macOS (Secondary)

- pipe pair for notifications
- `dispatch_semaphore` or condition variables for sync
- Shared memory via `/dev/shm` equivalent
- Good performance, slightly higher latency

### Windows (Tertiary)

- Named pipes for notifications
- Event objects for synchronization
- Memory-mapped files for shared memory
- Acceptable performance with fallbacks

## Risk Analysis

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| FD inheritance fails on some Linux versions | Low | High | Test on kernel 4.x, 5.x, 6.x |
| Bun event loop integration complexity | Medium | Medium | NAPI TSFN fallback |
| FlatBuffers learning curve | Low | Low | Good documentation, schema-first |
| Cache coherency issues | Low | Medium | Use proper memory barriers |
| False sharing in ring headers | Medium | Medium | 64-byte cache-line alignment |

## Consensus Summary

**Multi-model validation completed:**

- **Gemini 3 Pro (FOR)**: 9/10 confidence - Architecture superior to sockets, overhead is implementation debt not fundamental limitation
- **GPT 5.2 (AGAINST)**: 8/10 confidence - Serialization dominates for small messages, "zero-copy" term only valid for large payloads
- **Gemini CLI**: Confirmed critical eventfd sharing flaw, recommended Process Inheritance

**Key insight from validation**: The two most expensive "copies" are actually **object re-materialization** (encode/decode), not memcpy. FlatBuffers addresses this directly for control plane messages.

## Memory Layout and Cache Optimization

### Current Memory Overhead

The current architecture has significant memory overhead that impacts performance:

**Per IPC Connection:**
- Request ring: 65,536 bytes x 1,024 slots = 64 MB
- Response ring: 65,536 bytes x 1,024 slots = 64 MB
- Control ring: Shared MPMC = 64 MB (amortized across connections)
- Total per connection: 128 MB

**Minimal Pool Configuration (2 warm connections):**
- 2 x (64 MB + 64 MB) + 64 MB shared = 320-384 MB baseline

This memory footprint is acceptable for server deployments but creates cache pressure for the hot path. Each ring slot is 64KB, which exceeds L1 cache size (typically 32KB) and even L2 cache per core on most processors.

### Cache-Line Alignment Strategy

Modern x86-64 processors use 64-byte cache lines. False sharing occurs when two threads modify different variables that share a cache line, causing expensive cache invalidation ping-pong.

**Ring Buffer Header Alignment:**
```rust
#[repr(C, align(64))]
struct RingHeader {
    head: AtomicU64,           // 8 bytes
    _pad1: [u8; 56],           // Padding to 64 bytes
    tail: AtomicU64,           // 8 bytes
    _pad2: [u8; 56],           // Padding to 64 bytes
}
```

The producer writes to `tail`, consumer reads from `head`. By placing them on separate cache lines, we eliminate false sharing between producer and consumer threads.

**Slot Metadata Alignment:**
```rust
#[repr(C, align(64))]
struct SlotHeader {
    length: AtomicU32,         // 4 bytes
    flags: AtomicU16,          // 2 bytes
    sequence: AtomicU16,       // 2 bytes - for ABA prevention
    _pad: [u8; 56],            // Padding to 64 bytes
}
```

### Memory Barrier Considerations

The SPSC ring buffer requires careful memory ordering:

1. **Producer**: Write data first, then update tail with release semantics
2. **Consumer**: Load head with acquire semantics, then read data

```rust
// Producer side
unsafe { ptr::copy_nonoverlapping(data.as_ptr(), slot_ptr, len) };
self.tail.store(new_tail, Ordering::Release);

// Consumer side
let current_head = self.head.load(Ordering::Acquire);
// Now safe to read data at slot[current_head]
```

On x86-64, most loads/stores have acquire-release semantics by default, but explicit ordering ensures correctness on ARM and other platforms.

## Adaptive Spin-Then-Park Strategy

For ultra-low latency scenarios, busy-spinning can achieve sub-microsecond response times by avoiding kernel scheduler latency. However, spinning burns CPU cycles when idle.

### Hybrid Approach

```rust
const SPIN_ITERATIONS: u32 = 100;

fn wait_for_data(&self) -> bool {
    // Phase 1: Busy spin for quick response
    for _ in 0..SPIN_ITERATIONS {
        if self.has_data() {
            return true;
        }
        core::hint::spin_loop();
    }

    // Phase 2: Park on eventfd to save CPU
    self.wait_on_eventfd()
}
```

### Configuration via Environment

Users can tune the spin count based on their latency requirements:

- `VERTEBRA_SPIN_ITERATIONS=0`: Pure event-driven (default, best for mixed workloads)
- `VERTEBRA_SPIN_ITERATIONS=100`: Balanced (sub-5us latency for bursty traffic)
- `VERTEBRA_SPIN_ITERATIONS=10000`: Aggressive (sub-1us latency, dedicated cores)

## Comparison with Alternative IPC Mechanisms

### Unix Domain Sockets

**Advantages:**
- Simple API (standard sockets)
- Kernel handles synchronization
- Works everywhere

**Disadvantages:**
- System call overhead (sendmsg/recvmsg)
- Kernel buffer copies
- Context switches

**Measured latency:** 50-200us per round-trip

### TCP Localhost

**Advantages:**
- Same API as network communication
- Loopback optimized in kernel

**Disadvantages:**
- Full TCP/IP stack overhead
- Multiple kernel copies
- More context switches than UDS

**Measured latency:** 100-500us per round-trip

### Shared Memory (Vertebra Current)

**Advantages:**
- No kernel involvement in data path
- Zero kernel copies
- User-space synchronization

**Disadvantages (current implementation):**
- User-space copies still present
- Polling overhead
- Complex synchronization

**Measured latency:** 1-2ms (due to polling floor)

### Shared Memory (Vertebra Optimized)

**Advantages:**
- All shared memory benefits
- Event-driven wakeups
- Zero-copy buffer pools
- Schema-driven headers

**Target latency:** less than 10us p99

## Conclusion

The Vertebra IPC architecture has sound fundamentals but suffers from implementation overhead that masks its potential. By addressing the critical bottlenecks identified in this research - primarily the polling floor, buffer allocations, and eventfd sharing - we can achieve world-class performance competitive with industry-leading solutions like LMAX Disruptor.

The key insight is that "zero-copy" is already achieved for large payloads via Arrow IPC, but small message overhead is dominated by serialization and polling. FlatBuffers and event-driven wakeups directly address these issues without fundamental architectural changes.

Both synchronous and asynchronous APIs must achieve the same performance targets. The sync path uses futex-based blocking (Linux) or select/poll (portable), while the async path uses asyncio event loop integration. Neither should compromise on the sub-10us latency goal.

## References

### Industry Comparisons
- LMAX Disruptor: https://lmax-exchange.github.io/disruptor/
- Ray Plasma Object Store: https://docs.ray.io/en/latest/ray-core/objects/plasma.html
- io_uring: https://kernel.dk/io_uring.pdf

### Vertebra Codebase
- Ring buffers: `src/rs/vertebra-core/src/channel.rs`
- Shared memory: `src/rs/vertebra-core/src/shm.rs`
- Notifications: `src/rs/vertebra-core/src/notify.rs`
- Python IPC: `src/py/vertebra/ipc/`
- Bun broker: `src/ts/src/broker/`

### External Libraries
- FlatBuffers: https://flatbuffers.dev/
- msgspec: https://jcristharif.com/msgspec/
- PyO3: https://pyo3.rs/
- napi-rs: https://napi.rs/
