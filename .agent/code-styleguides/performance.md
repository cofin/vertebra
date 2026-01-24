# Performance Style Guide

Sub-10µs IPC patterns for Vertebra's shared-memory transport.

## Core Principles

### Never Use `asyncio.to_thread()` for IPC

`to_thread()` adds **50-130µs overhead** per call due to thread pool scheduling. This is unacceptable for high-frequency IPC.

```python
# BAD: 200µs+ latency
async def recv_slow():
    return await asyncio.to_thread(ring.recv_blocking)

# GOOD: 2µs latency
async def recv_fast():
    transport = AsyncShmTransport(ring)
    return await transport.recv()
```

### Use `AsyncShmTransport` with Hybrid Spin-Wait

The transport implements a three-phase receive pattern:

1. **Fast path**: `try_recv()` - immediate check (sub-1µs)
2. **Spin phase**: busy-wait for `VERTEBRA_SPIN_US` microseconds
3. **Slow path**: `add_reader()` and await notification

```python
from vertebra.ipc.async_transport import AsyncShmTransport

ring = AsyncRing(4096, 64)
transport = AsyncShmTransport(ring, spin_duration_us=5)

# This achieves p99 < 3µs for immediate data
data = await transport.recv()
```

### Configure Spin Duration

The `VERTEBRA_SPIN_US` environment variable controls spin phase duration:

| Value | Latency | CPU Cost | Use Case |
|-------|---------|----------|----------|
| 0 | Higher | Lowest | Battery-constrained |
| 2-3 | Lowest | Moderate | Ultra-low latency |
| 5 (default) | Low | Moderate | General use |
| 10+ | Moderate | Lower | Background tasks |

## Performance Targets

| Benchmark | Target | Achieved |
|-----------|--------|----------|
| Sync immediate | p99 < 10µs | 2.12µs |
| Async transport | p99 < 20µs | 2.02µs |
| 64 concurrent | p99 < 20µs | 1.72µs |
| Rust recv_hybrid | p99 < 5µs | 1.64µs |

## Removed Dead Code

The following legacy patterns were removed from the codebase:

| Removed | Reason |
|---------|--------|
| `Channel.send()` async | Used `to_thread` (100µs+ overhead) |
| `Channel.receive()` async | Used `to_thread` (100µs+ overhead) |
| `Channel.respond()` async | Used `to_thread` (100µs+ overhead) |
| `AsyncChannel` class | Unused, had slow polling (100µs sleep) |
| `AsyncChannel._recv_threaded()` | Dead fallback code |

Use `Channel.*_sync()` methods for synchronous operations, and `AsyncShmTransport` for async.

## Anti-Patterns

### Don't Use `asyncio.to_thread()` for IPC

```python
# BAD: 100µs+ overhead per call
await asyncio.to_thread(ring.recv_blocking)

# GOOD: Use transport directly
transport = AsyncShmTransport(ring)
await transport.recv()  # 2µs
```

### Don't Create Transports Per-Request

```python
# BAD: Transport creation overhead
async def handle_request():
    transport = AsyncShmTransport(ring)  # Creates new transport
    data = await transport.recv()
    transport.close()

# GOOD: Reuse transport
class Handler:
    def __init__(self, ring):
        self.transport = AsyncShmTransport(ring)

    async def handle_request(self):
        return await self.transport.recv()
```

### Don't Poll Without Spin Phase

```python
# BAD: Polling adds 100µs+ delay
while True:
    data = ring.try_recv()
    if data:
        return data
    await asyncio.sleep(0.0001)  # 100µs minimum

# GOOD: Hybrid spin-wait
transport = AsyncShmTransport(ring, spin_duration_us=5)
return await transport.recv()  # Spins then falls back
```

## Benchmarking

Run the benchmark suite to validate performance:

```bash
# Python benchmarks
uv run python benchmarks/ipc_fast_path.py

# Rust benchmarks
cargo bench --package vertebra-core --bench spin_wait
```

## Implementation Details

### Why Spin-Wait Works

1. **Avoids syscall overhead**: `try_recv()` is a pure memory read
2. **Captures immediate responses**: Most IPC responses arrive within 5µs
3. **Falls back gracefully**: `add_reader()` handles slow responses
4. **Industry standard**: Pattern used in LMAX Disruptor, Aeron, Solarflare

### GIL Considerations

The Rust layer releases the GIL during blocking waits:

```rust
// In PyO3
py.allow_threads(|| {
    ring.recv_blocking(timeout_ms)
})
```

This allows other Python threads to run during IPC waits.

## Integration with SQLSpec/Broker

The `BrokerClient` and SQLSpec extensions automatically use `AsyncShmTransport`:

```python
# This uses hybrid spin-wait internally
from vertebra.resources.broker import BrokerClient

async with BrokerClient(config) as client:
    result = await client.query("SELECT 1")  # Sub-20µs IPC
```

No code changes needed - upgrading Vertebra automatically improves latency.
