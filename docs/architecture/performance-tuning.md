# Performance Tuning Guide

Vertebra is designed for high-performance data applications. This guide details configuration options and architectural decisions that impact performance, particularly the choice between MessagePack and Arrow serialization.

## Serialization Strategy: MessagePack vs Arrow

Vertebra employs a dual-serialization strategy to optimize for both low-latency small messages and high-throughput large datasets.

-   **MessagePack (msgspec):** Used for control messages, small payloads, and RPC calls. It is extremely fast for small objects but incurs per-row overhead for tabular data.
-   **Apache Arrow:** Used for large tabular datasets. It utilizes column-oriented memory layout and zero-copy IPC, making it significantly faster for bulk data transfer.

### Benchmark Results

The following benchmark compares serialization time for a dataset with 4 columns (`id: int`, `name: str`, `value: float`, `active: bool`).

| Rows | MsgPack (us) | Arrow (us) | Speedup |
|------|--------------|------------|---------|
| 100 | 5.08 | 9.52 | 0.53x |
| 1,000 | 49.32 | 11.62 | 4.24x |
| 10,000 | 471.15 | 31.71 | 14.86x |
| 100,000 | 5,332.91 | 196.57 | 27.13x |

**Analysis:**
-   **Small Data (< 1k rows):** MessagePack is faster or comparable. The overhead of setting up an Arrow RecordBatch stream outweighs the per-row serialization cost.
-   **Large Data (> 1k rows):** Arrow dominates. At 100k rows, Arrow is **27x faster** than MessagePack.

### Configuration

You can configure the threshold at which Vertebra switches from MessagePack to Arrow for SQL results.

```python
# src/py/vertebra/_config.py

# Default threshold
ARROW_ROW_THRESHOLD = 1000
```

Lowering this threshold favors Arrow for smaller datasets, which is beneficial if your downstream consumers (e.g., Pandas, Polars, frontend data grids) can consume Arrow IPC streams directly.

## Shared Memory Tuning

Vertebra uses a shared memory ring buffer for IPC between the Bun engine and Python workers.

-   **Slot Size:** Controls the maximum size of a single message.
-   **Ring Size:** Controls the number of messages in the buffer.

These can be configured via environment variables or the `BrokerConfig`.

```python
# app.py
from vertebra import Vertebra, BrokerConfig

app = Vertebra(
    broker=BrokerConfig(
        slot_size=131072,  # 128KB default
        slots=1024         # 1024 slots default
    )
)
```

For extremely high-throughput workloads sending massive payloads, consider increasing `slot_size`. Note that this increases total memory usage (`slot_size * slots`).

## IPC Protocol Optimization

Vertebra uses zero-parse-cost protocol headers and hybrid spin-wait for minimal latency.

### Hybrid Spin-Wait

The IPC layer uses a three-phase receive strategy to achieve sub-microsecond latency:

1. **Immediate Check**: Try non-blocking receive first (data may already be available)
2. **Spin Phase**: Spin checking ring buffer (iteration-based in Python, time-based in Rust)
3. **Park Phase**: Fall back to kernel-level blocking (futex on Linux)

This approach captures immediate responses without syscall overhead while still allowing efficient blocking for longer waits.
This is implemented for both SPSC (response channel) and MPMC (request channel) rings.

**Python Configuration (iteration-based):**

```bash
# Configure spin iterations for Python AsyncShmTransport
export VERTEBRA_SPIN_ITERS=50  # Default: 50 iterations (~5µs)

# Lower values = lower latency, higher CPU usage during wait
# Higher values = higher latency ceiling, lower CPU usage
```

Python uses iteration-based spin to avoid `time.perf_counter()` overhead (~100-300ns per call).
~50 iterations ≈ 5µs on typical hardware.

**Rust Configuration (time-based):**

The Rust layer (`recv_hybrid()`) accepts a `spin_us` parameter for time-based spin-wait.
This is suitable for low-level Rust code where timer overhead is negligible.

**When to adjust:**
- **Latency-critical (trading, gaming)**: Use 30-50 iterations (or 2-3µs in Rust)
- **Throughput-focused (batch processing)**: Use 0 to skip spinning entirely
- **Mixed workloads**: Default 50 iterations balances both

### Rust vs Python Receive Methods

| Method | Wait Mechanism | Use Case | Typical Latency |
|--------|---------------|----------|-----------------|
| `SpscRing::recv_hybrid()` | Futex | Pure sync, Rust code | ~0.8µs |
| `AsyncRing::recv_hybrid()` | Eventfd + poll | Python asyncio (add_reader) | ~1.6µs |
| `Channel.recv_fast()` | Hybrid spin + asyncio | High-level Python async | ~2.0µs |

The futex path is faster but doesn't integrate with asyncio's event loop. Use `AsyncRing` when you need `loop.add_reader()` compatibility.

### Protocol Headers

All IPC messages include a 16-byte header:

```
┌─────────────────────┬─────────────────────┐
│  Header (16 bytes)  │  Serialized Payload  │
└─────────────────────┴─────────────────────┘
```

Header fields (little-endian):
- `id` (u64): Correlation ID for request/response matching
- `flags` (u32): Message flags (PRIORITY, COMPRESSED, ERROR)
- `payload_len` (u32): Payload length in bytes

### Multiplexing

The IPC client supports concurrent in-flight requests via correlation ID tracking:

```python
from vertebra.resources.broker import BrokerClientConfig, AsyncBrokerClient

config = BrokerClientConfig(
    max_concurrent_requests=64,  # Default: 64 concurrent requests
    timeout_ms=5000,
)
client = AsyncBrokerClient(config)
```

### Performance Targets

| Metric | Target | Achieved | Notes |
|--------|--------|----------|-------|
| Sync p99 (immediate) | <10µs | **1.15µs** | Data already in ring buffer |
| Sync p99 (blocked) | <15µs | **0.8µs** | Futex wakeup latency |
| Async p99 | <20µs | **2.0µs** | With asyncio integration |
| Multiplexed (64x) | <20µs | **1.7µs** | Concurrent correlation tracking |
| Throughput | >1M/sec | ✓ | Requires batching for full throughput |

### Running Benchmarks

```bash
# Python IPC fast path benchmark
uv run python benchmarks/ipc_fast_path.py

# Rust benchmarks (recommended for accurate latency measurement)
cd src/rs && cargo bench --package vertebra-core --bench spsc

# Key Rust benchmarks:
# - spsc_recv_hybrid_immediate: Data already available (~1.1µs)
# - spsc_recv_hybrid_pure_wakeup: True wakeup latency (~0.8µs)
# - spsc_recv_hybrid_wakeup: With thread spawn overhead (~100µs)
```

### Understanding Benchmark Results

The "wakeup" benchmarks may show higher latencies (~100µs) due to thread spawn/join overhead per iteration. This is **not** actual IPC latency. The `pure_wakeup` benchmark uses a persistent sender thread to measure true IPC wakeup latency (~0.8µs).

In production, worker threads are persistent, so the `pure_wakeup` benchmark reflects real-world performance.
