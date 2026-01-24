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

Vertebra uses zero-parse-cost protocol headers for minimal latency.

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

| Metric | Target | Notes |
|--------|--------|-------|
| p99 latency | <10µs | Protocol layer achieves this |
| Throughput | >1M/sec | Requires batching for full throughput |
| Hot path allocations | Zero | Pre-allocated buffers |

### Running Benchmarks

```bash
# Python benchmark
uv run python benchmarks/ipc_performance.py --iterations 10000

# TypeScript benchmark
bun run benchmarks/ipc_performance.ts
```
