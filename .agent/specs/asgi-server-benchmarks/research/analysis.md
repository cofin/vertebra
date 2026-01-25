# ASGI Server Benchmarks - Analysis

## Sequential Thinking Analysis

### Thought 1: Understanding the Landscape

The ASGI/WSGI server benchmarking space is well-established, with Granian providing a comprehensive reference implementation. The key servers to compare are:

1. **Granian** - Rust-based ASGI/RSGI/WSGI server, claims highest throughput
2. **Uvicorn** - De facto standard ASGI server, uses uvloop + httptools
3. **Gunicorn** - Classic WSGI server with various worker types
4. **Hypercorn** - ASGI server with HTTP/2 and HTTP/3 support
5. **Daphne** - Django Channels' reference ASGI server
6. **Socketify** - High-performance alternative with uSockets
7. **Vertebra** - Our hybrid Bun+Python architecture with shared-memory IPC

### Thought 2: Granian's Benchmarking Approach

Granian uses a sophisticated benchmarking system:

- **Tool**: `oha` - Rust-based HTTP load generator (faster than `wrk` or `hey`)
- **Methodology**: Primer (4s) → Warmup (3s) → Measurement (10s)
- **Metrics**: Total requests, RPS, avg/p99/max latency
- **Test Types**: GET (10B-100KB), Echo (body streaming), Files (pathsend), I/O waits, WebSockets
- **Concurrency**: Multiple levels (c64, c128, c512)
- **All async servers use uvloop** for fair comparison

### Thought 3: Key Differentiators for Vertebra

Vertebra's architecture is fundamentally different:

1. **Bun as HTTP layer** - Handles raw HTTP parsing/serialization
2. **Shared-memory IPC** - Ring buffers for Python worker communication
3. **Python for business logic** - ASGI app execution happens in Python
4. **No socket overhead** - Data plane uses shared memory only

This means we're testing a DIFFERENT pipeline than pure Python servers:
- Pure servers: HTTP → Python parsing → Python app → Python response
- Vertebra: HTTP → Bun parsing → SHM → Python app → SHM → Bun response

### Thought 4: What We Should Measure

Primary metrics:
1. **RPS (Requests Per Second)** - Raw throughput at various concurrency levels
2. **Latency Distribution** - p50, p95, p99, max
3. **Memory Footprint** - Per-worker base memory usage
4. **CPU Efficiency** - Throughput per CPU core

Secondary metrics:
5. **Scaling Efficiency** - How performance changes with worker count
6. **Response Size Impact** - Performance at 10B, 1KB, 10KB, 100KB
7. **I/O Wait Behavior** - How servers handle async database waits
8. **Static File Serving** - Zero-copy file response performance

### Thought 5: Test Application Design

Following Granian's pattern, we need standardized test apps:

```python
# app/asgi.py
async def app(scope, receive, send):
    if scope["path"] == "/b10":
        # 10 byte response
        body = b"0123456789"
    elif scope["path"] == "/b1k":
        # 1KB response
        body = b"x" * 1024
    # ... etc
```

Each server runs the SAME application code for fair comparison.

### Thought 6: Benchmark Runner Architecture

Components needed:

1. **Server Configurations** - How to start each server with equivalent settings
2. **Test Scenarios** - Which endpoints to hit, at what concurrency
3. **Result Collection** - Parsing oha output into structured data
4. **Report Generation** - Markdown tables, charts, comparison summaries
5. **Reproducibility** - Documented environment requirements

### Thought 7: Vertebra-Specific Considerations

For Vertebra benchmarks, we need to consider:

1. **Startup overhead** - Bun + Python worker spawn time
2. **IPC latency component** - Time spent in shared-memory transport
3. **Worker scaling** - How adding Python workers affects throughput
4. **Native path vs ASGI path** - Compiled handlers vs full ASGI

We should break down Vertebra's results to show where time is spent:
- Bun HTTP handling
- IPC transport (send + receive)
- Python ASGI processing
- Response serialization

### Thought 8: Tool Selection

**Primary load generator**: `oha` (Rust-based, faster than alternatives)

Alternatives considered:
- `wrk` - Lua scripting, but oha is faster
- `hey` - Go-based, simpler but slower than oha
- `ab` - Apache Bench, outdated
- `vegeta` - Good for rate limiting, but oha covers more

**Why oha**:
- Written in Rust (low overhead)
- Real-time TUI for debugging
- HTTP/2 support
- Latency percentiles built-in
- `--no-tui` for CI/automated runs

### Thought 9: Concurrency Levels

Based on Granian's approach:

| Level | Concurrency | Use Case |
|-------|-------------|----------|
| Low | c8 | Database-heavy apps |
| Medium | c64 | Typical web APIs |
| High | c256 | High-traffic public APIs |
| Extreme | c512+ | Stress testing |

Each server should be tested at multiple concurrency levels to show scaling characteristics.

### Thought 10: Output Format

Results should be stored as JSON for processing:

```json
{
  "environment": {
    "cpu": "AMD Ryzen 7 5700X",
    "os": "Linux 6.x",
    "python": "3.12",
    "oha_version": "1.4.0"
  },
  "results": {
    "uvicorn": {
      "hello/c64": {
        "total_requests": 500000,
        "rps": 50000,
        "latency_avg_ms": 1.2,
        "latency_p99_ms": 3.4,
        "latency_max_ms": 15.2
      }
    }
  }
}
```

Markdown reports generated from JSON for human consumption.

## Complexity Assessment

- **Classification**: Medium-High
- **Estimated Phases**: 3 (Infrastructure → Implementation → Reporting)
- **Risk Level**: Low (additive, no production code changes)
- **Breaking Changes**: None (benchmarks are standalone)

## Similar Features in Codebase

1. `benchmarks/ipc_fast_path.py` - IPC latency benchmarking (follow structure)
2. `benchmarks/py_serialization.py` - Serialization comparison (output format)
3. `benchmarks/native_litestar.py` - Simple server benchmark (expand on this)
4. Granian's `benchmarks/` - External reference implementation

## Patterns to Follow

- **Dataclass results**: Use `@dataclass` for structured benchmark results
- **Phased execution**: Warmup → Measurement → Cleanup
- **JSON output**: Machine-readable results for CI integration
- **CLI arguments**: argparse for flexible configuration
- **Make target**: `make bench-servers` for easy execution
