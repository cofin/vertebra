# ASGI Server Benchmarks - Research Plan

## Executive Summary

This document details the research conducted for implementing a comprehensive ASGI server benchmarking suite for Vertebra. The goal is to provide fair, reproducible comparisons between Vertebra and competing ASGI/WSGI servers: Uvicorn, Gunicorn, Hypercorn, Daphne, Socketify, and Granian.

## Research Sources

### Primary Sources

1. **Granian Benchmark Suite** - https://github.com/emmett-framework/granian/tree/master/benchmarks
   - Complete reference implementation with `benchmarks.py` orchestrator
   - Uses `oha` for load generation
   - Standardized test applications (ASGI, RSGI, WSGI)
   - JSON result format with markdown report generation

2. **oha Load Generator** - https://github.com/hatoo/oha
   - Rust-based HTTP load testing tool
   - Real-time TUI for interactive debugging
   - Supports HTTP/1.1, HTTP/2, experimental HTTP/3
   - Output formats: text, json, csv
   - Key flags: `-n` (requests), `-z` (duration), `-c` (concurrency), `-q` (rate limit)

3. **Industry Benchmarks**
   - TechEmpower Framework Benchmarks methodology
   - FastAPI's benchmarking approach
   - Python Application Servers in 2025 analysis

### Existing Vertebra Benchmarks

1. `benchmarks/ipc_fast_path.py` - IPC latency targeting sub-10us p99
2. `benchmarks/py_serialization.py` - MessagePack vs JSON vs Arrow
3. `benchmarks/native_litestar.py` - Simple Uvicorn RPS test
4. `benchmarks/ipc_performance.py` - General IPC throughput

## Granian Benchmark Deep Dive

### Methodology

Granian's benchmark suite follows a rigorous three-phase approach:

1. **Primer Phase** (4 seconds)
   - Concurrency: 8
   - Purpose: Initialize connections, warm JIT

2. **Warmup Phase** (3 seconds)
   - Concurrency: Target concurrency level
   - Purpose: Stabilize measurements

3. **Measurement Phase** (10 seconds)
   - Concurrency: Target concurrency level
   - Purpose: Collect actual metrics

### Test Categories

| Category | Description | Endpoints |
|----------|-------------|-----------|
| rsgi_body | RSGI response types | bytes/string at 10B-100KB |
| interfaces | ASGI vs RSGI vs WSGI | GET, Echo |
| http2 | HTTP/1 vs HTTP/2 | Performance comparison |
| files | File serving | pathsend, memory load |
| loops | Event loop comparison | asyncio, uvloop, rloop |
| vs_asgi | Granian vs Uvicorn vs Hypercorn | Full comparison |
| vs_wsgi | Granian vs Gunicorn vs uWSGI | Full comparison |
| ws | WebSocket | Broadcasting, echo |

### Server Configurations

**Granian**:
```bash
granian --interface asgi --workers 1 --threads 1 --http h1 app:app
```

**Uvicorn** (H11 - pure Python):
```bash
uvicorn --workers 1 --loop uvloop --http h11 app:app
```

**Uvicorn** (Httptools - Cython):
```bash
uvicorn --workers 1 --loop uvloop --http httptools app:app
```

**Hypercorn**:
```bash
hypercorn --workers 1 -k uvloop app:app
```

**Gunicorn** (gthread):
```bash
gunicorn --workers 1 --worker-class gthread --threads 4 app:app
```

**Gunicorn** (gevent):
```bash
gunicorn --workers 1 --worker-class gevent app:app
```

### Test Application Structure

The test application provides standardized endpoints:

```python
# Plaintext responses at various sizes
BODY_10 = b"0" * 10
BODY_1K = b"0" * 1024
BODY_10K = b"0" * 10240
BODY_100K = b"0" * 102400

# Response templates
RESPONSE_PLAIN_10 = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n"
    b"Content-Length: 10\r\n\r\n" + BODY_10
)

async def app(scope, receive, send):
    path = scope["path"]

    if path == "/b10":
        await send_response(send, BODY_10, "text/plain")
    elif path == "/b1k":
        await send_response(send, BODY_1K, "text/plain")
    elif path == "/echo":
        body = await receive_body(receive)
        await send_response(send, body, "text/plain")
    elif path == "/io10":
        await asyncio.sleep(0.010)  # Simulate 10ms DB query
        await send_response(send, b"ok", "text/plain")
    # ... etc
```

### Results Format

```json
{
  "cpu_count": 16,
  "timestamp": "2025-01-24T10:00:00Z",
  "python_version": "3.12.0",
  "granian_version": "2.6.0",
  "results": {
    "vs_asgi": {
      "granian_asgi": {
        "get_10kb_c64": {
          "requests": 521847,
          "rps": 52184.7,
          "latency_avg": 1.23,
          "latency_p99": 2.45,
          "latency_max": 8.91
        }
      }
    }
  }
}
```

## oha Tool Analysis

### Installation

```bash
# Via Cargo (recommended for latest version)
cargo install oha

# Via Homebrew (macOS)
brew install oha

# Via package manager (Arch)
pacman -S oha
```

### Key Command Line Options

| Flag | Description | Example |
|------|-------------|---------|
| `-n` | Number of requests | `-n 100000` |
| `-z` | Duration (overrides -n) | `-z 10s` |
| `-c` | Concurrent connections | `-c 64` |
| `-q` | Rate limit (QPS) | `-q 10000` |
| `-j` | JSON output | `-j` |
| `--no-tui` | Disable real-time display | `--no-tui` |
| `--latency-correction` | Avoid coordinated omission | `--latency-correction` |
| `-m` | HTTP method | `-m POST` |
| `-d` | Request body | `-d '{"key":"value"}'` |
| `-H` | Header | `-H "Content-Type: application/json"` |
| `-t` | Request timeout | `-t 10s` |

### Typical Usage Pattern

```bash
# Warmup (discarded)
oha -z 4s -c 8 --no-tui http://127.0.0.1:8000/hello > /dev/null

# Actual measurement
oha -z 10s -c 64 -j --no-tui http://127.0.0.1:8000/hello > results.json
```

### Output Parsing

```json
{
  "successRate": 100.0,
  "total": 521847,
  "slowest": 0.008912,
  "fastest": 0.000089,
  "average": 0.001234,
  "requestsPerSec": 52184.7,
  "latencyDistribution": {
    "10": 0.000512,
    "25": 0.000789,
    "50": 0.001123,
    "75": 0.001456,
    "90": 0.001890,
    "95": 0.002234,
    "99": 0.002456
  }
}
```

## Competitor Analysis

### Performance Rankings (2025 Benchmarks)

Based on multiple independent benchmarks:

| Server | RPS (Hello World) | Memory/Worker | HTTP/2 | HTTP/3 | Notes |
|--------|-------------------|---------------|--------|--------|-------|
| Socketify | 540,000+ | ~12MB | Yes | No | Fastest Python server, uses uSockets |
| Robyn | 500,000+ | ~15MB | Yes | No | Rust runtime (Actix-web + Tokio) |
| Granian | 50,000 | ~15MB | Yes | No | Rust HTTP parser + PyO3 |
| Uvicorn | 45,000 | ~20MB | No | No | De facto standard, uvloop + httptools |
| Hypercorn | 35,000 | ~25MB | Yes | Yes | HTTP/3 support |
| Daphne | 25,000 | ~30MB | Yes | No | Django Channels reference |
| Gunicorn | 10,000 | ~30MB | No | No | Classic WSGI, various workers |

### High-Performance Servers (Additional)

**Socketify** (https://github.com/cirospaciari/socketify.py):
- Claims 541,645 req/s vs Uvicorn's 337,265 in ASGI benchmarks
- Built on uSockets (C++ high-performance networking)
- Supports WSGI, ASGI, and native interface
- TechEmpower Top 1 in plaintext tests

**Robyn** (https://github.com/sparckles/Robyn):
- Python framework with Rust runtime
- Uses Actix-web + Tokio for async execution
- Achieved 1M req/s in TechEmpower live results
- PyO3 bridge between Python and Rust

**BlackSheep** (https://github.com/Neoteroi/BlackSheep):
- Fast ASGI framework (runs on Uvicorn/Granian/etc.)
- Cython-optimized with httptools
- Best performance with PyPy runtime
- Targets sub-1ms latency for REST APIs

**Spikard** (https://github.com/Goldziher/spikard):
- Rust-powered multi-language web toolkit
- Tower-HTTP runtime with zero-copy bindings
- 30-40% performance improvement via PyO3 optimization
- Code generation from OpenAPI/GraphQL/gRPC specs

### Architectural Differences

**Granian** (Rust):
- Rust HTTP parser + Python via PyO3
- Single interpreter, shared workers (threads)
- Free-threaded Python support since v2.0

**Uvicorn** (Python + C):
- httptools (libuv bindings) for HTTP parsing
- uvloop for async event loop
- Pure Python process model

**Hypercorn** (Python):
- Trio/asyncio support
- HTTP/2 multiplexing
- HTTP/3 (experimental)

**Vertebra** (Bun + Rust + Python):
- Bun handles HTTP (fastest JS runtime)
- Rust ring buffers for IPC
- Python for ASGI execution
- Zero socket overhead in data plane

## Vertebra Benchmark Design

### Unique Testing Requirements

1. **IPC Overhead Isolation**
   - Measure Bun HTTP layer alone
   - Measure IPC round-trip alone
   - Measure Python ASGI execution alone
   - Calculate sum vs actual end-to-end

2. **Scaling Characteristics**
   - Single Python worker
   - Multiple workers (2, 4, 8)
   - Worker-to-connection ratio effects

3. **Native vs ASGI Paths**
   - Compiled handlers (native fast path)
   - Full ASGI protocol (compatibility path)

### Proposed Test Endpoints

```
/hello          - Simple 12-byte JSON response
/b10            - 10-byte plaintext
/b1k            - 1KB plaintext
/b10k           - 10KB plaintext
/echo           - Echo POST body
/io10           - 10ms async sleep (DB simulation)
/io100          - 100ms async sleep (slow query simulation)
/file           - Static file serving (50KB image)
/json           - JSON serialization (1KB payload)
```

### Configuration Equivalence

To ensure fair comparison, all servers should run with:

- **Workers**: 1 (default) + multi-worker scaling tests
- **Threads**: 1 where applicable
- **Event Loop**: uvloop (where available)
- **HTTP Version**: HTTP/1.1 (primary), HTTP/2 (secondary)
- **Keep-Alive**: Enabled
- **Access Logging**: Disabled

### Vertebra-Specific Configuration

```python
# Start Vertebra with controlled settings
vertebra serve app:app \
    --workers 1 \
    --slots 1024 \
    --slot-size 65536 \
    --host 127.0.0.1 \
    --port 8000
```

## Implementation Architecture

### Directory Structure

```
benchmarks/
├── servers/                    # Server benchmark suite
│   ├── __init__.py
│   ├── runner.py              # Main orchestrator
│   ├── config.py              # Server configurations
│   ├── scenarios.py           # Test scenarios
│   ├── parsers.py             # oha output parsing
│   ├── reports.py             # Report generation
│   └── app/
│       ├── asgi.py            # Standard ASGI test app
│       └── wsgi.py            # Standard WSGI test app
├── results/                   # Benchmark output
│   ├── data.json
│   └── report.md
└── README.md                  # Usage documentation
```

### Orchestrator Design

```python
@dataclass
class ServerConfig:
    name: str
    command: list[str]
    interface: str  # asgi, wsgi
    startup_delay: float = 2.0
    warmup_delay: float = 4.0

@dataclass
class Scenario:
    name: str
    endpoint: str
    method: str = "GET"
    body: bytes | None = None
    concurrency_levels: list[int] = field(default_factory=lambda: [64, 256])
    duration_seconds: int = 10

@dataclass
class BenchmarkResult:
    server: str
    scenario: str
    concurrency: int
    total_requests: int
    rps: float
    latency_avg_ms: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_max_ms: float
    error_rate: float
```

### Execution Flow

```
1. Parse CLI arguments
2. Validate server installations
3. For each server in configuration:
   a. Start server process
   b. Wait for startup
   c. For each scenario:
      i.   Run primer (4s, c8)
      ii.  Run warmup (3s, target concurrency)
      iii. Run measurement (10s, target concurrency)
      iv.  Parse results
   d. Stop server process
4. Aggregate results
5. Generate reports
6. Output JSON + Markdown
```

### Report Format

```markdown
# ASGI Server Benchmark Results

**Date**: 2025-01-24
**CPU**: AMD Ryzen 7 5700X @ 4.5GHz (16 cores)
**OS**: Linux 6.12.58
**Python**: 3.12.0
**oha**: 1.4.0

## Hello World (12-byte JSON)

| Server | RPS (c64) | RPS (c256) | p99 (c64) | p99 (c256) |
|--------|-----------|------------|-----------|------------|
| Vertebra | 65,000 | 180,000 | 1.2ms | 2.8ms |
| Granian | 52,000 | 145,000 | 1.4ms | 3.2ms |
| Uvicorn | 48,000 | 130,000 | 1.6ms | 3.8ms |
| Hypercorn | 38,000 | 95,000 | 2.1ms | 5.2ms |

## 10KB Response

| Server | RPS (c64) | RPS (c256) | p99 (c64) | p99 (c256) |
|--------|-----------|------------|-----------|------------|
...
```

## Makefile Integration

```makefile
.PHONY: bench-servers
bench-servers:                               ## Run ASGI server benchmarks
    @echo "${INFO} Running server benchmarks..."
    @uv run python benchmarks/servers/runner.py
    @echo "${OK} Results saved to benchmarks/results/"

.PHONY: bench-servers-quick
bench-servers-quick:                         ## Quick benchmark (reduced duration)
    @uv run python benchmarks/servers/runner.py --quick

.PHONY: bench-vertebra-only
bench-vertebra-only:                         ## Benchmark only Vertebra
    @uv run python benchmarks/servers/runner.py --servers vertebra
```

## Risk Mitigation

### Reproducibility Concerns

1. **Environment variance**: Document exact CPU, OS, Python version
2. **Background processes**: Recommend dedicated benchmark machine
3. **Thermal throttling**: Allow cool-down between servers
4. **Network stack**: Use localhost only, disable firewall

### Fairness Concerns

1. **uvloop availability**: All async servers use uvloop when possible
2. **Worker count**: Single worker for apples-to-apples
3. **Logging disabled**: All servers run in production-like mode
4. **Same test app**: Identical application code for all ASGI servers

### Installation Complexity

Required installations:
- Python 3.12+
- oha (cargo install or brew)
- uvicorn, gunicorn, hypercorn, daphne, granian (pip)
- Bun (for Vertebra)

Optional:
- socketify (if available)
- Docker (for isolated environments)

## Detailed Scenarios

### Scenario 1: Hello World (Minimal Overhead)

**Purpose**: Measure raw request handling performance with minimal application logic.

**Configuration**:
- Endpoint: `/hello`
- Response: `{"hello": "world"}` (12 bytes)
- Method: GET
- Concurrency: 8, 64, 256, 512
- Duration: 10 seconds each

**What it measures**:
- HTTP parsing speed
- Routing overhead
- Response serialization
- Connection management

### Scenario 2: Response Size Scaling

**Purpose**: Understand how response size affects throughput.

**Configuration**:
- Endpoints: `/b10`, `/b1k`, `/b10k`, `/b100k`
- Response: Plaintext at various sizes
- Method: GET
- Concurrency: 64, 256
- Duration: 10 seconds each

**What it measures**:
- Buffer management efficiency
- Zero-copy capabilities
- I/O bandwidth utilization
- Memory allocation patterns

### Scenario 3: Request Body Handling (Echo)

**Purpose**: Measure bidirectional data handling.

**Configuration**:
- Endpoint: `/echo`
- Request body: 1KB, 10KB plaintext
- Method: POST
- Concurrency: 64, 256
- Duration: 10 seconds each

**What it measures**:
- Request body parsing
- Streaming vs buffering
- Response generation from input

### Scenario 4: I/O Wait Simulation

**Purpose**: Understand behavior under realistic database-bound workloads.

**Configuration**:
- Endpoints: `/io10`, `/io100`
- Response: Simple acknowledgment
- Method: GET
- Concurrency: 256, 512
- Duration: 10 seconds each

**What it measures**:
- Connection pool efficiency
- Event loop scheduling
- Concurrency handling under I/O pressure

### Scenario 5: Static File Serving

**Purpose**: Measure static file handling and potential zero-copy optimizations.

**Configuration**:
- Endpoint: `/file`
- Response: 50KB JPEG image
- Method: GET
- Concurrency: 64, 256
- Duration: 10 seconds each

**What it measures**:
- sendfile() usage
- Memory mapping
- Disk I/O handling

### Scenario 6: JSON Serialization

**Purpose**: Measure realistic API response generation.

**Configuration**:
- Endpoint: `/json`
- Response: 1KB JSON with nested structures
- Method: GET
- Concurrency: 64, 256
- Duration: 10 seconds each

**What it measures**:
- JSON encoder performance
- Object creation overhead
- Real-world API patterns

## Hardware Recommendations

For reliable, reproducible benchmarks:

### Minimum Requirements
- CPU: 4+ cores, 2.5GHz+
- RAM: 8GB+
- Storage: SSD (for file serving tests)
- Network: Not needed (localhost only)

### Recommended Setup
- CPU: 8+ cores, high clock speed
- RAM: 16GB+
- Isolated environment (no other workloads)
- CPU frequency governor set to "performance"
- Background services minimized

### Cloud Instance Recommendations
- AWS: c7i.2xlarge or equivalent
- GCP: c2-standard-8
- Azure: F8s_v2

### Environment Preparation

```bash
# Set CPU governor to performance (Linux)
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Disable transparent huge pages
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled

# Increase file descriptor limits
ulimit -n 65536

# Disable swap (to avoid GC-induced swapping)
sudo swapoff -a
```

## Error Handling and Edge Cases

### Server Startup Failures

```python
async def start_server(config: ServerConfig) -> subprocess.Popen:
    process = subprocess.Popen(config.command, ...)

    # Wait for server to be ready
    for _ in range(30):  # 30 second timeout
        try:
            async with httpx.AsyncClient() as client:
                await client.get(f"http://127.0.0.1:{config.port}/health")
                return process
        except:
            await asyncio.sleep(1)

    process.kill()
    raise RuntimeError(f"Server {config.name} failed to start")
```

### Result Validation

Results should be validated for:
1. Error rate below threshold (< 0.1%)
2. RPS above minimum viable (> 100)
3. Latency values are reasonable (< 60s)
4. Total requests match duration * approximate RPS

### Graceful Degradation

If a server fails:
1. Log the failure
2. Skip remaining scenarios for that server
3. Continue with other servers
4. Mark server as "incomplete" in results

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Server Benchmarks

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly on Sunday
  workflow_dispatch:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install oha
        run: cargo install oha

      - name: Install servers
        run: |
          pip install uvicorn gunicorn hypercorn daphne granian

      - name: Run benchmarks
        run: make bench-servers

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: benchmarks/results/
```

### Historical Tracking

Results should be stored with timestamps to track performance over time:

```
benchmarks/results/
├── 2025-01-24/
│   ├── data.json
│   └── report.md
├── 2025-01-17/
│   ├── data.json
│   └── report.md
└── latest -> 2025-01-24/
```

## Conclusion

This research provides a comprehensive foundation for implementing the ASGI server benchmark suite. Key decisions:

1. **Use oha** as the primary load generator (faster than alternatives)
2. **Follow Granian's methodology** for scientific reproducibility
3. **Standardized test apps** ensure fair comparison
4. **JSON output** enables automated CI integration
5. **Markdown reports** for human-readable results
6. **Vertebra-specific breakdown** to understand performance characteristics

The implementation should prioritize:
1. Reproducibility (exact environment documentation)
2. Fairness (equivalent configurations)
3. Comprehensiveness (multiple response sizes, concurrency levels)
4. Automation (CI-friendly output format)

## References

1. Granian Benchmarks: https://github.com/emmett-framework/granian/tree/master/benchmarks
2. oha Load Generator: https://github.com/hatoo/oha
3. Uvicorn Documentation: https://www.uvicorn.org/
4. Hypercorn Documentation: https://hypercorn.readthedocs.io/
5. ASGI Specification: https://asgi.readthedocs.io/
6. TechEmpower Benchmarks Methodology: https://www.techempower.com/benchmarks/
7. Python Application Servers 2025: https://www.deployhq.com/blog/python-application-servers-in-2025
