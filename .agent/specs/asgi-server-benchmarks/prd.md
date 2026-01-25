# PRD: ASGI Server Benchmarking Suite

## Intelligence Context

### Complexity Assessment

- **Classification**: Medium
- **Estimated Phases**: 3 (Infrastructure → Benchmarks → Reporting)
- **Risk Level**: Low (additive feature, no production code changes)
- **Breaking Changes**: None (standalone benchmarking tooling)

### Similar Features Referenced

1. `benchmarks/ipc_fast_path.py` - IPC latency benchmarking with phased targets
2. `benchmarks/py_serialization.py` - Serialization codec comparison
3. `benchmarks/native_litestar.py` - Simple Uvicorn RPS measurement
4. `benchmarks/ipc_performance.py` - General throughput testing
5. Granian's `benchmarks/` - External reference implementation (highly influential)
6. Socketify benchmarks - Claims fastest Python web framework
7. Robyn benchmarks - Rust runtime achieving 1M req/s

### Patterns to Follow

- **Dataclass Results**: Use `@dataclass` for structured `BenchmarkResult` objects
- **Phased Execution**: Primer → Warmup → Measurement cycle (from Granian)
- **JSON Output**: Machine-readable results for CI/CD integration
- **CLI Arguments**: `argparse` for flexible configuration
- **Make Target**: `make bench-servers` for easy execution
- **Print Summary**: Consistent result formatting with targets

### Detected Stack and Commands

- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **Build Tool**: `oha` (Rust HTTP load generator)
- **Test Commands**: `make test`, `make bench-servers` (to be added)
- **Lint Command**: `make lint-all`
- **Install Command**: `make install`

---

## Problem Statement

### User Problem Being Solved

Vertebra claims to be a high-performance Python application backbone, but there is no comprehensive, reproducible way to verify these claims against industry-standard ASGI servers. Developers evaluating Vertebra need objective performance data to make informed decisions about adopting the framework.

Currently, the only server benchmark (`native_litestar.py`) is rudimentary—it benchmarks only Uvicorn using a simple httpx client loop, which introduces significant client-side overhead and doesn't represent realistic load patterns. There is no comparison with Granian (the current fastest ASGI server), Hypercorn (HTTP/2 leader), Daphne (Django reference), or Socketify (alternative high-performance option).

Without rigorous benchmarks, Vertebra's marketing claims are unverifiable, and the team lacks data to prioritize performance optimizations. The IPC performance benchmarks (`ipc_fast_path.py`) are excellent for internal transport optimization but don't measure end-to-end HTTP request handling.

### Business Value

1. **Marketing Credibility**: Objective third-party-style benchmarks establish trust with potential users
2. **Performance Regression Detection**: CI-integrated benchmarks catch performance regressions before release
3. **Optimization Guidance**: Breakdown of where time is spent guides engineering investment
4. **Competitive Positioning**: Clear data for "Vertebra vs. X" comparisons
5. **Community Engagement**: Open benchmark methodology invites community validation

### Success Criteria

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Category 1 (Servers) | 6 ASGI servers compared | Benchmark suite completeness |
| Category 2 (Runtimes) | 4 integrated runtimes compared | Benchmark suite completeness |
| Category 3 (Frameworks) | 5 frameworks compared | Benchmark suite completeness |
| Scenario coverage | 6+ scenarios per category | Test endpoint count |
| Reproducibility | <5% variance between runs | Statistical analysis |
| Documentation | Complete methodology docs | Documentation review |
| CI Integration | Automated weekly runs | GitHub Actions workflow |
| Report quality | Markdown + JSON output | Report generation |

---

## Benchmark Categories

The Python web ecosystem operates at three distinct layers that must be benchmarked separately for meaningful comparisons:

### Category 1: Raw ASGI Server Performance
**Purpose**: Isolate HTTP parsing, connection handling, and event loop efficiency.
**Method**: Same minimal ASGI application on different servers.

| Server | Type | Notes |
|--------|------|-------|
| Uvicorn (h11) | Pure Python | Baseline, most compatible |
| Uvicorn (httptools) | Cython | ~30% faster HTTP parsing |
| Granian | Rust + PyO3 | Single interpreter, threaded |
| Hypercorn | Python | HTTP/2, HTTP/3 capable |
| Daphne | Python | Django Channels reference |
| Socketify | C++ (uSockets) | Claims fastest, custom event loop |

### Category 2: Integrated Runtime Performance
**Purpose**: Measure full-stack performance where server and framework are inseparable.
**Method**: Each runtime runs its native application format.

| Runtime | Architecture | Notes |
|---------|--------------|-------|
| Vertebra | Bun + Rust IPC + Python | Shared-memory transport |
| Robyn | Rust (Actix + Tokio) | Native interface, not ASGI |
| Sanic (standalone) | Python + uvloop | Built-in optimized server |
| Spikard | Rust (Tower-HTTP) | Zero-copy PyO3 bindings |

### Category 3: Framework Overhead Comparison
**Purpose**: Isolate framework routing, middleware, and serialization overhead.
**Method**: Same server (Uvicorn httptools), different frameworks.

| Framework | Overhead Source | Notes |
|-----------|-----------------|-------|
| Raw ASGI | None (baseline) | Pure ASGI callable |
| Starlette | Routing, middleware | Foundation for FastAPI |
| FastAPI | Pydantic validation | ~30-50% overhead vs Starlette |
| BlackSheep | Cython routing | Optimized for speed |
| Litestar | Msgspec validation | Vertebra's framework |
| Sanic (on Uvicorn) | Full framework | Compare vs standalone |

---

## Acceptance Criteria

### Functional Requirements

#### FR-1: Test Applications for Each Category

**Category 1 - Raw ASGI App** (for server comparison):
- [ ] Create `benchmarks/servers/app/asgi.py` with standardized endpoints
- [ ] `/hello` - JSON response `{"hello": "world"}` (12 bytes)
- [ ] `/b10`, `/b1k`, `/b10k`, `/b100k` - Plaintext at various sizes
- [ ] `/echo` - Echo POST body back
- [ ] `/io10`, `/io100` - Async sleep for I/O simulation
- [ ] `/file` - Serve static 50KB file
- [ ] `/json` - Serialize and return 1KB JSON structure

**Category 2 - Native Runtime Apps** (each runtime's native format):
- [ ] Create `benchmarks/servers/app/robyn_app.py` for Robyn native interface
- [ ] Create `benchmarks/servers/app/sanic_app.py` for Sanic standalone
- [ ] Create `benchmarks/servers/app/spikard_app.py` for Spikard
- [ ] Vertebra uses same ASGI app via `vertebra serve`

**Category 3 - Framework Apps** (all run on Uvicorn httptools):
- [ ] Create `benchmarks/servers/app/starlette_app.py` for Starlette
- [ ] Create `benchmarks/servers/app/fastapi_app.py` for FastAPI
- [ ] Create `benchmarks/servers/app/blacksheep_app.py` for BlackSheep
- [ ] Create `benchmarks/servers/app/litestar_app.py` for Litestar
- [ ] Create `benchmarks/servers/app/sanic_asgi_app.py` for Sanic on ASGI

#### FR-2: Server and Runtime Configuration System

- [ ] Create `benchmarks/servers/config.py` with categorized definitions
- [ ] **Category 1 - ASGI Servers**: Uvicorn (h11), Uvicorn (httptools), Granian, Hypercorn, Daphne, Socketify
- [ ] **Category 2 - Integrated Runtimes**: Vertebra, Robyn, Sanic (standalone), Spikard
- [ ] **Category 3 - Frameworks**: Raw ASGI, Starlette, FastAPI, BlackSheep, Litestar, Sanic (on Uvicorn)
- [ ] Each entry has: name, category, command, port, startup delay, interface type
- [ ] Configurable worker counts (default: 1 for fair comparison)
- [ ] uvloop enabled for all async servers where supported

#### FR-3: Benchmark Orchestrator

- [ ] Create `benchmarks/servers/runner.py` as main entry point
- [ ] Server lifecycle management (start → benchmark → stop)
- [ ] Three-phase execution: Primer (4s, c8) → Warmup (3s) → Measurement (10s)
- [ ] Configurable concurrency levels (default: 8, 64, 256, 512)
- [ ] Graceful handling of server startup failures
- [ ] Progress output during long-running benchmarks

#### FR-4: oha Integration

- [ ] Create `benchmarks/servers/parsers.py` for oha output parsing
- [ ] Use oha with `--no-tui -j` for JSON output
- [ ] Parse: total requests, RPS, latency distribution (avg, p50, p95, p99, max)
- [ ] Error rate tracking (success rate < 99.9% flags warning)
- [ ] Latency correction enabled (`--latency-correction`)

#### FR-5: Result Collection and Storage

- [ ] Create `benchmarks/servers/results.py` for result dataclasses
- [ ] `BenchmarkResult` dataclass with all metrics
- [ ] `BenchmarkSuite` container for all results
- [ ] JSON serialization to `benchmarks/results/data.json`
- [ ] Results include environment metadata (CPU, OS, Python version, oha version)
- [ ] Timestamped results for historical tracking

#### FR-6: Report Generation

- [ ] Create `benchmarks/servers/reports.py` for markdown generation
- [ ] Generate `benchmarks/results/report.md` with formatted tables
- [ ] Comparison tables by scenario (RPS, latency)
- [ ] Summary section with rankings per scenario
- [ ] Environment information section
- [ ] Charts (optional, via ASCII or external tool)

#### FR-7: Vertebra-Specific Metrics

- [ ] IPC latency breakdown (Bun HTTP + Transport + Python ASGI)
- [ ] Native path vs ASGI path comparison (if applicable)
- [ ] Worker scaling efficiency (1, 2, 4, 8 workers)
- [ ] Memory per worker tracking
- [ ] Integration with existing IPC benchmarks

#### FR-8: CLI Interface

- [ ] `--servers` - Select specific servers to benchmark
- [ ] `--scenarios` - Select specific scenarios to run
- [ ] `--quick` - Reduced duration mode for fast iteration
- [ ] `--output-dir` - Custom output directory
- [ ] `--concurrency` - Custom concurrency levels
- [ ] `--duration` - Custom measurement duration
- [ ] `--workers` - Worker count for scaling tests
- [ ] `--verbose` - Detailed progress output

### Non-Functional Requirements

#### NFR-1: Performance

- [ ] Benchmark runner overhead < 1% of measured results
- [ ] Startup detection via health check (max 30s timeout)
- [ ] Cool-down period between servers (5s default)

#### NFR-2: Reliability

- [ ] Graceful handling of missing server installations
- [ ] Partial results saved if benchmark interrupted
- [ ] Server process cleanup on exit (even on crash)
- [ ] Validation of result sanity (RPS > 0, error rate < 10%)

#### NFR-3: Documentation

- [ ] README in `benchmarks/servers/` with usage instructions
- [ ] Environment setup documentation
- [ ] Methodology explanation for transparency
- [ ] Troubleshooting guide for common issues

#### NFR-4: CI Integration

- [ ] `make bench-servers` target in Makefile
- [ ] Optional GitHub Actions workflow for scheduled runs
- [ ] Artifact storage for historical comparison
- [ ] Alert on significant performance regression

---

## Technical Approach

### Architecture Overview

```
benchmarks/servers/
├── __init__.py
├── runner.py              # Main orchestrator
├── config.py              # Server configurations
├── scenarios.py           # Test scenario definitions
├── parsers.py             # oha output parsing
├── results.py             # Result dataclasses
├── reports.py             # Report generation
├── utils.py               # Utilities (server health, process mgmt)
├── app/
│   ├── asgi.py           # Standard ASGI test application
│   ├── wsgi.py           # Standard WSGI test application (optional)
│   └── assets/
│       └── image.jpg     # 50KB test file
└── README.md             # Documentation

benchmarks/results/
├── data.json             # Machine-readable results
├── report.md             # Human-readable report
└── history/              # Historical results (optional)
```

### Pattern References from Codebase

**Dataclass Results Pattern (ipc_fast_path.py:54-108)**:
```python
@dataclass
class FastPathResult:
    name: str
    phase: FastPathPhase
    iterations: int
    total_time_s: float
    latencies_us: list[float]
    target_us: float
    allocations_per_msg: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput(self) -> float:
        return self.iterations / self.total_time_s if self.total_time_s > 0 else 0

    @property
    def p99_us(self) -> float:
        # ... percentile calculation
```

Follow this pattern for `BenchmarkResult`.

**Print Summary Pattern (ipc_fast_path.py:110-137)**:
```python
def print_summary(self) -> None:
    status = "PASS" if self.passes_target else "FAIL"
    print(f"\n{'=' * 70}")
    print(f"[{status}] {self.name} (Phase {self.phase.value}: {self.phase.name})")
    # ... formatted output
```

**Benchmark Runner Pattern (native_litestar.py:26-32)**:
```python
async def benchmark_endpoint(url: str, count: int = 10000):
    async with AsyncClient() as client:
        start = time.time()
        for _ in range(count):
            await client.get(url)
        end = time.time()
    return count / (end - start)
```

Replace with oha subprocess call for professional benchmarking.

### Server Configurations

```python
# config.py
from dataclasses import dataclass

@dataclass
class ServerConfig:
    name: str
    display_name: str
    command: list[str]
    port: int
    interface: str  # "asgi" or "wsgi"
    startup_delay: float = 2.0
    requires_install: str | None = None  # pip package name

SERVERS = {
    "vertebra": ServerConfig(
        name="vertebra",
        display_name="Vertebra",
        command=["vertebra", "serve", "benchmarks.servers.app.asgi:app",
                 "--port", "8001", "--workers", "1"],
        port=8001,
        interface="asgi",
    ),
    "uvicorn_h11": ServerConfig(
        name="uvicorn_h11",
        display_name="Uvicorn (h11)",
        command=["uvicorn", "--host", "127.0.0.1", "--port", "8002",
                 "--workers", "1", "--loop", "uvloop", "--http", "h11",
                 "--no-access-log", "benchmarks.servers.app.asgi:app"],
        port=8002,
        interface="asgi",
        requires_install="uvicorn[standard]",
    ),
    "uvicorn_httptools": ServerConfig(
        name="uvicorn_httptools",
        display_name="Uvicorn (httptools)",
        command=["uvicorn", "--host", "127.0.0.1", "--port", "8003",
                 "--workers", "1", "--loop", "uvloop", "--http", "httptools",
                 "--no-access-log", "benchmarks.servers.app.asgi:app"],
        port=8003,
        interface="asgi",
        requires_install="uvicorn[standard]",
    ),
    "granian": ServerConfig(
        name="granian",
        display_name="Granian",
        command=["granian", "--interface", "asgi", "--host", "127.0.0.1",
                 "--port", "8004", "--workers", "1", "--threads", "1",
                 "--http", "1", "--no-ws", "benchmarks.servers.app.asgi:app"],
        port=8004,
        interface="asgi",
        requires_install="granian",
    ),
    "hypercorn": ServerConfig(
        name="hypercorn",
        display_name="Hypercorn",
        command=["hypercorn", "--bind", "127.0.0.1:8005", "--workers", "1",
                 "-k", "uvloop", "benchmarks.servers.app.asgi:app"],
        port=8005,
        interface="asgi",
        requires_install="hypercorn[uvloop]",
    ),
    "daphne": ServerConfig(
        name="daphne",
        display_name="Daphne",
        command=["daphne", "-b", "127.0.0.1", "-p", "8006",
                 "benchmarks.servers.app.asgi:app"],
        port=8006,
        interface="asgi",
        requires_install="daphne",
    ),
    "socketify": ServerConfig(
        name="socketify",
        display_name="Socketify",
        command=["python", "-c",
                 "from socketify import ASGI; "
                 "from benchmarks.servers.app.asgi import app; "
                 "ASGI(app).listen(8007).run()"],
        port=8007,
        interface="asgi",
        requires_install="socketify",
    ),
    "robyn": ServerConfig(
        name="robyn",
        display_name="Robyn",
        command=["python", "-m", "robyn", "benchmarks.servers.app.robyn_app:app",
                 "--host", "127.0.0.1", "--port", "8008", "--workers", "1"],
        port=8008,
        interface="native",  # Robyn has its own interface
        requires_install="robyn",
    ),
    "blacksheep": ServerConfig(
        name="blacksheep",
        display_name="BlackSheep (Uvicorn)",
        command=["uvicorn", "--host", "127.0.0.1", "--port", "8009",
                 "--workers", "1", "--loop", "uvloop", "--http", "httptools",
                 "--no-access-log", "benchmarks.servers.app.blacksheep_app:app"],
        port=8009,
        interface="asgi",
        requires_install="blacksheep uvicorn[standard]",
    ),
    "spikard": ServerConfig(
        name="spikard",
        display_name="Spikard",
        command=["python", "-m", "spikard", "serve",
                 "benchmarks.servers.app.spikard_app:app",
                 "--host", "127.0.0.1", "--port", "8010"],
        port=8010,
        interface="native",  # Spikard has Tower-HTTP runtime
        requires_install="spikard",
    ),
}
```

### Test Application

```python
# app/asgi.py
import asyncio
import os

# Pre-computed responses for minimal overhead
HELLO_BODY = b'{"hello":"world"}'
BODY_10 = b"0" * 10
BODY_1K = b"0" * 1024
BODY_10K = b"0" * 10240
BODY_100K = b"0" * 102400

# JSON structure (1KB)
JSON_RESPONSE = b'{"users":[' + b','.join([
    b'{"id":%d,"name":"User %d","email":"user%d@example.com","active":true}'
    % (i, i, i) for i in range(10)
]) + b']}'

# Load file for static serving
FILE_PATH = os.path.join(os.path.dirname(__file__), "assets", "image.jpg")
try:
    with open(FILE_PATH, "rb") as f:
        FILE_CONTENT = f.read()
except FileNotFoundError:
    FILE_CONTENT = b"0" * 51200  # 50KB placeholder

async def send_response(send, body, content_type):
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", content_type.encode()],
            [b"content-length", str(len(body)).encode()],
        ],
    })
    await send({
        "type": "http.response.body",
        "body": body,
    })

async def receive_body(receive):
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break
    return body

async def app(scope, receive, send):
    if scope["type"] != "http":
        return

    path = scope["path"]

    if path == "/hello":
        await send_response(send, HELLO_BODY, "application/json")
    elif path == "/b10":
        await send_response(send, BODY_10, "text/plain")
    elif path == "/b1k":
        await send_response(send, BODY_1K, "text/plain")
    elif path == "/b10k":
        await send_response(send, BODY_10K, "text/plain")
    elif path == "/b100k":
        await send_response(send, BODY_100K, "text/plain")
    elif path == "/echo":
        body = await receive_body(receive)
        await send_response(send, body, "text/plain")
    elif path == "/io10":
        await asyncio.sleep(0.010)
        await send_response(send, b"ok", "text/plain")
    elif path == "/io100":
        await asyncio.sleep(0.100)
        await send_response(send, b"ok", "text/plain")
    elif path == "/file":
        await send_response(send, FILE_CONTENT, "image/jpeg")
    elif path == "/json":
        await send_response(send, JSON_RESPONSE, "application/json")
    elif path == "/health":
        await send_response(send, b"ok", "text/plain")
    else:
        await send({
            "type": "http.response.start",
            "status": 404,
            "headers": [[b"content-type", b"text/plain"]],
        })
        await send({"type": "http.response.body", "body": b"Not Found"})
```

### oha Integration

```python
# parsers.py
import json
import subprocess
from dataclasses import dataclass

@dataclass
class OhaResult:
    total_requests: int
    rps: float
    success_rate: float
    latency_avg_ms: float
    latency_p50_ms: float
    latency_p75_ms: float
    latency_p90_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_max_ms: float

def run_oha(
    url: str,
    duration_seconds: int = 10,
    concurrency: int = 64,
    method: str = "GET",
    body: bytes | None = None,
) -> OhaResult:
    """Run oha and parse results."""
    cmd = [
        "oha",
        "-z", f"{duration_seconds}s",
        "-c", str(concurrency),
        "-j",
        "--no-tui",
        "--latency-correction",
        "-m", method,
    ]

    if body is not None:
        cmd.extend(["-d", body.decode()])

    cmd.append(url)

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)

    # Convert seconds to milliseconds for latencies
    return OhaResult(
        total_requests=data["total"],
        rps=data["requestsPerSec"],
        success_rate=data["successRate"],
        latency_avg_ms=data["average"] * 1000,
        latency_p50_ms=data["latencyDistribution"]["50"] * 1000,
        latency_p75_ms=data["latencyDistribution"]["75"] * 1000,
        latency_p90_ms=data["latencyDistribution"]["90"] * 1000,
        latency_p95_ms=data["latencyDistribution"]["95"] * 1000,
        latency_p99_ms=data["latencyDistribution"]["99"] * 1000,
        latency_max_ms=data["slowest"] * 1000,
    )
```

### Report Generation

```python
# reports.py
from datetime import datetime
from results import BenchmarkSuite

def generate_markdown_report(suite: BenchmarkSuite) -> str:
    lines = [
        "# ASGI Server Benchmark Results",
        "",
        f"**Date**: {suite.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
        f"**CPU**: {suite.environment.cpu}",
        f"**OS**: {suite.environment.os}",
        f"**Python**: {suite.environment.python_version}",
        f"**oha**: {suite.environment.oha_version}",
        "",
    ]

    # Group results by scenario
    scenarios = {}
    for result in suite.results:
        key = result.scenario
        if key not in scenarios:
            scenarios[key] = []
        scenarios[key].append(result)

    for scenario_name, results in scenarios.items():
        lines.append(f"## {scenario_name}")
        lines.append("")
        lines.append("| Server | Concurrency | RPS | Avg (ms) | p99 (ms) | Max (ms) |")
        lines.append("|--------|-------------|-----|----------|----------|----------|")

        for r in sorted(results, key=lambda x: -x.rps):
            lines.append(
                f"| {r.server} | c{r.concurrency} | {r.rps:,.0f} | "
                f"{r.latency_avg_ms:.2f} | {r.latency_p99_ms:.2f} | {r.latency_max_ms:.2f} |"
            )
        lines.append("")

    return "\n".join(lines)
```

### Makefile Integration

```makefile
.PHONY: bench-servers
bench-servers:                               ## Run ASGI server benchmarks
	@echo "${INFO} Running ASGI server benchmarks..."
	@command -v oha >/dev/null 2>&1 || { echo "${ERROR} oha not found. Install with: cargo install oha"; exit 1; }
	@uv run python benchmarks/servers/runner.py
	@echo "${OK} Results saved to benchmarks/results/"

.PHONY: bench-servers-quick
bench-servers-quick:                         ## Quick server benchmark (reduced duration)
	@echo "${INFO} Running quick server benchmarks..."
	@uv run python benchmarks/servers/runner.py --quick --duration 3

.PHONY: bench-vertebra
bench-vertebra:                              ## Benchmark only Vertebra
	@echo "${INFO} Running Vertebra benchmarks..."
	@uv run python benchmarks/servers/runner.py --servers vertebra

.PHONY: bench-install-deps
bench-install-deps:                          ## Install benchmark dependencies
	@echo "${INFO} Installing benchmark dependencies..."
	@command -v oha >/dev/null 2>&1 || cargo install oha
	@uv pip install uvicorn[standard] granian hypercorn[uvloop] daphne
	@echo "${OK} Benchmark dependencies installed"
```

### File Structure Plan

**New Files**:
- `benchmarks/servers/__init__.py` - Package init
- `benchmarks/servers/runner.py` - Main orchestrator
- `benchmarks/servers/config.py` - Server configurations
- `benchmarks/servers/scenarios.py` - Test scenario definitions
- `benchmarks/servers/parsers.py` - oha output parsing
- `benchmarks/servers/results.py` - Result dataclasses
- `benchmarks/servers/reports.py` - Report generation
- `benchmarks/servers/utils.py` - Utilities
- `benchmarks/servers/app/asgi.py` - Standard ASGI app
- `benchmarks/servers/app/assets/image.jpg` - Test file (50KB)
- `benchmarks/servers/README.md` - Documentation
- `benchmarks/results/.gitkeep` - Results directory

**Modified Files**:
- `Makefile` - Add `bench-servers` targets
- `pyproject.toml` - Add benchmark dependencies (optional extras)

### Documentation Updates Required

1. **benchmarks/servers/README.md** - Complete usage documentation
2. **docs/architecture/benchmarks.md** - Link to new benchmarks (if docs exist)
3. **CLAUDE.md** - Add benchmark commands to Development Commands section

---

## Testing Strategy

### Unit Tests

The benchmark suite itself doesn't require extensive unit testing since it's a development/CI tool, but key components should be validated:

```python
# tests/unit/test_benchmark_parsers.py
def test_oha_result_parsing():
    """Verify oha JSON output parsing."""
    sample_output = {
        "total": 100000,
        "requestsPerSec": 10000.0,
        "successRate": 100.0,
        "average": 0.001,
        "latencyDistribution": {"50": 0.0008, "99": 0.002},
        "slowest": 0.005,
    }
    result = parse_oha_output(sample_output)
    assert result.rps == 10000.0
    assert result.latency_p99_ms == 2.0
```

### Integration Tests

```bash
# Run quick validation that benchmarks work
uv run pytest benchmarks/servers/tests/ -v

# Manual validation
uv run python benchmarks/servers/runner.py --servers uvicorn_h11 --scenarios hello --quick
```

### Benchmark Validation

Each benchmark run should validate:
1. Error rate < 1%
2. RPS > 100 (sanity check)
3. Latency values positive and reasonable (< 60s)
4. Server started successfully
5. Results file written correctly

### Coverage Requirements

- Parsers: 90%+ coverage
- Results dataclasses: 100% coverage (dataclass validation)
- Report generation: 80%+ coverage
- Runner: Manual testing (subprocess management)

---

## Implementation Notes

### Pattern Deviations (with Rationale)

1. **External tool (oha) instead of Python client**: Python HTTP clients (httpx, aiohttp) introduce significant overhead. oha is a Rust-based tool specifically designed for benchmarking with minimal overhead.

2. **Subprocess-based server management**: Servers run as separate processes to accurately reflect production deployment. This is more complex than in-process testing but provides realistic results.

3. **Hardcoded ports per server**: Avoids port conflicts when running multiple servers in sequence. Each server gets a dedicated port (8001-8006).

### Dependencies

**Required**:
- `oha` - Rust HTTP load generator (cargo install oha)

**Optional (for comparison)**:
- `uvicorn[standard]` - Uvicorn with uvloop and httptools
- `granian` - Rust-based ASGI server
- `hypercorn[uvloop]` - ASGI server with HTTP/2
- `daphne` - Django Channels ASGI server

### Migration Considerations

This is a new feature with no migration requirements. Existing benchmarks (`ipc_fast_path.py`, etc.) remain unchanged.

### oha Installation

```bash
# Via Cargo (recommended)
cargo install oha

# Via Homebrew (macOS)
brew install oha

# Via package manager (Arch Linux)
pacman -S oha
```

---

## Verification Checklist

### Functional Verification

- [ ] `make bench-servers` runs without errors
- [ ] All 6 servers benchmarked (or graceful skip if not installed)
- [ ] All 6+ scenarios executed per server
- [ ] JSON results written to `benchmarks/results/data.json`
- [ ] Markdown report written to `benchmarks/results/report.md`
- [ ] Vertebra included in comparison

### Quality Verification

- [ ] Report tables properly formatted
- [ ] Latency values in correct units (milliseconds)
- [ ] RPS values reasonable (> 1000 for hello world)
- [ ] Environment metadata captured
- [ ] No server processes left running after benchmark

### Documentation Verification

- [ ] README explains installation and usage
- [ ] Methodology documented
- [ ] CLI options documented
- [ ] Troubleshooting section included

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| oha not installed | Medium | High | Clear error message with installation instructions |
| Server fails to start | Medium | Medium | Health check with timeout, graceful skip |
| Port conflicts | Low | Medium | Dedicated ports per server |
| Benchmark interference | Low | Medium | Cool-down period between servers |
| Results variance | Medium | Low | Multiple runs, statistical analysis |
| Missing server packages | High | Low | Graceful skip with warning |

---

## Implementation Phases

### Phase 1: Infrastructure (Core Framework)

**Tasks**:
1. Create `benchmarks/servers/` directory structure
2. Implement `config.py` with server configurations
3. Implement `parsers.py` for oha output parsing
4. Implement `results.py` with dataclasses
5. Implement `utils.py` with process management
6. Create standard ASGI test application

**Validation**: Unit tests pass, test app runs on Uvicorn

### Phase 2: Benchmark Execution

**Tasks**:
1. Implement `scenarios.py` with test definitions
2. Implement `runner.py` orchestrator
3. Add CLI argument parsing
4. Implement three-phase execution (primer/warmup/measure)
5. Add Makefile targets
6. Create `README.md` documentation

**Validation**: `make bench-servers --quick` completes successfully

### Phase 3: Reporting and Polish

**Tasks**:
1. Implement `reports.py` for markdown generation
2. Add JSON output with environment metadata
3. Add historical results tracking (optional)
4. Add Vertebra-specific breakdown metrics
5. Add CI workflow (optional)
6. Final documentation polish

**Validation**: Full benchmark run produces complete report

---

## Appendix: Expected Output Format

### JSON Results (data.json)

```json
{
  "timestamp": "2025-01-24T10:30:00Z",
  "environment": {
    "cpu": "AMD Ryzen 7 5700X @ 4.5GHz",
    "cores": 16,
    "os": "Linux 6.12.58",
    "python_version": "3.12.0",
    "oha_version": "1.4.0"
  },
  "results": [
    {
      "server": "vertebra",
      "scenario": "hello",
      "concurrency": 64,
      "duration_seconds": 10,
      "total_requests": 650000,
      "rps": 65000.0,
      "success_rate": 100.0,
      "latency_avg_ms": 0.98,
      "latency_p50_ms": 0.82,
      "latency_p95_ms": 1.45,
      "latency_p99_ms": 1.89,
      "latency_max_ms": 5.23
    }
  ]
}
```

### Markdown Report (report.md)

```markdown
# ASGI Server Benchmark Results

**Date**: 2025-01-24 10:30:00
**CPU**: AMD Ryzen 7 5700X @ 4.5GHz
**OS**: Linux 6.12.58
**Python**: 3.12.0
**oha**: 1.4.0

## Hello World (12-byte JSON)

| Server | c64 RPS | c256 RPS | c64 p99 | c256 p99 |
|--------|---------|----------|---------|----------|
| Vertebra | 65,000 | 180,000 | 1.89ms | 2.45ms |
| Granian | 52,000 | 145,000 | 2.12ms | 3.21ms |
| Uvicorn (httptools) | 48,000 | 130,000 | 2.34ms | 3.89ms |
| Uvicorn (h11) | 35,000 | 95,000 | 3.12ms | 5.45ms |
| Hypercorn | 32,000 | 88,000 | 3.45ms | 5.89ms |
| Daphne | 25,000 | 70,000 | 4.12ms | 7.23ms |

## 10KB Response

| Server | c64 RPS | c256 RPS | c64 p99 | c256 p99 |
|--------|---------|----------|---------|----------|
...
```

---

## References

1. Granian Benchmarks: https://github.com/emmett-framework/granian/tree/master/benchmarks
2. oha Load Generator: https://github.com/hatoo/oha
3. Uvicorn Documentation: https://www.uvicorn.org/
4. Hypercorn Documentation: https://hypercorn.readthedocs.io/
5. ASGI Specification: https://asgi.readthedocs.io/
6. TechEmpower Benchmarks: https://www.techempower.com/benchmarks/
