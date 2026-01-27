# ASGI Server Benchmarks

Comprehensive benchmark suite for comparing ASGI servers, integrated runtimes, and web frameworks.

## Quick Start

```bash
# Set up benchmark environment (creates separate venv + installs oha)
make bench-setup

# Run quick benchmark
make bench-servers-quick

# Run full benchmark
make bench-servers
```

## Isolated Benchmark Environment

The benchmark suite uses a **separate virtual environment** (`benchmarks/.venv/`) to avoid polluting
the main project dependencies with benchmark-only packages like uvicorn, granian, fastapi, etc.

### Setup

```bash
# Create benchmark venv and install all dependencies
make bench-setup

# Verify setup
uv run python -m benchmarks.servers.venv_manager check
```

### Using Your Own Environment

If you prefer to use the main project venv or your own environment:

```bash
# Install dependencies to current venv
pip install uvicorn[standard] granian hypercorn[uvloop] daphne starlette fastapi blacksheep litestar sanic

# Run benchmarks without isolated venv
uv run python -m benchmarks.servers.runner --no-venv
```

## Requirements

### Required

- **Python 3.12+**
- **oha** - Rust-based HTTP load generator ([install](https://github.com/hatoo/oha))

### Benchmark Dependencies (installed by `make bench-setup`)

The following are installed in the isolated benchmark venv:

- `uvicorn[standard]` - Uvicorn with uvloop + httptools
- `granian` - Rust ASGI server
- `hypercorn[uvloop]` - HTTP/2 capable server
- `daphne` - Django Channels server
- `starlette` - Minimal ASGI framework
- `fastapi` - Feature-rich framework
- `blacksheep` - High-performance framework
- `litestar` - msgspec-powered framework
- `sanic` - Standalone + ASGI modes

## Benchmark Categories

The suite benchmarks three distinct categories:

### Category 1: Raw ASGI Server Performance

Same minimal ASGI application on different servers. Isolates HTTP parsing, connection handling, and event loop efficiency.

| Server | Notes |
|--------|-------|
| Uvicorn (h11) | Pure Python HTTP parsing, baseline |
| Uvicorn (httptools) | ~30% faster via Cython HTTP parsing |
| Granian | Rust runtime with PyO3 |
| Hypercorn | HTTP/2 and HTTP/3 capable |
| Daphne | Django Channels reference |
| Socketify | C++ uSockets backend |

### Category 2: Integrated Runtime Performance

Each runtime runs its native application format. Server and framework are inseparable.

| Runtime | Notes |
|---------|-------|
| Vertebra | Rust HTTP + Rust IPC + Python ASGI (raw) |
| Vertebra ASGI (Litestar) | Litestar via standard ASGI bridge (`--no-native`) |
| Vertebra Native (Litestar) | Litestar with Rust router bypass (`--native`) |
| Vertebra (FastAPI) | FastAPI through Vertebra ASGI bridge |
| Robyn | Rust Actix + Tokio runtime |
| Sanic (standalone) | Built-in uvloop server |
| Spikard | Rust Tower-HTTP runtime |

### Category 3: Framework Overhead

Same server (Uvicorn httptools), different frameworks. Isolates routing, middleware, and serialization overhead.

| Framework | Notes |
|-----------|-------|
| Raw ASGI | No framework, baseline |
| Starlette | Minimal routing/middleware |
| FastAPI (Uvicorn) | Pydantic validation, Uvicorn httptools |
| FastAPI (Granian) | Pydantic validation, Granian Rust runtime |
| BlackSheep | Cython-optimized routing |
| Litestar (Uvicorn) | msgspec validation, Uvicorn httptools |
| Litestar (Granian) | msgspec validation, Granian Rust runtime |
| Sanic (ASGI) | Full framework on ASGI |

## Test Scenarios

| Endpoint | Response | Purpose |
|----------|----------|---------|
| `/hello` | 12-byte JSON | Minimal overhead test |
| `/b10` | 10 bytes | Small response |
| `/b1k` | 1KB | Typical API response |
| `/b10k` | 10KB | Larger response |
| `/b100k` | 100KB | Large response |
| `/echo` | POST body | Bidirectional I/O |
| `/io10` | After 10ms sleep | Fast I/O simulation |
| `/io100` | After 100ms sleep | Slow query simulation |
| `/file` | 50KB image | Static file serving |
| `/json` | 1KB JSON | Serialization test |

## Usage

### Basic Usage

```bash
# Standard benchmark (all available servers)
uv run python -m benchmarks.servers.runner

# Quick mode (shorter duration)
uv run python -m benchmarks.servers.runner --quick

# Specific servers
uv run python -m benchmarks.servers.runner --servers vertebra,granian,uvicorn_httptools

# Specific scenarios
uv run python -m benchmarks.servers.runner --scenarios hello,json,body_1k
```

### CLI Options

| Option | Description |
|--------|-------------|
| `--servers` | Comma-separated list of servers |
| `--scenarios` | Comma-separated scenarios, or 'all' |
| `--quick` | Quick mode with reduced duration |
| `--duration` | Measurement duration in seconds |
| `--concurrency` | Comma-separated concurrency levels |
| `--workers` | Worker count for servers (default: 1) |
| `--output-dir` | Custom output directory |
| `--verbose` | Show server stdout/stderr |
| `--no-venv` | Don't use benchmark venv, use current environment |
| `--list-servers` | List available servers |
| `--list-scenarios` | List available scenarios |

### Examples

```bash
# Full comprehensive benchmark
uv run python -m benchmarks.servers.runner --scenarios all --concurrency 8,64,256,512

# Benchmark only Vertebra with verbose output
uv run python -m benchmarks.servers.runner --servers vertebra --verbose

# Custom duration and concurrency
uv run python -m benchmarks.servers.runner --duration 30 --concurrency 128,512

# List all available servers
uv run python -m benchmarks.servers.runner --list-servers
```

## Output

Results are saved to `benchmarks/results/`:

- `data.json` - Machine-readable results with all metrics
- `report.md` - Human-readable markdown report

### JSON Schema

```json
{
  "environment": {
    "cpu": "AMD Ryzen 7 5700X",
    "cpu_cores": 16,
    "os": "Linux",
    "os_version": "6.12.58",
    "python_version": "3.12.0",
    "oha_version": "1.4.0",
    "timestamp": "2025-01-24T10:30:00Z"
  },
  "results": [
    {
      "server": "vertebra",
      "category": "runtime",
      "scenario": "hello",
      "concurrency": 64,
      "rps": 65000.0,
      "latency_p99_ms": 1.89,
      ...
    }
  ],
  "errors": []
}
```

## Methodology

### Three-Phase Execution

1. **Primer** (4s): Warm up caches, JIT compilation, connection pools
2. **Warmup** (3s): Stabilization period (no measurement)
3. **Measurement** (10s): Actual data collection

### Fair Comparison

- Single worker process for all servers
- uvloop event loop where supported
- Access logging disabled
- Dedicated port per server (no port reuse)
- Cool-down period between servers

### Latency Correction

Uses `oha --latency-correction` to avoid coordinated omission bias, ensuring accurate percentile measurements under load.

## Troubleshooting

### oha not found

```bash
# Install via Cargo
cargo install oha

# Or via Homebrew (macOS)
brew install oha

# Or via package manager (Arch Linux)
pacman -S oha
```

### Server won't start

1. Check if port is already in use: `lsof -i :8001`
2. Verify server package is installed
3. Run with `--verbose` to see server output

### Results look wrong

- Ensure single worker mode (default)
- Check for background processes consuming CPU
- Allow sufficient cool-down between servers
- Verify health check passes before benchmarking

### Missing dependencies

```bash
# Install all benchmark dependencies
make bench-install-deps
```

## Adding New Servers

1. Add configuration to `config.py`:

```python
"my_server": ServerConfig(
    name="my_server",
    display_name="My Server",
    category=BenchmarkCategory.SERVER,
    command=("my-server", "--port", "8099", "benchmarks.servers.app.asgi:app"),
    port=8099,
    requires_install=("my-server",),
    notes="Description of the server",
)
```

2. Test that the server starts and responds to `/health`.

## References

- [oha](https://github.com/hatoo/oha) - HTTP load generator
- [Granian Benchmarks](https://github.com/emmett-framework/granian/tree/master/benchmarks) - Methodology reference
- [TechEmpower Benchmarks](https://www.techempower.com/benchmarks/) - Industry standard
