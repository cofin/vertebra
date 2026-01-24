# Recovery Guide for ASGI Server Benchmarks

## Quick Resume

**Last Phase**: Planning Complete
**Current Status**: Ready for Implementation
**PRD Location**: `specs/active/asgi-server-benchmarks/`

---

## Intelligence Context

### Complexity
- **Level**: Medium
- **Phases**: 3 (Infrastructure → Benchmarks → Reporting)
- **Risk**: Low (additive feature, no production code changes)

### Stack
- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **Primary Tool**: `oha` (Rust HTTP load generator)
- **Category 1 (Servers)**: Uvicorn (h11/httptools), Granian, Hypercorn, Daphne, Socketify
- **Category 2 (Runtimes)**: Vertebra, Robyn, Sanic, Spikard
- **Category 3 (Frameworks)**: Raw ASGI, Starlette, FastAPI, BlackSheep, Litestar, Sanic

### Commands
- **Test**: `make test`
- **Lint**: `make lint-all`
- **Benchmark (Quick)**: `make bench-servers-quick`
- **Benchmark (Full)**: `make bench-servers`
- **Install Dependencies**: `make bench-install-deps`

### Documentation
- **System**: None (standalone tooling)
- **Primary Doc**: `benchmarks/servers/README.md`

### Similar Implementations
- `benchmarks/ipc_fast_path.py` - IPC latency benchmarking pattern
- `benchmarks/py_serialization.py` - Result dataclass pattern
- `benchmarks/native_litestar.py` - Simple server benchmark (to replace)
- Granian's `benchmarks/` - External reference implementation

### Patterns Being Followed
- Dataclass results with computed properties (from ipc_fast_path.py)
- Three-phase execution: Primer → Warmup → Measurement (from Granian)
- JSON + Markdown output
- CLI with argparse
- Makefile integration

---

## State Summary

### Completed
- [x] Project auto-discovery (CLAUDE.md, Makefile)
- [x] Existing benchmark analysis
- [x] Granian methodology research
- [x] oha tool analysis
- [x] Competitor analysis
- [x] Architecture design
- [x] Test application design
- [x] Server configuration design
- [x] Report format design
- [x] PRD document (3200+ words)
- [x] Research document (2400+ words)
- [x] Analysis document
- [x] Task breakdown

### Pending
- [ ] Phase 1: Infrastructure implementation
- [ ] Phase 2: Benchmark execution implementation
- [ ] Phase 3: Reporting and polish

---

## Key Decisions Made

1. **Use oha** as primary load generator (faster than wrk, hey)
2. **Follow Granian's methodology** for fair, reproducible benchmarks
3. **Standardized test app** shared across all ASGI servers
4. **JSON + Markdown output** for both automation and human readability
5. **Dedicated ports per server** (8001-8006) to avoid conflicts
6. **Single worker default** for apples-to-apples comparison
7. **uvloop enabled** for all async servers where available

---

## File Structure

```
benchmarks/servers/
├── __init__.py
├── runner.py              # Main orchestrator
├── config.py              # Server configurations
├── scenarios.py           # Test scenario definitions
├── parsers.py             # oha output parsing
├── results.py             # Result dataclasses
├── reports.py             # Report generation
├── utils.py               # Utilities
├── app/
│   ├── __init__.py
│   ├── asgi.py           # Standard ASGI test application
│   └── assets/
│       └── image.jpg     # 50KB test file
└── README.md

benchmarks/results/
├── data.json              # Machine-readable results
├── report.md              # Human-readable report
└── .gitkeep
```

---

## Test Endpoints

| Endpoint | Response | Purpose |
|----------|----------|---------|
| `/hello` | `{"hello":"world"}` (12B) | Minimal overhead |
| `/b10` | 10 bytes | Small response |
| `/b1k` | 1KB | Typical API response |
| `/b10k` | 10KB | Larger response |
| `/b100k` | 100KB | Large response |
| `/echo` | Request body | Bidirectional |
| `/io10` | "ok" after 10ms sleep | I/O simulation |
| `/io100` | "ok" after 100ms sleep | Slow query simulation |
| `/file` | 50KB image | Static file |
| `/json` | 1KB JSON | Serialization |
| `/health` | "ok" | Health check |

---

## Server Configurations

## Category 1: ASGI Servers (Raw Server Performance)

| Server | Port | Command |
|--------|------|---------|
| Uvicorn (h11) | 8002 | `uvicorn --port 8002 --http h11 --loop uvloop asgi:app` |
| Uvicorn (httptools) | 8003 | `uvicorn --port 8003 --http httptools --loop uvloop asgi:app` |
| Granian | 8004 | `granian --interface asgi --port 8004 --workers 1 asgi:app` |
| Hypercorn | 8005 | `hypercorn --bind :8005 --workers 1 -k uvloop asgi:app` |
| Daphne | 8006 | `daphne -p 8006 asgi:app` |
| Socketify | 8007 | `python -m socketify asgi:app --port 8007 --interface asgi` |

## Category 2: Integrated Runtimes (Full Stack Performance)

| Runtime | Port | Command |
|---------|------|---------|
| Vertebra | 8001 | `vertebra serve asgi:app --port 8001 --workers 1` |
| Robyn | 8010 | `python robyn_app.py --port 8010 --processes 1` |
| Sanic (standalone) | 8011 | `sanic sanic_app:app --port 8011 --single-process` |
| Spikard | 8012 | `python spikard_app.py` |

## Category 3: Frameworks (on Uvicorn httptools)

| Framework | Port | App File |
|-----------|------|----------|
| Raw ASGI | 8020 | `asgi.py` |
| Starlette | 8021 | `starlette_app.py` |
| FastAPI | 8022 | `fastapi_app.py` |
| BlackSheep | 8023 | `blacksheep_app.py` |
| Litestar | 8024 | `litestar_app.py` |
| Sanic (ASGI) | 8025 | `sanic_asgi_app.py` |

---

## oha Usage

```bash
# Quick test
oha -z 3s -c 64 --no-tui -j http://127.0.0.1:8001/hello

# Full benchmark
oha -z 10s -c 256 --no-tui -j --latency-correction http://127.0.0.1:8001/hello
```

---

## Environment Requirements

### Required
- Python 3.12+
- oha (`cargo install oha`)
- Vertebra (the project itself)

### Optional (for full comparison)
```bash
# Category 1: ASGI Servers
pip install uvicorn[standard] granian hypercorn[uvloop] daphne socketify

# Category 2: Integrated Runtimes
pip install robyn sanic spikard

# Category 3: Frameworks
pip install starlette fastapi blacksheep litestar
```

---

## Next Steps

Run `/implement asgi-server-benchmarks` to begin implementation.

### Implementation Order

1. **Phase 1**: Create directory structure, config, parsers, results, utilities, test app
2. **Phase 2**: Implement runner, scenarios, CLI, Makefile targets
3. **Phase 3**: Report generation, JSON output, documentation

---

## Troubleshooting

### oha not found
```bash
cargo install oha
# or
brew install oha  # macOS
```

### Server won't start
- Check if port is already in use
- Check if server package is installed
- Check logs in subprocess output

### Results look wrong
- Ensure single worker mode
- Check for background processes
- Allow cool-down between servers
- Verify health check passes before benchmarking

---

## References

- Granian Benchmarks: https://github.com/emmett-framework/granian/tree/master/benchmarks
- oha: https://github.com/hatoo/oha
- PRD: `specs/active/asgi-server-benchmarks/prd.md`
- Research: `specs/active/asgi-server-benchmarks/research/plan.md`
