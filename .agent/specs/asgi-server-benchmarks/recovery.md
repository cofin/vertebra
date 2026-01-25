# Recovery Guide for ASGI Server Benchmarks

## Quick Resume

**Last Phase**: Implementation Complete
**Current Status**: All 19 servers configured and ready
**PRD Location**: `specs/active/asgi-server-benchmarks/`

---

## Intelligence Context

### Complexity
- **Level**: Medium
- **Phases**: 3 (Infrastructure → Benchmarks → Reporting)
- **Risk**: Low (additive feature, no production code changes)

### Stack
- **Languages**: Python 3.12+, TypeScript (Bun), Rust
- **Primary Tool**: `oha` (Rust HTTP load generator, v1.12+ with `--output-format json`)
- **Total Servers**: 19 configurations across 3 categories

### Commands
- **Test**: `make test`
- **Lint**: `make lint-all`
- **Benchmark Setup**: `make bench-setup` (creates isolated venv)
- **Benchmark (Quick)**: `make bench-servers-quick`
- **Benchmark (Full)**: `make bench-servers`

### Documentation
- **Primary Doc**: `benchmarks/servers/README.md`

---

## State Summary

### ✅ Completed
- [x] Phase 1: Infrastructure implementation
- [x] Phase 2: Benchmark execution implementation
- [x] Phase 3: Reporting and polish
- [x] Unit testing (106 tests, 86% coverage)
- [x] Framework-specific test applications
- [x] Cross-server Litestar comparison
- [x] Cross-server FastAPI comparison
- [x] Isolated benchmark venv
- [x] oha binary detection for ~/.cargo/bin/

### ✅ Tracking & CI
- [x] Historical results tracking (`benchmarks/results/runs/`)
- [x] Vertebra-specific metrics breakdown (ASGI vs Native, overhead analysis)
- [x] CI workflow for Vertebra benchmarks only (`.github/workflows/benchmark.yml`)

---

## Server Configurations (19 Total)

### Category 1: ASGI Servers (4)

| Server | Port |
|--------|------|
| Uvicorn (h11) | 8002 |
| Uvicorn (httptools) | 8003 |
| Granian | 8004 |
| Socketify | 8007 |

### Category 2: Integrated Runtimes (7)

| Runtime | Port | Notes |
|---------|------|-------|
| Vertebra | 8001 | Raw ASGI |
| Vertebra ASGI (Litestar) | 8008 | `--no-native` |
| Vertebra Native (Litestar) | 8009 | `--native` (Rust router) |
| Vertebra (FastAPI) | 8013 | ASGI bridge only |
| Robyn | 8010 | Rust Actix runtime |
| Sanic (standalone) | 8011 | Built-in uvloop |
| Spikard | 8012 | Tower-HTTP runtime |

### Category 3: Framework Overhead (8)

| Framework | Port | Server |
|-----------|------|--------|
| Raw ASGI | 8020 | Uvicorn |
| Starlette | 8021 | Uvicorn |
| FastAPI (Uvicorn) | 8022 | Uvicorn |
| FastAPI (Granian) | 8027 | Granian |
| BlackSheep | 8023 | Uvicorn |
| Litestar (Uvicorn) | 8024 | Uvicorn |
| Litestar (Granian) | 8026 | Granian |
| Sanic (ASGI) | 8025 | Uvicorn |

---

## File Structure

```
benchmarks/servers/
├── __init__.py
├── runner.py              # Main orchestrator
├── config.py              # 19 server configurations
├── scenarios.py           # 10 test scenarios
├── parsers.py             # oha output parsing + cargo bin detection
├── results.py             # Result dataclasses
├── reports.py             # Report generation
├── utils.py               # Process management
├── venv_manager.py        # Isolated benchmark venv
├── app/
│   ├── __init__.py
│   ├── asgi.py            # Raw ASGI app
│   ├── starlette_app.py   # Starlette framework
│   ├── fastapi_app.py     # FastAPI framework
│   ├── blacksheep_app.py  # BlackSheep framework
│   ├── litestar_app.py    # Litestar framework
│   ├── sanic_app.py       # Sanic (standalone + ASGI)
│   ├── robyn_app.py       # Robyn native
│   ├── spikard_app.py     # Spikard native
│   └── assets/
│       └── test.bin       # 50KB test file
└── README.md

benchmarks/results/
└── .gitkeep
```

---

## Key Features Added

### 1. Isolated Benchmark Environment
- `benchmarks/.venv/` separate from project `.venv`
- `make bench-setup` creates venv and installs dependencies
- `--no-venv` flag to use current environment

### 2. Cross-Server Framework Testing
- Litestar: Uvicorn, Granian, Vertebra ASGI, Vertebra Native
- FastAPI: Uvicorn, Granian, Vertebra

### 3. Vertebra Native Integration
- `--native` flag for Rust router bypass
- `--no-native` flag for standard ASGI bridge
- Measures native path performance gain

### 4. oha Binary Detection
- Checks `~/.cargo/bin/oha` (cargo install location)
- Checks common system paths
- No more "oha not found" errors when installed via cargo

### 5. Historical Results Tracking
- Saves runs to `benchmarks/results/runs/<timestamp>.json`
- Compares current run against previous for Vertebra servers
- Detects regressions (>5% RPS decrease)
- `--list-history` flag to view past runs

### 6. Vertebra Metrics Breakdown
- Native vs ASGI Router comparison (Litestar)
- IPC transport overhead estimation
- Framework overhead breakdown
- Displayed in both terminal and markdown reports

### 7. CI Workflow
- `.github/workflows/benchmark.yml`
- Runs only Vertebra variants (respectful of external projects)
- Weekly scheduled runs + manual triggers
- Commits history to track regression over time
- Comments on PRs if regression detected

---

## Usage Examples

```bash
# Set up isolated benchmark environment
make bench-setup

# Run quick benchmark on all servers
make bench-servers-quick

# Compare Litestar across servers
uv run python -m benchmarks.servers.runner \
  --servers vertebra_native_litestar,vertebra_asgi_litestar,litestar_uvicorn,litestar_granian \
  --quick

# Compare FastAPI across servers
uv run python -m benchmarks.servers.runner \
  --servers fastapi_uvicorn,fastapi_granian,vertebra_fastapi \
  --quick

# Full benchmark with custom settings
uv run python -m benchmarks.servers.runner \
  --duration 30 --concurrency 128,512 --scenarios hello,json
```

---

## Status: IMPLEMENTATION COMPLETE ✓

All core functionality implemented:
- 21 server configurations
- 10 test scenarios
- 7 framework-specific apps
- Isolated benchmark venv
- Cross-server comparisons
- 106 unit tests (86% coverage)
- Comprehensive documentation

Ready for final review and archival.
