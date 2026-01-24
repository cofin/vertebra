# ASGI Server Benchmarks - Implementation Tasks

## Overview

- **Complexity**: Medium-High
- **Total Phases**: 3
- **Estimated Task Count**: 24
- **Dependencies**: oha (Rust load generator), various ASGI servers and frameworks

## Benchmark Categories

| Category | What it Tests | Entries |
|----------|---------------|---------|
| 1. Raw Servers | HTTP parsing, event loop | 6 ASGI servers |
| 2. Integrated Runtimes | Full stack (server+framework) | 4 runtimes |
| 3. Framework Overhead | Routing, middleware, validation | 6 frameworks |

---

## Phase 1: Infrastructure (Core Framework)

### Task 1.1: Create Directory Structure
- [ ] Create `benchmarks/servers/` package
- [ ] Create `benchmarks/servers/__init__.py`
- [ ] Create `benchmarks/servers/app/` directory
- [ ] Create `benchmarks/servers/app/__init__.py`
- [ ] Create `benchmarks/servers/app/assets/` directory
- [ ] Create `benchmarks/results/.gitkeep`

**Validation**: Directory structure exists

### Task 1.2: Implement Categorized Configurations
- [ ] Create `benchmarks/servers/config.py`
- [ ] Define `ServerConfig` and `FrameworkConfig` dataclasses
- [ ] Define `BenchmarkCategory` enum (SERVER, RUNTIME, FRAMEWORK)

**Category 1 - ASGI Servers**:
- [ ] Uvicorn (h11) - port 8002
- [ ] Uvicorn (httptools) - port 8003
- [ ] Granian - port 8004
- [ ] Hypercorn - port 8005
- [ ] Daphne - port 8006
- [ ] Socketify - port 8007

**Category 2 - Integrated Runtimes**:
- [ ] Vertebra - port 8001
- [ ] Robyn - port 8010
- [ ] Sanic (standalone) - port 8011
- [ ] Spikard - port 8012

**Category 3 - Frameworks** (all on Uvicorn httptools, port 8020-8025):
- [ ] Raw ASGI (baseline)
- [ ] Starlette
- [ ] FastAPI
- [ ] BlackSheep
- [ ] Litestar
- [ ] Sanic (ASGI mode)

**Validation**: Import config module without errors

### Task 1.3: Implement oha Parser
- [ ] Create `benchmarks/servers/parsers.py`
- [ ] Define `OhaResult` dataclass
- [ ] Implement `run_oha()` function
- [ ] Parse JSON output from oha
- [ ] Convert latency units (seconds to milliseconds)
- [ ] Handle oha command failures gracefully

**Validation**: Parse sample oha output correctly

### Task 1.4: Implement Result Dataclasses
- [ ] Create `benchmarks/servers/results.py`
- [ ] Define `BenchmarkResult` dataclass
- [ ] Define `EnvironmentInfo` dataclass
- [ ] Define `BenchmarkSuite` dataclass
- [ ] Implement JSON serialization
- [ ] Add result validation (sanity checks)

**Validation**: Create and serialize sample results

### Task 1.5: Implement Utilities
- [ ] Create `benchmarks/servers/utils.py`
- [ ] Implement `start_server()` with health check
- [ ] Implement `stop_server()` with cleanup
- [ ] Implement `wait_for_health()` function
- [ ] Implement `detect_oha_version()` function
- [ ] Implement `detect_environment()` function

**Validation**: Start and stop Uvicorn programmatically

### Task 1.6: Create Test Applications (Category 1 - Raw ASGI)
- [ ] Create `benchmarks/servers/app/asgi.py`
- [ ] Implement `/hello` endpoint (JSON)
- [ ] Implement `/b10`, `/b1k`, `/b10k`, `/b100k` endpoints
- [ ] Implement `/echo` endpoint
- [ ] Implement `/io10`, `/io100` endpoints
- [ ] Implement `/file` endpoint
- [ ] Implement `/json` endpoint
- [ ] Implement `/health` endpoint
- [ ] Add 50KB test image to assets/

**Validation**: Run with Uvicorn, all endpoints work

### Task 1.7: Create Test Applications (Category 2 - Native Runtimes)
- [ ] Create `benchmarks/servers/app/robyn_app.py` for Robyn
- [ ] Create `benchmarks/servers/app/sanic_app.py` for Sanic standalone
- [ ] Create `benchmarks/servers/app/spikard_app.py` for Spikard
- [ ] Each app mirrors the same endpoints as raw ASGI

**Validation**: Each runtime app starts and responds correctly

### Task 1.8: Create Test Applications (Category 3 - Frameworks)
- [ ] Create `benchmarks/servers/app/starlette_app.py` for Starlette
- [ ] Create `benchmarks/servers/app/fastapi_app.py` for FastAPI
- [ ] Create `benchmarks/servers/app/blacksheep_app.py` for BlackSheep
- [ ] Create `benchmarks/servers/app/litestar_app.py` for Litestar
- [ ] Create `benchmarks/servers/app/sanic_asgi_app.py` for Sanic ASGI
- [ ] Each app mirrors the same endpoints as raw ASGI

**Validation**: All framework apps work on Uvicorn

---

## Phase 2: Benchmark Execution

### Task 2.1: Define Test Scenarios
- [ ] Create `benchmarks/servers/scenarios.py`
- [ ] Define `Scenario` dataclass
- [ ] Add "hello" scenario (JSON response)
- [ ] Add "body_10b" to "body_100k" scenarios
- [ ] Add "echo" scenario
- [ ] Add "io_wait" scenarios
- [ ] Add "file" scenario
- [ ] Add "json" scenario

**Validation**: Import scenarios module

### Task 2.2: Implement Benchmark Orchestrator
- [ ] Create `benchmarks/servers/runner.py`
- [ ] Implement main entry point
- [ ] Implement server iteration loop
- [ ] Implement scenario iteration loop
- [ ] Implement three-phase execution (primer/warmup/measure)
- [ ] Add progress output
- [ ] Handle keyboard interrupt gracefully

**Validation**: Run quick benchmark against single server

### Task 2.3: Implement CLI Arguments
- [ ] Add argparse to runner.py
- [ ] Add `--servers` argument (comma-separated)
- [ ] Add `--scenarios` argument (comma-separated)
- [ ] Add `--quick` argument (reduced duration)
- [ ] Add `--duration` argument
- [ ] Add `--concurrency` argument
- [ ] Add `--output-dir` argument
- [ ] Add `--verbose` argument
- [ ] Add `--workers` argument (for scaling tests)

**Validation**: `--help` shows all options

### Task 2.4: Add Makefile Targets
- [ ] Add `bench-servers` target
- [ ] Add `bench-servers-quick` target
- [ ] Add `bench-vertebra` target
- [ ] Add `bench-install-deps` target
- [ ] Add oha installation check

**Validation**: `make bench-servers-quick` runs

### Task 2.5: Create README Documentation
- [ ] Create `benchmarks/servers/README.md`
- [ ] Document installation requirements
- [ ] Document usage examples
- [ ] Document available servers
- [ ] Document available scenarios
- [ ] Document CLI options
- [ ] Add troubleshooting section

**Validation**: README is complete and accurate

---

## Phase 3: Reporting and Polish

### Task 3.1: Implement Markdown Report Generator
- [ ] Create `benchmarks/servers/reports.py`
- [ ] Implement `generate_markdown_report()` function
- [ ] Add environment information section
- [ ] Add comparison tables by scenario
- [ ] Add latency percentile columns
- [ ] Sort servers by RPS (descending)
- [ ] Handle missing results gracefully

**Validation**: Generate readable markdown from sample data

### Task 3.2: Implement JSON Output
- [ ] Add JSON serialization to results.py
- [ ] Write results to `benchmarks/results/data.json`
- [ ] Include timestamp
- [ ] Include environment metadata
- [ ] Include all benchmark results
- [ ] Format JSON with indentation

**Validation**: JSON file is valid and complete

### Task 3.3: Add Historical Results Tracking (Optional)
- [ ] Create timestamped subdirectories in results/
- [ ] Symlink `latest` to most recent run
- [ ] Optionally compare to previous run
- [ ] Highlight regressions if detected

**Validation**: Historical results preserved

### Task 3.4: Add Vertebra-Specific Metrics
- [ ] Add IPC latency breakdown (if measurable)
- [ ] Add native path vs ASGI path comparison
- [ ] Add worker scaling test (1, 2, 4, 8 workers)
- [ ] Report memory per worker (optional)
- [ ] Integrate with existing IPC benchmarks

**Validation**: Vertebra section in report

### Task 3.5: Add CI Workflow (Optional)
- [ ] Create `.github/workflows/benchmark.yml`
- [ ] Schedule weekly runs
- [ ] Upload artifacts (results/)
- [ ] Cache oha installation
- [ ] Set up Python and server dependencies

**Validation**: Workflow runs on push to branch

### Task 3.6: Final Documentation Polish
- [ ] Update CLAUDE.md with benchmark commands
- [ ] Add benchmark section to main README if appropriate
- [ ] Review and clean up code comments
- [ ] Ensure all docstrings are complete
- [ ] Add type hints throughout

**Validation**: All documentation accurate

---

## Verification Checklist

After implementation, verify:

- [ ] `make bench-servers-quick` completes in < 15 minutes
- [ ] Category 1: All 6 ASGI servers benchmarked (or gracefully skipped)
- [ ] Category 2: All 4 integrated runtimes benchmarked
- [ ] Category 3: All 6 frameworks benchmarked
- [ ] JSON results file created with category groupings
- [ ] Markdown report with separate sections per category
- [ ] No server processes left running
- [ ] Report tables properly formatted
- [ ] CLI help is accurate
- [ ] README documentation is complete
- [ ] No lint errors (`make lint-all`)
- [ ] Vertebra included in Category 2 comparisons

---

## Dependencies

### Required
- [ ] oha installed (`cargo install oha`)
- [ ] Vertebra working (`vertebra serve`)

### Category 1 - ASGI Servers (optional)
- [ ] uvicorn[standard] installed
- [ ] granian installed
- [ ] hypercorn[uvloop] installed
- [ ] daphne installed
- [ ] socketify installed

### Category 2 - Integrated Runtimes (optional)
- [ ] robyn installed
- [ ] sanic installed
- [ ] spikard installed

### Category 3 - Frameworks (optional)
- [ ] starlette installed
- [ ] fastapi installed
- [ ] blacksheep installed
- [ ] litestar installed

---

## Notes

- Each phase builds on the previous
- Phase 1 can be tested with just Uvicorn
- Phase 2 requires all servers for full testing
- Phase 3 can be done incrementally
- CI workflow is optional but recommended
