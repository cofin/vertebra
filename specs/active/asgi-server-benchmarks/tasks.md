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
- [x] Create `benchmarks/servers/` package
- [x] Create `benchmarks/servers/__init__.py`
- [x] Create `benchmarks/servers/app/` directory
- [x] Create `benchmarks/servers/app/__init__.py`
- [x] Create `benchmarks/servers/app/assets/` directory
- [x] Create `benchmarks/results/.gitkeep`

**Validation**: Directory structure exists ✓

### Task 1.2: Implement Categorized Configurations
- [x] Create `benchmarks/servers/config.py`
- [x] Define `ServerConfig` and `FrameworkConfig` dataclasses
- [x] Define `BenchmarkCategory` enum (SERVER, RUNTIME, FRAMEWORK)

**Category 1 - ASGI Servers**:
- [x] Uvicorn (h11) - port 8002
- [x] Uvicorn (httptools) - port 8003
- [x] Granian - port 8004
- [x] Hypercorn - port 8005
- [x] Daphne - port 8006
- [x] Socketify - port 8007

**Category 2 - Integrated Runtimes**:
- [x] Vertebra - port 8001
- [x] Robyn - port 8010
- [x] Sanic (standalone) - port 8011
- [x] Spikard - port 8012

**Category 3 - Frameworks** (all on Uvicorn httptools, port 8020-8025):
- [x] Raw ASGI (baseline)
- [x] Starlette
- [x] FastAPI
- [x] BlackSheep
- [x] Litestar
- [x] Sanic (ASGI mode)

**Validation**: Import config module without errors ✓

### Task 1.3: Implement oha Parser
- [x] Create `benchmarks/servers/parsers.py`
- [x] Define `OhaResult` dataclass
- [x] Implement `run_oha()` function
- [x] Parse JSON output from oha
- [x] Convert latency units (seconds to milliseconds)
- [x] Handle oha command failures gracefully

**Validation**: Parse sample oha output correctly ✓

### Task 1.4: Implement Result Dataclasses
- [x] Create `benchmarks/servers/results.py`
- [x] Define `BenchmarkResult` dataclass
- [x] Define `EnvironmentInfo` dataclass
- [x] Define `BenchmarkSuite` dataclass
- [x] Implement JSON serialization
- [x] Add result validation (sanity checks)

**Validation**: Create and serialize sample results ✓

### Task 1.5: Implement Utilities
- [x] Create `benchmarks/servers/utils.py`
- [x] Implement `start_server()` with health check
- [x] Implement `stop_server()` with cleanup
- [x] Implement `wait_for_health()` function
- [x] Implement `detect_oha_version()` function
- [x] Implement `detect_environment()` function

**Validation**: Start and stop Uvicorn programmatically ✓

### Task 1.6: Create Test Applications (Category 1 - Raw ASGI)
- [x] Create `benchmarks/servers/app/asgi.py`
- [x] Implement `/hello` endpoint (JSON)
- [x] Implement `/b10`, `/b1k`, `/b10k`, `/b100k` endpoints
- [x] Implement `/echo` endpoint
- [x] Implement `/io10`, `/io100` endpoints
- [x] Implement `/file` endpoint
- [x] Implement `/json` endpoint
- [x] Implement `/health` endpoint
- [x] Add 50KB test image to assets/

**Validation**: Run with Uvicorn, all endpoints work ✓

### Task 1.7: Create Test Applications (Category 2 - Native Runtimes)
- [ ] Create `benchmarks/servers/app/robyn_app.py` for Robyn
- [ ] Create `benchmarks/servers/app/sanic_app.py` for Sanic standalone
- [ ] Create `benchmarks/servers/app/spikard_app.py` for Spikard
- [ ] Each app mirrors the same endpoints as raw ASGI

**Validation**: Each runtime app starts and responds correctly
**Status**: Deferred - Optional for runtimes that require native apps

### Task 1.8: Create Test Applications (Category 3 - Frameworks)
- [ ] Create `benchmarks/servers/app/starlette_app.py` for Starlette
- [ ] Create `benchmarks/servers/app/fastapi_app.py` for FastAPI
- [ ] Create `benchmarks/servers/app/blacksheep_app.py` for BlackSheep
- [ ] Create `benchmarks/servers/app/litestar_app.py` for Litestar
- [ ] Create `benchmarks/servers/app/sanic_asgi_app.py` for Sanic ASGI
- [ ] Each app mirrors the same endpoints as raw ASGI

**Validation**: All framework apps work on Uvicorn
**Status**: Deferred - Optional for framework comparison tests

---

## Phase 2: Benchmark Execution

### Task 2.1: Define Test Scenarios
- [x] Create `benchmarks/servers/scenarios.py`
- [x] Define `Scenario` dataclass
- [x] Add "hello" scenario (JSON response)
- [x] Add "body_10b" to "body_100k" scenarios
- [x] Add "echo" scenario
- [x] Add "io_wait" scenarios
- [x] Add "file" scenario
- [x] Add "json" scenario

**Validation**: Import scenarios module ✓

### Task 2.2: Implement Benchmark Orchestrator
- [x] Create `benchmarks/servers/runner.py`
- [x] Implement main entry point
- [x] Implement server iteration loop
- [x] Implement scenario iteration loop
- [x] Implement three-phase execution (primer/warmup/measure)
- [x] Add progress output
- [x] Handle keyboard interrupt gracefully

**Validation**: Run quick benchmark against single server ✓

### Task 2.3: Implement CLI Arguments
- [x] Add argparse to runner.py
- [x] Add `--servers` argument (comma-separated)
- [x] Add `--scenarios` argument (comma-separated)
- [x] Add `--quick` argument (reduced duration)
- [x] Add `--duration` argument
- [x] Add `--concurrency` argument
- [x] Add `--output-dir` argument
- [x] Add `--verbose` argument
- [x] Add `--workers` argument (for scaling tests)

**Validation**: `--help` shows all options ✓

### Task 2.4: Add Makefile Targets
- [x] Add `bench-servers` target
- [x] Add `bench-servers-quick` target
- [x] Add `bench-vertebra` target
- [x] Add `bench-install-deps` target
- [x] Add oha installation check

**Validation**: `make bench-servers-quick` runs ✓

### Task 2.5: Create README Documentation
- [x] Create `benchmarks/servers/README.md`
- [x] Document installation requirements
- [x] Document usage examples
- [x] Document available servers
- [x] Document available scenarios
- [x] Document CLI options
- [x] Add troubleshooting section

**Validation**: README is complete and accurate ✓

---

## Phase 3: Reporting and Polish

### Task 3.1: Implement Markdown Report Generator
- [x] Create `benchmarks/servers/reports.py`
- [x] Implement `generate_markdown_report()` function
- [x] Add environment information section
- [x] Add comparison tables by scenario
- [x] Add latency percentile columns
- [x] Sort servers by RPS (descending)
- [x] Handle missing results gracefully

**Validation**: Generate readable markdown from sample data ✓

### Task 3.2: Implement JSON Output
- [x] Add JSON serialization to results.py
- [x] Write results to `benchmarks/results/data.json`
- [x] Include timestamp
- [x] Include environment metadata
- [x] Include all benchmark results
- [x] Format JSON with indentation

**Validation**: JSON file is valid and complete ✓

### Task 3.3: Add Historical Results Tracking (Optional)
- [ ] Create timestamped subdirectories in results/
- [ ] Symlink `latest` to most recent run
- [ ] Optionally compare to previous run
- [ ] Highlight regressions if detected

**Validation**: Historical results preserved
**Status**: Deferred - Optional enhancement

### Task 3.4: Add Vertebra-Specific Metrics
- [ ] Add IPC latency breakdown (if measurable)
- [ ] Add native path vs ASGI path comparison
- [ ] Add worker scaling test (1, 2, 4, 8 workers)
- [ ] Report memory per worker (optional)
- [ ] Integrate with existing IPC benchmarks

**Validation**: Vertebra section in report
**Status**: Deferred - Optional enhancement

### Task 3.5: Add CI Workflow (Optional)
- [ ] Create `.github/workflows/benchmark.yml`
- [ ] Schedule weekly runs
- [ ] Upload artifacts (results/)
- [ ] Cache oha installation
- [ ] Set up Python and server dependencies

**Validation**: Workflow runs on push to branch
**Status**: Deferred - Optional enhancement

### Task 3.6: Final Documentation Polish
- [x] Update CLAUDE.md with benchmark commands
- [x] Add benchmark section to main README if appropriate
- [x] Review and clean up code comments
- [x] Ensure all docstrings are complete
- [x] Add type hints throughout

**Validation**: All documentation accurate ✓

### Task 3.7: Unit Testing
- [x] Create `src/py/tests/unit/benchmarks/` directory
- [x] Create `test_parsers.py` with 13 tests covering OhaResult and parse_oha_output()
- [x] Create `test_results.py` with 17 tests covering BenchmarkResult, EnvironmentInfo, BenchmarkSuite
- [x] Create `test_scenarios.py` with 26 tests covering Scenario and helper functions
- [x] Create `test_config.py` with 30 tests covering ServerConfig and category filtering
- [x] Create `test_reports.py` with 20 tests covering markdown and CSV report generation
- [x] All 106 tests passing
- [x] Overall coverage: 86% for tested modules (config: 100%, scenarios: 100%, reports: 95%, results: 89%, parsers: 66%)
- [x] Coverage targets met: parsers 66% (subprocess calls not tested), results 89%, scenarios 100%, config 100%, reports 95%

**Validation**: Test suite complete, 106 tests passing, 86% coverage on core modules ✓

---

## Verification Checklist

After implementation, verify:

- [x] `make bench-servers-quick` completes (requires oha installed)
- [x] Category 1: All 6 ASGI servers configured (or gracefully skipped)
- [x] Category 2: All 4 integrated runtimes configured
- [x] Category 3: All 6 frameworks configured
- [x] JSON results file created with category groupings
- [x] Markdown report with separate sections per category
- [x] No server processes left running (atexit cleanup)
- [x] Report tables properly formatted
- [x] CLI help is accurate
- [x] README documentation is complete
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
