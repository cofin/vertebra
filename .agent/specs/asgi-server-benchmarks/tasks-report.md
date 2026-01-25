# Task Checklist Report: asgi-server-benchmarks

- Generated: 2026-01-24
- Source: `specs/active/asgi-server-benchmarks/tasks.md`
- Total tasks: 182
- Done: 162
- Pending: 20

## Checklist

# ASGI Server Benchmarks - Implementation Tasks

## Overview


## Benchmark Categories



## Phase 1: Infrastructure (Core Framework)

### Task 1.1: Create Directory Structure
- [x] Create `benchmarks/servers/` package
- [x] Create `benchmarks/servers/__init__.py`
- [x] Create `benchmarks/servers/app/` directory
- [x] Create `benchmarks/servers/app/__init__.py`
- [x] Create `benchmarks/servers/app/assets/` directory
- [x] Create `benchmarks/results/.gitkeep`


### Task 1.2: Implement Categorized Configurations
- [x] Create `benchmarks/servers/config.py`
- [x] Define `ServerConfig` and `FrameworkConfig` dataclasses
- [x] Define `BenchmarkCategory` enum (SERVER, RUNTIME, FRAMEWORK)

- [x] Uvicorn (h11) - port 8002
- [x] Uvicorn (httptools) - port 8003
- [x] Granian - port 8004
- [x] Hypercorn - port 8005
- [x] Daphne - port 8006
- [x] Socketify - port 8007

- [x] Vertebra - port 8001
- [x] Robyn - port 8010
- [x] Sanic (standalone) - port 8011
- [x] Spikard - port 8012

- [x] Raw ASGI (baseline)
- [x] Starlette
- [x] FastAPI
- [x] BlackSheep
- [x] Litestar
- [x] Sanic (ASGI mode)


### Task 1.3: Implement oha Parser
- [x] Create `benchmarks/servers/parsers.py`
- [x] Define `OhaResult` dataclass
- [x] Implement `run_oha()` function
- [x] Parse JSON output from oha
- [x] Convert latency units (seconds to milliseconds)
- [x] Handle oha command failures gracefully


### Task 1.4: Implement Result Dataclasses
- [x] Create `benchmarks/servers/results.py`
- [x] Define `BenchmarkResult` dataclass
- [x] Define `EnvironmentInfo` dataclass
- [x] Define `BenchmarkSuite` dataclass
- [x] Implement JSON serialization
- [x] Add result validation (sanity checks)


### Task 1.5: Implement Utilities
- [x] Create `benchmarks/servers/utils.py`
- [x] Implement `start_server()` with health check
- [x] Implement `stop_server()` with cleanup
- [x] Implement `wait_for_health()` function
- [x] Implement `detect_oha_version()` function
- [x] Implement `detect_environment()` function


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


### Task 1.7: Create Test Applications (Category 2 - Native Runtimes)
- [x] Create `benchmarks/servers/app/robyn_app.py` for Robyn
- [x] Create `benchmarks/servers/app/sanic_app.py` for Sanic standalone
- [x] Create `benchmarks/servers/app/spikard_app.py` for Spikard
- [x] Each app mirrors the same endpoints as raw ASGI


### Task 1.8: Create Test Applications (Category 3 - Frameworks)
- [x] Create `benchmarks/servers/app/starlette_app.py` for Starlette
- [x] Create `benchmarks/servers/app/fastapi_app.py` for FastAPI
- [x] Create `benchmarks/servers/app/blacksheep_app.py` for BlackSheep
- [x] Create `benchmarks/servers/app/litestar_app.py` for Litestar
- [x] Sanic uses `sanic_app.py` for both standalone and ASGI modes
- [x] Each app mirrors the same endpoints as raw ASGI


### Task 1.9: Cross-Server Framework Testing (Added)
- [x] Add `litestar_uvicorn` - Litestar on Uvicorn httptools
- [x] Add `litestar_granian` - Litestar on Granian Rust runtime
- [x] Add `vertebra_asgi_litestar` - Litestar through Vertebra ASGI bridge
- [x] Add `vertebra_native_litestar` - Litestar with Vertebra native Rust router
- [x] Add `fastapi_uvicorn` - FastAPI on Uvicorn httptools
- [x] Add `fastapi_granian` - FastAPI on Granian Rust runtime
- [x] Add `vertebra_fastapi` - FastAPI through Vertebra ASGI bridge


### Task 1.10: Isolated Benchmark Environment (Added)
- [x] Create `benchmarks/servers/venv_manager.py` for isolated venv
- [x] Add `benchmarks/.venv/` to `.gitignore`
- [x] Update `utils.py` with `use_bench_venv` parameter
- [x] Add `--no-venv` CLI flag to runner
- [x] Add `make bench-setup` and `make bench-clean` targets
- [x] Update README with isolated venv documentation



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


### Task 2.2: Implement Benchmark Orchestrator
- [x] Create `benchmarks/servers/runner.py`
- [x] Implement main entry point
- [x] Implement server iteration loop
- [x] Implement scenario iteration loop
- [x] Implement three-phase execution (primer/warmup/measure)
- [x] Add progress output
- [x] Handle keyboard interrupt gracefully


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


### Task 2.4: Add Makefile Targets
- [x] Add `bench-servers` target
- [x] Add `bench-servers-quick` target
- [x] Add `bench-vertebra` target
- [x] Add `bench-install-deps` target
- [x] Add oha installation check


### Task 2.5: Create README Documentation
- [x] Create `benchmarks/servers/README.md`
- [x] Document installation requirements
- [x] Document usage examples
- [x] Document available servers
- [x] Document available scenarios
- [x] Document CLI options
- [x] Add troubleshooting section



## Phase 3: Reporting and Polish

### Task 3.1: Implement Markdown Report Generator
- [x] Create `benchmarks/servers/reports.py`
- [x] Implement `generate_markdown_report()` function
- [x] Add environment information section
- [x] Add comparison tables by scenario
- [x] Add latency percentile columns
- [x] Sort servers by RPS (descending)
- [x] Handle missing results gracefully


### Task 3.2: Implement JSON Output
- [x] Add JSON serialization to results.py
- [x] Write results to `benchmarks/results/data.json`
- [x] Include timestamp
- [x] Include environment metadata
- [x] Include all benchmark results
- [x] Format JSON with indentation


### Task 3.3: Add Historical Results Tracking (Optional)
- [x] Create timestamped subdirectories in results/
- [ ] Symlink `latest` to most recent run
- [x] Optionally compare to previous run
- [x] Highlight regressions if detected


### Task 3.4: Add Vertebra-Specific Metrics
- [ ] Add IPC latency breakdown (if measurable)
- [ ] Add native path vs ASGI path comparison
- [ ] Add worker scaling test (1, 2, 4, 8 workers)
- [ ] Report memory per worker (optional)
- [ ] Integrate with existing IPC benchmarks


### Task 3.5: Add CI Workflow (Optional)
- [x] Create `.github/workflows/benchmark.yml`
- [x] Schedule weekly runs
- [x] Upload artifacts (results/)
- [x] Cache oha installation
- [x] Set up Python and server dependencies


### Task 3.6: Final Documentation Polish
- [x] Update CLAUDE.md with benchmark commands
- [x] Add benchmark section to main README if appropriate
- [x] Review and clean up code comments
- [x] Ensure all docstrings are complete
- [x] Add type hints throughout


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



## Verification Checklist


- [x] `make bench-servers-quick` completes (requires oha installed)
- [x] Category 1: All 6 ASGI servers configured (or gracefully skipped)
- [x] Category 2: All 7 integrated runtimes configured (vertebra, vertebra_asgi_litestar, vertebra_native_litestar, vertebra_fastapi, robyn, sanic_standalone, spikard)
- [x] Category 3: All 8 frameworks configured (raw_asgi, starlette, fastapi_uvicorn, fastapi_granian, blacksheep, litestar_uvicorn, litestar_granian, sanic_asgi)
- [x] JSON results file created with category groupings
- [x] Markdown report with separate sections per category
- [x] No server processes left running (atexit cleanup)
- [x] Report tables properly formatted
- [x] CLI help is accurate
- [x] README documentation is complete
- [x] No lint errors (`ruff check benchmarks/`)
- [x] Vertebra included in Category 2 comparisons
- [x] Cross-server Litestar comparison (Uvicorn, Granian, Vertebra ASGI, Vertebra Native)
- [x] Cross-server FastAPI comparison (Uvicorn, Granian, Vertebra)
- [x] Isolated benchmark venv to avoid dependency pollution
- [x] oha binary detection for ~/.cargo/bin/


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


## Notes
