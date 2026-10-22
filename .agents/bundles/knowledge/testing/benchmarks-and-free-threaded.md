---
type: Guide
title: "Benchmark Suite & Free-Threaded Python 3.14t"
description: "Criterion microbenchmarks, the tools.benchmarks harness and its CI workflow, PGO tooling, --group bench isolation, and the free-threaded Python 3.14t build and test gate."
status: stable
tags: [testing, benchmarks, free-threaded, python, rust]
sources:
  - resource: Makefile
  - resource: pyproject.toml
  - resource: tools/benchmarks/cli.py
  - resource: tools/benchmarks/runner.py
  - resource: tools/benchmarks/scenarios.py
  - resource: tools/benchmarks/targets.py
  - resource: tools/benchmarks/store.py
  - resource: tools/pgo/rust_pgo.py
  - resource: tools/pgo/python_pgo.py
  - resource: .github/workflows/benchmark.yml
  - resource: src/py/tests/unit/test_freethreaded_gil.py
  - resource: src/rs/vertebra-core/Cargo.toml
  - resource: src/rs/vertebra-http/Cargo.toml
---

# Benchmark Suite & Free-Threaded Python 3.14t

Vertebra has Rust Criterion microbenchmarks (`src/rs/vertebra-core/benches/`, `src/rs/vertebra-http/benches/`) and an end-to-end comparative HTTP harness in [`tools/benchmarks/`](../../../../tools/benchmarks/cli.py). No throughput figures are recorded in the repository outside `tools/benchmarks/results/`.

## 1. Benchmark Targets (`Makefile` & `tools.benchmarks`)

| Target | Command | Scope |
|---|---|---|
| `make bench` | `make build`, then `uv run --group bench python -m tools.benchmarks run -t ${TARGETS:-vtb} -s ${SCENARIOS:-default} ${BENCH_ARGS}` | `default` scenarios (`hello`, `plaintext`, `body_1k`, `body_10k`, `json`, `echo`, `io_10ms`) against the eight Vertebra targets (embedded and SHM topologies; native, ASGI, and WSGI). |
| `make bench-all` | `make build`, then `... run -t all -s full ${BENCH_ARGS}` | Every scenario (`full` adds `body_100k`) against all targets: the eight `vtb-*` targets, `vtb-ft-native-4w`, and the `granian-asgi`, `uvicorn-httptools`, and `spikard` baselines. |
| `make bench-ft` | Installs `3.14t`, syncs `.venv-ft`, builds `src/rs/target-ft`, then `... run -t freethreaded -s ${SCENARIOS:-multiworker}` | Free-threaded Python `3.14t` (an interpreter built with `Py_GIL_DISABLED`). `multiworker` = `hello`, `json`, `echo`, `io_10ms`. |
| `make bench-report` | `uv run --group bench python -m tools.benchmarks report` | Latest run summary. |
| `make bench-diff` | `uv run --group bench python -m tools.benchmarks compare` | Latest run against the previous run. |
| `cargo bench` | `cd src/rs && cargo bench` | Criterion: `spsc`, `mpmc`, `spin_wait`, `router`, `guards`, `ring_throughput`, `codec`, `shm_transport` (`vertebra-core`) and `server_throughput` (`vertebra-http`). |

`vtb-ft-native-4w` needs `src/rs/target-ft/release/vertebra-server`. Without a prior `make bench-ft` build, the target's command falls back to an `echo` placeholder instead of failing.

## 2. Harness Layout (`tools/benchmarks/`)

- `cli.py`: `run` (`-t/--targets` `all|vtb|baselines|<name>`, `-s` scenario preset or names, `-d/--duration` 10 s, `-c/--concurrency` 128, `-w/--warmup` 2 s, `-o/--output`, `--note`), plus `list-scenarios`, `list-targets`, `ingest`, `compare`, `report`, and `runs`.
- `runner.py`: `OhaRunner` drives the `oha` load generator and parses its JSON output; `oha` must be on `PATH`.
- `scenarios.py`: `SCENARIOS` and the `quick`, `default`, `full`, and `multiworker` presets.
- `targets.py`: `Target` definitions grouped as `VERTEBRA_TARGETS`, `FREETHREADED_TARGETS`, and `BASELINE_TARGETS`; `apps/` holds the benchmarked Vertebra, ASGI, WSGI, and Spikard apps.
- `store.py` and `sql/`: results go to `tools/benchmarks/results/benchmarks.jsonl` and are ingested into the DuckDB history `tools/benchmarks/results/benchmarks.duckdb`; `reports.py` renders summaries.
- `src/py/tests/unit/test_bench_infrastructure.py` covers the harness.

## 3. Scheduled Benchmarks (`.github/workflows/benchmark.yml`)

The workflow runs on `workflow_dispatch` (input `full_benchmark`), on published releases, and weekly (`cron: '0 6 * * 1'`). It installs `oha` with `cargo install oha`, runs `make install` and `make build`, then `make bench` (or `make bench-all` when `full_benchmark` is true), uploads `tools/benchmarks/results/` with 90-day retention, and on `main` commits `tools/benchmarks/results/*.duckdb`.

## 4. Profile-Guided Optimization (`tools/pgo/`)

- [`rust_pgo.py`](../../../../tools/pgo/rust_pgo.py): `RustPgoConfig` (`PgoMode` `generate | use | baseline`), `build_instrumented`, `build_optimized`, `build_instrumented_extension`, `build_optimized_extension`, `merge_profiles`, `weighted_merge_profiles`, `find_llvm_profdata`, `validate_profdata`, and `check_profdata_size`.
- [`python_pgo.py`](../../../../tools/pgo/python_pgo.py): `PythonBuildInfo`, `PlatformSupport`, `PLATFORM_MATRIX`, `detect_python_build`, `check_pgo_availability`, and `get_platform_support`.

No `Makefile` target calls these modules. `src/py/tests/unit/build/test_packaging_and_pgo.py` exercises them, and `vtb build --pgo` records PGO metadata during packaging.

## 5. Isolating `--group bench` Dependencies (`pyproject.toml`)

Keep the benchmark-only dependencies (the `duckdb>=1.0` results store and the `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.21`, `granian>=1.6`, and `spikard>=0.12` baselines) in `[dependency-groups].bench` in [`pyproject.toml`](../../../../pyproject.toml), never in `dev`:

- `pyproject.toml` sets no `[tool.uv] default-groups`, so `uv run` during `make py-test` and the CI `3.14t` job syncs only `dev`. Keeping `duckdb` out of `dev` prevents a source build of `duckdb` when no `cp314t` wheel exists.
- Every `make bench*` target passes `--group bench` to `uv run`.

## 6. Free-Threaded Build (`make bench-ft`)

PyO3 links different CPython ABI symbols when `Py_GIL_DISABLED` is set, so free-threaded artifacts use their own environment and target directory:

1. `UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python 3.14t --no-dev --extra framework` builds `.venv-ft`.
2. `PYO3_PYTHON=$(uv python find 3.14t) cargo build --manifest-path src/rs/Cargo.toml --release --target-dir src/rs/target-ft` builds the workspace against `libpython3.14t` without touching `src/rs/target`.
3. The harness runs from the default `.venv` (`--group bench`) and launches `src/rs/target-ft/release/vertebra-server` with `.venv-ft` site-packages injected through `PYTHONPATH` and `LD_LIBRARY_PATH` (`_ft_env` in `targets.py`).

`make clean` removes both `src/rs/target` and `src/rs/target-ft`.

## 7. Free-Threaded Test Gate

- The CI `test-python` matrix includes `3.14t` and runs `make py-test` there.
- `_vertebra`, `_bundler`, and `_storage` declare `#[pymodule(gil_used = false)]`.
- [`test_freethreaded_gil.py`](../../../../src/py/tests/unit/test_freethreaded_gil.py) asserts `_vertebra.GIL_USED`, `_bundler.GIL_USED`, and `_storage.GIL_USED` are `False` and that `sys._is_gil_enabled()` stays false after import.
- `unit/server/test_hotpath_and_314t.py` and `unit/server/test_loops.py` cover the free-threaded dispatch and `EventLoopPool` paths; `GilAuditor` (`server/_gil_audit.py`) reports native extensions that re-enable the GIL.
