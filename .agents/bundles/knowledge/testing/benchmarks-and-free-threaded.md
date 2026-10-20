---
name: testing-benchmarks-and-free-threaded
description: Criterion and tools.benchmarks suites, --group bench dependency isolation, and Python 3.14t free-threaded verification.
sources:
  - Makefile
  - pyproject.toml
  - tools/benchmarks/cli.py
  - tools/benchmarks/runner.py
  - tools/benchmarks/scenarios.py
  - src/rs/vertebra-core/Cargo.toml
  - src/rs/vertebra-http/Cargo.toml
---

# Benchmark Suite & Free-Threaded Python 3.14t

Vertebra maintains both Rust Criterion microbenchmarks (`src/rs/vertebra-core/benches/`, `src/rs/vertebra-http/benches/`) and an end-to-end comparative benchmark harness in [`tools/benchmarks/`](../../../../tools/benchmarks/cli.py).

## 1. Benchmark Targets (`Makefile` & `tools.benchmarks`)

| Target | Command | Scope |
|---|---|---|
| `make bench` | `make build && uv run --group bench python -m tools.benchmarks run -t ${TARGETS:-vtb} -s ${SCENARIOS:-default}` | Builds release binaries/wheels and runs the default benchmark scenarios against Vertebra (`vtb`). |
| `make bench-all` | `make build && uv run --group bench python -m tools.benchmarks run -t all -s full` | Runs all scenarios across all targets (`vtb`, `granian`, `uvicorn`, `spikard`) and records results in `tools/benchmarks/results/`. |
| `make bench-ft` | Installs `3.14t`, syncs `.venv-ft`, builds `src/rs/target-ft` against `libpython3.14t`, and runs `-t freethreaded -s multiworker` | Benchmarks free-threaded Python `3.14t` (`Py_GIL_DISABLED=1`). |
| `make bench-report` | `uv run --group bench python -m tools.benchmarks report` | Renders the latest benchmark run summary table. |
| `make bench-diff` | `uv run --group bench python -m tools.benchmarks compare` | Compares the latest benchmark run against the previous run. |
| `cargo bench` | `cd src/rs && cargo bench` | Runs Criterion microbenchmarks (`spsc`, `mpmc`, `ring_throughput`, `shm_transport`, `spin_wait`, `codec`, `guards`, `router`, `server_throughput`). |

## 2. Isolating `--group bench` Dependencies (`pyproject.toml`)

Keep heavy C++/Rust competitor benchmark dependencies (`duckdb>=1.0`, `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.21`, `granian>=1.6`, `spikard>=0.12`) exclusively inside `[dependency-groups].bench` in [`pyproject.toml`](../../../../pyproject.toml)—never in `[dependency-groups].dev`:

- During `make py-test` or CI matrix runs on Python `3.14t`, `uv run` syncs only the `dev` group. Keeping `duckdb` in `bench` prevents `uv` from attempting a C++ source build of `duckdb` when no prebuilt `cp314t` wheel exists.
- All `make bench*` targets explicitly pass `--group bench` to `uv run`.

## 3. Separate Cargo Target Directory for `3.14t` (`src/rs/target-ft`)

PyO3 links against different CPython ABI symbols when `Py_GIL_DISABLED` is active. `make bench-ft` sets `--target-dir src/rs/target-ft` and `UV_PROJECT_ENVIRONMENT=.venv-ft` so free-threaded release artifacts never overwrite or invalidate the standard GIL-enabled build cache in `src/rs/target`.
