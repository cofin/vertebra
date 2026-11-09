---
type: Guide
title: "Benchmark Suite & Free-Threaded Python 3.14t/3.15t"
description: "Criterion microbenchmarks, the tools.benchmarks harness and its CI workflow, PGO tooling, --group bench isolation, and the free-threaded Python 3.14t/3.15t build and test gate."
status: stable
tags:
  - testing
  - benchmarks
  - free-threaded
  - python
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-09T15:21:00Z"
    commit: 5dac3083
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
sources:
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Python package metadata, dependencies, and tool configuration"
  - id: src-tools-benchmarks-cli-py
    resource: tools/benchmarks/cli.py
    title: "Repository tooling module (tools/benchmarks/cli.py)"
  - id: src-tools-benchmarks-runner-py
    resource: tools/benchmarks/runner.py
    title: "Repository tooling module (tools/benchmarks/runner.py)"
  - id: src-tools-benchmarks-scenarios-py
    resource: tools/benchmarks/scenarios.py
    title: "Repository tooling module (tools/benchmarks/scenarios.py)"
  - id: src-tools-benchmarks-targets-py
    resource: tools/benchmarks/targets.py
    title: "Repository tooling module (tools/benchmarks/targets.py)"
  - id: src-tools-benchmarks-store-py
    resource: tools/benchmarks/store.py
    title: "Repository tooling module (tools/benchmarks/store.py)"
  - id: src-build-packaging-pgo-py
    resource: src/py/vertebra/build/packaging/pgo.py
    title: "Python implementation module (src/py/vertebra/build/packaging/pgo.py)"
  - id: src-tools-pgo-init-py
    resource: tools/pgo/__init__.py
    title: "Repository tooling module (tools/pgo/__init__.py)"
  - id: src-tools-pgo-rust-pgo-py
    resource: tools/pgo/rust_pgo.py
    title: "Repository tooling module (tools/pgo/rust_pgo.py)"
  - id: src-tools-pgo-python-pgo-py
    resource: tools/pgo/python_pgo.py
    title: "Repository tooling module (tools/pgo/python_pgo.py)"
  - id: src-benchmark-yml
    resource: .github/workflows/benchmark.yml
    title: "GitHub Actions workflow (benchmark.yml)"
  - id: src-tests-unit-test-freethreaded-gil-py
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Python implementation module (src/py/tests/unit/test_freethreaded_gil.py)"
  - id: src-vertebra-core-cargo-toml
    resource: src/rs/vertebra-core/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-core/Cargo.toml)"
  - id: src-vertebra-http-cargo-toml
    resource: src/rs/vertebra-http/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-http/Cargo.toml)"
---

# Benchmark Suite & Free-Threaded Python 3.14t/3.15t

<!-- truth: start -->
- `duckdb>=1.0` lives in the GIL-only `[dependency-groups].duckdb` group that `bench` includes; `uvicorn`, `granian`, and `spikard` stay in `[dependency-groups].bench`; free-threaded (`3.14t`/`3.15t`) test runs install neither group.
- Run `make bench`, `make bench-ft`, `make rs-bench`, `make bench-micro`, and `make bench-check` against release or PGO builds (`make build` / `make build-pgo`) to enforce regression gates.
<!-- truth: end -->

Vertebra has Rust Criterion microbenchmarks (`src/rs/vertebra-core/benches/`, `src/rs/vertebra-http/benches/`) and an end-to-end comparative HTTP harness in [`tools/benchmarks/`](../../../../tools/benchmarks/cli.py)[^src-tools-benchmarks-cli-py]. No throughput figures are recorded in the repository outside `tools/benchmarks/results/`.

## 1. Benchmark Targets (`Makefile` & `tools.benchmarks`)

| Target | Command | Scope |
|---|---|---|
| `make bench` | `make build`, then `uv run --group bench python -m tools.benchmarks run -t ${TARGETS:-vtb} -s ${SCENARIOS:-default} ${BENCH_ARGS}` | `default` scenarios (`hello`, `plaintext`, `body_1k`, `body_10k`, `json`, `echo`, `io_10ms`) against the Vertebra targets (embedded and SHM topologies across native, RSGI, ASGI, and WSGI). |
| `make bench-all` | `make build`, then `... run -t all -s full ${BENCH_ARGS}` | Every scenario (`full` adds `body_100k`) against all targets: `vtb-embedded-*`, `vtb-shm-*`, `vtb-ft-*`, and the `granian-asgi`, `granian-rsgi`, `uvicorn-httptools`, and `spikard` baselines. |
| `make bench-ft` | Resolves `FT_PYTHON` (`3.15t` if installed, else `3.14t`), syncs `.venv-ft`, builds `src/rs/target-ft`, then `... run -t freethreaded -s ${SCENARIOS:-multiworker}` | Free-threaded Python `3.15t`/`3.14t` (`Py_GIL_DISABLED`). `multiworker` = `hello`, `json`, `echo`, `io_10ms`. |
| `make bench-report` | `uv run --group bench python -m tools.benchmarks report` | Latest run summary. |
| `make bench-diff` | `uv run --group bench python -m tools.benchmarks compare` | Latest run against the previous run. |
| `cargo bench` | `cd src/rs && cargo bench` | Criterion: `spsc`, `mpmc`, `spin_wait`, `router`, `guards`, `ring_throughput`, `codec`, `shm_transport` (`vertebra-core`) and `server_throughput` (`vertebra-http`). |

`FREETHREADED_TARGETS` require `src/rs/target-ft/release/vertebra-server` and raise `FileNotFoundError` immediately if `make bench-ft` has not built the free-threaded binary.

## 2. Harness Layout (`tools/benchmarks/`)

- `cli.py`: `run` (`-t/--targets` `all|vtb|vtb-embedded|vtb-shm|freethreaded|baselines|<name>`, `-s` scenario preset or names, `-d/--duration` 10 s, `-c/--concurrency` 128, `-w/--warmup` 2 s, `--jit` setting `PYTHON_JIT=1`, `-o/--output`, `--note`), plus `list-scenarios`, `list-targets`, `ingest`, `compare`, `report`, and `runs`.
- `runner.py`: `OhaRunner` drives the `oha` load generator and parses its JSON output; `oha` must be on `PATH`.
- `scenarios.py`: `SCENARIOS` and the `quick`, `default`, `full`, and `multiworker` presets.
- `targets.py`: `Target` definitions grouped as `VERTEBRA_TARGETS`, `FREETHREADED_TARGETS`, and `BASELINE_TARGETS` (including `granian-asgi` and `granian-rsgi`); `apps/` holds the benchmarked Vertebra, RSGI, ASGI, WSGI, and Spikard apps.
- `store.py` and `sql/`: results go to `tools/benchmarks/results/benchmarks.jsonl` and are ingested into the DuckDB history `tools/benchmarks/results/benchmarks.duckdb`; `reports.py` renders summaries.
- `src/py/tests/unit/test_bench_infrastructure.py` covers the harness.

## 3. Scheduled Benchmarks (`.github/workflows/benchmark.yml`)

The workflow runs on `workflow_dispatch` (input `full_benchmark`), on published releases, and weekly (`cron: '0 6 * * 1'`). It installs `oha` with `cargo install oha`, runs `make install` and `make build`, then `make bench` (or `make bench-all` when `full_benchmark` is true), uploads `tools/benchmarks/results/` with 90-day retention, and on `main` commits `tools/benchmarks/results/*.duckdb`.

## 4. Profile-Guided Optimization (`build/packaging/pgo.py`, `tools/pgo/`)

- [`pgo.py`](../../../../src/py/vertebra/build/packaging/pgo.py)[^src-build-packaging-pgo-py] & [`rust_pgo.py`](../../../../tools/pgo/rust_pgo.py)[^src-tools-pgo-rust-pgo-py]: `RustPGOConfig` (`PGOMode` `generate | use | baseline`), `EXTENSION_MANIFESTS`, `PGOPipelineResult`, `resolve_cargo_release_dir` (`CARGO_TARGET_DIR`), `build_instrumented`, `build_instrumented_extension`, `build_instrumented_extensions`, `run_default_pgo_workload`, `merge_profiles`, `weighted_merge_profiles`, `find_llvm_profdata`, `validate_profdata`, `check_profdata_size`, `build_optimized`, `build_optimized_extension`, `build_optimized_extensions`, and `run_pgo_pipeline` (`python -m tools.pgo.rust_pgo`).
- [`python_pgo.py`](../../../../tools/pgo/python_pgo.py)[^src-tools-pgo-python-pgo-py]: `PythonBuildInfo`, `PlatformSupport`, `PLATFORM_MATRIX`, `detect_python_build`, `check_pgo_availability`, and `get_platform_support`.

`make build-pgo` invokes `uv run python -m tools.pgo.rust_pgo` for two-phase instrumented and profile-use release builds across `vertebra-http` and all five PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`), or `PROFDATA=<path> make build-pgo` to reuse an existing profile. `src/py/tests/unit/build/test_packaging_and_pgo.py` exercises both modules, and `vtb build [--server-binary <path>]` bundles a pre-built `bin/vertebra-server` into standalone and onefile artifacts.

## 5. Isolating `--group bench` and `--group duckdb` Dependencies (`pyproject.toml`)

Keep the benchmark-only dependencies (the `duckdb>=1.0` results store reaching `bench` through `{ include-group = "duckdb" }`, and the `uvicorn[standard]>=0.30`, `httptools>=0.6`, `uvloop>=0.23.0; sys_platform != 'win32'`, `granian>=1.6`, and `spikard>=0.12` baselines) in `[dependency-groups].bench` in [`pyproject.toml`](../../../../pyproject.toml)[^src-pyproject-toml], never in `dev`:

- `pyproject.toml` sets no `[tool.uv] default-groups`; `make py-test` and `make coverage` run `uv run $(PY_TEST_GROUPS)`, so GIL interpreters sync `dev` plus `duckdb` and the CI `3.14t` / `3.15t` jobs sync `dev` only, where DuckDB tests skip through `pytest.importorskip("duckdb")`.
- Every `make bench*` target passes `--group bench` to `uv run`.

## 6. Free-Threaded Build (`make bench-ft`)

PyO3 links different CPython ABI symbols when `Py_GIL_DISABLED` is set, so free-threaded artifacts use their own environment and target directory:

1. `UV_PROJECT_ENVIRONMENT=.venv-ft uv sync --python "$FT_PYTHON" --no-dev` builds `.venv-ft` (selecting `3.15t` when installed, else `3.14t`).
2. `PYO3_PYTHON=$(uv python find "$FT_PYTHON") cargo build --manifest-path src/rs/Cargo.toml --release --target-dir src/rs/target-ft` builds the workspace against `libpython3.15t` / `libpython3.14t` without touching `src/rs/target`.
3. The harness runs from the default `.venv` (`--group bench`) and launches `src/rs/target-ft/release/vertebra-server` with `.venv-ft` site-packages injected ahead of the repo root through `PYTHONPATH` and `LD_LIBRARY_PATH` (`_ft_env` in `targets.py`).

`make clean` removes both `src/rs/target` and `src/rs/target-ft`.

## 7. Free-Threaded Test Gate

- The CI `test-python` matrix includes `3.14t` and `3.15t` and runs `make py-test` on both.
- `_vertebra`, `_bundler`, `_storage`, `_bigquery`, and `_spanner` declare `#[pymodule(gil_used = false)]`.
- [`test_freethreaded_gil.py`](../../../../src/py/tests/unit/test_freethreaded_gil.py)[^src-tests-unit-test-freethreaded-gil-py] asserts `GIL_USED` is `False` across `_vertebra`, `_bundler`, `_storage`, `_bigquery`, and `_spanner` and that `sys._is_gil_enabled()` stays false after import.
- `unit/server/test_stage1_hotpath_and_314t.py` and `unit/server/test_runtime_tuning.py` cover the free-threaded dispatch, loop backend selection, and `EventLoopPool` paths; `GilAuditReport` (`server/_gil_audit.py`) reports native extensions that re-enable the GIL.

Provenance sources: [^src-makefile], [^src-tools-benchmarks-runner-py], [^src-tools-benchmarks-scenarios-py], [^src-tools-benchmarks-targets-py], [^src-tools-benchmarks-store-py], [^src-tools-pgo-init-py], [^src-benchmark-yml], [^src-vertebra-core-cargo-toml], [^src-vertebra-http-cargo-toml].

[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-tools-benchmarks-cli-py]: `tools/benchmarks/cli.py` — Repository tooling module (tools/benchmarks/cli.py).
[^src-tools-benchmarks-runner-py]: `tools/benchmarks/runner.py` — Repository tooling module (tools/benchmarks/runner.py).
[^src-tools-benchmarks-scenarios-py]: `tools/benchmarks/scenarios.py` — Repository tooling module (tools/benchmarks/scenarios.py).
[^src-tools-benchmarks-targets-py]: `tools/benchmarks/targets.py` — Repository tooling module (tools/benchmarks/targets.py).
[^src-tools-benchmarks-store-py]: `tools/benchmarks/store.py` — Repository tooling module (tools/benchmarks/store.py).
[^src-build-packaging-pgo-py]: `src/py/vertebra/build/packaging/pgo.py` — Python implementation module (src/py/vertebra/build/packaging/pgo.py).
[^src-tools-pgo-init-py]: `tools/pgo/__init__.py` — Repository tooling module (tools/pgo/__init__.py).
[^src-tools-pgo-rust-pgo-py]: `tools/pgo/rust_pgo.py` — Repository tooling module (tools/pgo/rust_pgo.py).
[^src-tools-pgo-python-pgo-py]: `tools/pgo/python_pgo.py` — Repository tooling module (tools/pgo/python_pgo.py).
[^src-benchmark-yml]: `.github/workflows/benchmark.yml` — GitHub Actions workflow (benchmark.yml).
[^src-tests-unit-test-freethreaded-gil-py]: `src/py/tests/unit/test_freethreaded_gil.py` — Python implementation module (src/py/tests/unit/test_freethreaded_gil.py).
[^src-vertebra-core-cargo-toml]: `src/rs/vertebra-core/Cargo.toml` — Rust crate manifest (src/rs/vertebra-core/Cargo.toml).
[^src-vertebra-http-cargo-toml]: `src/rs/vertebra-http/Cargo.toml` — Rust crate manifest (src/rs/vertebra-http/Cargo.toml).
