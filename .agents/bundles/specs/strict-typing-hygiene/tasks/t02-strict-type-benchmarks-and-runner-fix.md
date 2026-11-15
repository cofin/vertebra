---
type: Task
id: strict-typing-hygiene:t02
flow_id: strict-typing-hygiene
title: Add ServerRunner Binary-Stderr Regression Test and Keep Benchmarks Strictly Typed
description: Add the ServerRunner UTF-8/non-UTF-8 stderr regression test and keep tools/benchmarks/** mypy --strict and Ruff clean.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - strict-typing-hygiene:t01
files: []
tests:
  - src/py/tests/unit/test_bench_infrastructure.py
  - src/py/tests/unit/test_benchmarks_cli.py
read_set:
  - tools/benchmarks/runner.py
  - tools/benchmarks/apps/asgi.py
  - tools/benchmarks/apps/spikard_app.py
  - src/py/tests/unit/test_bench_infrastructure.py
  - src/py/tests/unit/test_benchmarks_cli.py
estimated_loc: 95
tags:
  - benchmarks
  - typing
  - mypy
  - bugfix
sources:
  - id: bench-runner
    resource: tools/benchmarks/runner.py::ServerRunner
    title: ServerRunner Popen[bytes] and UTF-8/non-UTF-8 stderr handling (lines 135, 170)
  - id: bench-asgi
    resource: tools/benchmarks/apps/asgi.py
    title: Benchmark ASGI app typed send/receive helpers
  - id: bench-spikard
    resource: tools/benchmarks/apps/spikard_app.py
    title: Benchmark Spikard app typed route wrappers
verification_strategy: regression_tdd
---

# Task `t02`: Add ServerRunner Binary-Stderr Regression Test and Keep Benchmarks Strictly Typed

## Objective & Scope Boundary

Verify `tools/benchmarks/{runner.py,apps/asgi.py,apps/spikard_app.py}` typing invariants (`mypy --strict tools/benchmarks` and `ruff check tools/benchmarks` pass with zero errors) and add the `ServerRunner` binary-stderr regression test in `src/py/tests/unit/test_bench_infrastructure.py` per [`../spec.md` §2](../spec.md) [^bench-runner] [^bench-asgi] [^bench-spikard]. Out of scope: `src/py/tests` typing (`t03`).

## Bounded Read Manifest

- `tools/benchmarks/runner.py` — `ServerRunner.__init__`, `start`, and `wait_for_port` (`lines 125–183`) [^bench-runner].
- `tools/benchmarks/apps/asgi.py` — typed ASGI `send_response`, `receive_body`, and `send_not_found` helpers [^bench-asgi].
- `tools/benchmarks/apps/spikard_app.py` — optional `spikard` app routes and `Response` payload typing [^bench-spikard].
- `src/py/tests/unit/test_bench_infrastructure.py` — benchmark harness unit tests (`ServerRunner` stderr test target).
- `src/py/tests/unit/test_benchmarks_cli.py` — benchmark CLI unit tests.

## Contracts & Edge Cases

- Maintain `ServerRunner._process: subprocess.Popen[bytes] | None`, typed ASGI send/receive helpers in `tools/benchmarks/apps/asgi.py`, and typed `spikard.app.App` routes in `tools/benchmarks/apps/spikard_app.py` with zero `# type: ignore` suppressions.
- Add a unit regression test in `src/py/tests/unit/test_bench_infrastructure.py` exercising `ServerRunner` when a child command writes UTF-8 and non-UTF-8 bytes (`\xff\xfe`) to `stderr` and exits non-zero before opening its port, asserting `RuntimeError` includes the decoded stderr tail without `AttributeError`.

## Test Matrix

- Unit regression test in `src/py/tests/unit/test_bench_infrastructure.py` spawning a child process writing UTF-8 and invalid UTF-8 bytes to `stderr` and exiting non-zero, verifying `ServerRunner` raises `RuntimeError` containing the decoded replacement stderr tail, plus `mypy --strict tools/benchmarks`.

## Verification

```bash
uv run --no-sync ruff check tools/benchmarks src/py/tests/unit/test_bench_infrastructure.py src/py/tests/unit/test_benchmarks_cli.py && uv run --no-sync mypy --strict tools/benchmarks && uv run --no-sync pytest src/py/tests/unit/test_bench_infrastructure.py src/py/tests/unit/test_benchmarks_cli.py -q
```

## Acceptance Criteria

- [ ] `ServerRunner` binary-stderr regression test passes in `test_bench_infrastructure.py` and `uv run --no-sync mypy --strict tools/benchmarks` exits `0` with `0` errors (`R2`).
- [ ] Zero regressions in `test_bench_infrastructure.py` and `test_benchmarks_cli.py`.

[^bench-runner]: `tools/benchmarks/runner.py::ServerRunner` — `ServerRunner` `Popen[bytes]` and `.decode(errors="replace")` stderr handling (`lines 135, 170`).
[^bench-asgi]: `tools/benchmarks/apps/asgi.py` — Benchmark ASGI app typed send/receive helpers.
[^bench-spikard]: `tools/benchmarks/apps/spikard_app.py` — Benchmark Spikard app typed route wrappers.
