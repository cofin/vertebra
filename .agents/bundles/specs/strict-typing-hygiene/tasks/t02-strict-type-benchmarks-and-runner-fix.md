---
type: Task
id: "strict-typing-hygiene:t02"
flow_id: strict-typing-hygiene
title: "Fix tools/benchmarks/runner.py Subprocess Bug and Enforce Strict Benchmark Typing"
description: "Fix the Popen[bytes] vs Popen[str] / str.decode bug in tools/benchmarks/runner.py:170 and bring all of tools/benchmarks/** to 100% mypy --strict and Ruff compliance."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "strict-typing-hygiene:t01"
files:
  - tools/benchmarks/runner.py
  - tools/benchmarks/apps/asgi.py
  - tools/benchmarks/apps/spikard_app.py
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
    title: "ServerRunner._process Popen[str] mismatch and stderr .read().decode() at lines 135, 149, 170"
  - id: bench-asgi
    resource: tools/benchmarks/apps/asgi.py
    title: "Benchmark ASGI app returning Any from bytes-annotated helper at line 74"
  - id: bench-spikard
    resource: tools/benchmarks/apps/spikard_app.py
    title: "Benchmark Spikard app untyped route decorators, bare dict, and Response bytes argument"
---

# Task `t02`: Fix `tools/benchmarks/runner.py` Subprocess Bug and Enforce Strict Benchmark Typing

## Objective & Scope Boundary

Resolve all `16` `mypy --strict` errors and the `ServerRunner` subprocess stderr decoding mismatch across `tools/benchmarks/runner.py`, `tools/benchmarks/apps/asgi.py`, and `tools/benchmarks/apps/spikard_app.py` per [`../spec.md` §2](../spec.md) [^bench-runner] [^bench-asgi] [^bench-spikard]. Out of scope: `src/py/tests` typing (`t03`).

## Bounded Read Manifest

- `tools/benchmarks/runner.py` — `ServerRunner.__init__`, `start`, and `wait_for_port` (`lines 125–183`) [^bench-runner].
- `tools/benchmarks/apps/asgi.py` — JSON response encoding helper (`line 74`) [^bench-asgi].
- `tools/benchmarks/apps/spikard_app.py` — optional `spikard` app routes and `Response` payload typing [^bench-spikard].
- `src/py/tests/unit/test_bench_infrastructure.py` and `test_benchmarks_cli.py` — benchmark harness unit tests.

## Contracts & Edge Cases

- `tools/benchmarks/runner.py`: change `self._process: subprocess.Popen[bytes] | None = None` (matching `subprocess.Popen` without `text=True`) and decode `raw_stderr = self._process.stderr.read()` via `raw_stderr.decode("utf-8", errors="replace").strip()` so crashed benchmark servers report their last `20` stderr lines cleanly without `Popen[str]`/`str.decode` mismatches.
- `tools/benchmarks/apps/asgi.py`: route JSON serialization through `encode_json_bytes` (returning typed `bytes`, eliminating `no-any-return`).
- `tools/benchmarks/apps/spikard_app.py`: type generic `dict[str, Any]` payloads, type route handler wrappers cleanly without `# type: ignore`, and pass `str` (`body.decode("utf-8", errors="replace")`) or the exact `Response` content type expected by `spikard`, with zero inline `#` comments across all edited files.

## Test Matrix

- Add unit regression test in `src/py/tests/unit/test_bench_infrastructure.py` exercising `ServerRunner` when a child command writes UTF-8 and non-UTF-8 bytes to `stderr` and exits non-zero before opening its port, asserting `RuntimeError` includes the decoded stderr tail; verify `mypy --strict tools/benchmarks` and `ruff check tools/benchmarks` exit `0`.

## Verification

```bash
uv run --no-sync ruff check tools/benchmarks src/py/tests/unit/test_bench_infrastructure.py src/py/tests/unit/test_benchmarks_cli.py && uv run --no-sync mypy --strict tools/benchmarks && uv run --no-sync pytest src/py/tests/unit/test_bench_infrastructure.py src/py/tests/unit/test_benchmarks_cli.py -q
```

## Acceptance Criteria

- [ ] `ServerRunner` decodes binary subprocess `stderr` cleanly on early exit and `uv run --no-sync mypy --strict tools/benchmarks` exits `0` with `0` errors (`R2`).
- [ ] Zero regressions in `test_bench_infrastructure.py` and `test_benchmarks_cli.py`.

[^bench-runner]: `tools/benchmarks/runner.py::ServerRunner` — `ServerRunner._process` `Popen[str]` mismatch and `stderr` `.read().decode()` at lines 135, 149, 170.
[^bench-asgi]: `tools/benchmarks/apps/asgi.py` — Benchmark ASGI app returning `Any` from `bytes`-annotated helper at line 74.
[^bench-spikard]: `tools/benchmarks/apps/spikard_app.py` — Benchmark Spikard app untyped route decorators, bare `dict`, and `Response` `bytes` argument.
