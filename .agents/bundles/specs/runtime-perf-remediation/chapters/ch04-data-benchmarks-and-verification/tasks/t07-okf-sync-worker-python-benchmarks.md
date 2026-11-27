---
type: Task
id: "runtime-perf-remediation:ch04:t07"
chapter_id: ch04-data-benchmarks-and-verification
title: Sync worker, Python hot-path, and benchmark OKF chapters
description: Update asgi-and-wsgi-runners.md, hot-path-and-async-dispatch.md, and benchmarks-and-free-threaded.md in present-tense OKF v0.2 format matching verified ch01, ch03, and ch04 implementations.
status: stable
state: pending
priority: P2
verification_strategy: documentation_validation
worksheet_format: compact
depends_on:
  - "runtime-perf-remediation:ch04:t05"
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/spec.md
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
  - .agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
files:
  - .agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md
  - .agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
tests: []
estimated_loc: 150
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root specification documentation plan for worker, Python, and benchmark chapters
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 OKF knowledge sync and benchmark matrix
tags:
  - okf
  - docs
  - wsgi
  - python
  - benchmarks
---

# Sync Worker, Python Hot-Path, and Benchmark OKF Chapters

## Objective & Scope Boundary

Rewrite the worker runners, Python hot-path dispatch, and benchmark harness OKF knowledge chapters in present tense to match the verified implementations from `ch01`, `ch03`, and `ch04` [^src-root-spec] [^src-ch04-spec].

Preserve strict OKF v0.2 formatting and provenance across all three chapters.

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/spec.md`
- `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md`
- `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`

## Contracts & Edge Cases

- `asgi-and-wsgi-runners.md`: document adaptive `WsgiExecutor` (`VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`, verified stall threshold), shared `WsgiServerConfig`, lazy `RsgiHttpProtocol`, shared `RsgiScope` metadata, and `AsgiScope` populated-slot bitmask drop.
- `hot-path-and-async-dispatch.md`: document split `_on_notify_readable`/`_drain_batches` with 1-turn grace check, single-pass `native_only` mixed sync/async dispatch, direct `Request::body`, and specialized 1-arg simple extractors in `compile_handler_spec`.
- `benchmarks-and-free-threaded.md`: document `DataBackend` SQLite and PostgreSQL backends across `native`, `sqlspec`, `advanced_alchemy`, and `stdlib` (`VTB_BENCH_DB_ADAPTER`, `VTB_BENCH_DB_ADAPTER_FLAVOR`), `capture_advanced_alchemy_report`, new SQLSpec/AA HTTP data targets, and live Granian/SQLite/PostgreSQL verification commands.
- Keep every `<!-- truth: start --> ... <!-- truth: end -->` block at `<= 40` lines, match every frontmatter `sources:` entry 1:1 with `[^id]:` footnotes, and omit standalone `---` rules.

## Test Matrix

| Chapter | Required Coverage | OKF Invariant |
| :--- | :--- | :--- |
| `asgi-and-wsgi-runners.md` | `WsgiExecutor`, `WsgiServerConfig`, lazy `RsgiHttpProtocol`, `AsgiScope` | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |
| `hot-path-and-async-dispatch.md` | `_on_notify_readable`/`_drain_batches`, `native_only`, `Request::body`, 1-arg extractors | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |
| `benchmarks-and-free-threaded.md` | SQLite/Postgres `native`/`sqlspec`/`advanced_alchemy`, AA comparator, live gates | `<= 40` line truth block, 1:1 `sources`/`[^id]:`, zero body `---` |

## Verification

```bash
make review-check
```

## Acceptance Criteria

- All three OKF chapters reflect current WSGI/ASGI/RSGI runners, Python hot-path dispatch, and SQLite/PostgreSQL benchmark capabilities in present tense.
- Truth blocks, frontmatter `sources`, `[^id]:` footnotes, and relative links pass validation cleanly.

[^src-root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root specification documentation plan for worker, Python, and benchmark chapters.
[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 OKF knowledge sync and benchmark matrix.
