---
type: Spec
flow_id: native-redis-driver
chapter_id: ch07-queue-backends-and-conformance
title: "Chapter 07: Redis/Valkey Queue Backend Cutover, Service Conformance CI, and Public Docs"
description: "Cut RedisQueueBackend and ValkeyQueueBackend off redis.asyncio onto vertebra._redis, wire redis_service + valkey_service in ci.yml and test_driver_conformance.py, and update public docs and native-stubs.md."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - queues
  - streams
  - redis
  - valkey
  - ci
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "RedisQueueBackend and ValkeyQueueBackend using redis.asyncio"
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: "Database and cache driver conformance suite"
  - id: ci-wf
    resource: .github/workflows/ci.yml
    title: "CI service containers and driver test gates"
  - id: docs-db
    resource: docs/usage/databases.md
    title: "Database and cache drivers usage guide"
  - id: docs-queues
    resource: docs/usage/queues-and-realtime.md
    title: "Queues and realtime channels usage guide"
  - id: docs-stubs
    resource: docs/reference/native-stubs.md
    title: "Native extension stub API reference"
---

# Chapter 07: Redis/Valkey Queue Backend Cutover, Service Conformance CI, and Public Docs

<!-- truth: start -->
- `RedisQueueBackend` (`vertebra-core[redis]`) and `ValkeyQueueBackend` (`vertebra-core[valkey]`) replace `redis.asyncio` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient`, accepting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` while keeping `client: RedisStreamClient | None = None` injection [^queue-redis].
- `.github/workflows/ci.yml` runs both `redis_service` (Redis `7.2+`/`8.0`) and `valkey_service` (Valkey `8.0+`/`9.0`) plus `make redis-test` and `make redis-wheel-check`, and `test_driver_conformance.py` enforces fail-loud `vertebra._redis` checks [^ci-wf] [^conformance-suite].
- `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, and `docs/reference/native-stubs.md` (`heading_level: 4`) document `vertebra-core[redis]`/`[valkey]`, Streams queues, and `Sync*`/`Async*` notification sources [^docs-db] [^docs-queues] [^docs-stubs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/framework/queues/redis.py` still imports third-party `redis.asyncio` and rejects `valkey://`, `valkeys://`, and `unix://` URLs [^queue-redis]. Cutting both queue backends onto `vertebra._redis.AsyncRedisClient` eliminates the external `redis-py` dependency and enables full CI conformance against live Redis and Valkey service containers [^ci-wf].

**Non-Goals:** OKF limitations and product manifest sync (`ch08`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Queue backends | `src/py/vertebra/framework/queues/redis.py` [^queue-redis] | `_RedisStreamAdapter` (`PY-07` compliant) over `AsyncRedisClient` |
| Conformance & CI | `test_driver_conformance.py`, `.github/workflows/ci.yml` [^conformance-suite] [^ci-wf] | `redis_service` + `valkey_service`, `make redis-test`, `make redis-wheel-check` |
| Public docs | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md` [^docs-db] [^docs-queues] [^docs-stubs] | Tabbed install snippets + `heading_level: 4` stub docs |

**Knowledge Anchors:** [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R17**: Replace `redis.asyncio` in `framework/queues/redis.py` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient` supporting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` (`t01`).
- **R18**: Add `redis_service` + `valkey_service`, `make redis-test`, and `make redis-wheel-check` to `.github/workflows/ci.yml` and enforce native `CACHE_ENGINE_SPECS` conformance in `test_driver_conformance.py` (`t02`).
- **R19a**: Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, and `docs/reference/native-stubs.md` (`heading_level: 4`) (`t03`).

```python
# src/py/vertebra/framework/queues/redis.py
class _RedisStreamAdapter:
    # Implements RedisStreamClient (xgroup_create, xadd, xreadgroup, xack, xautoclaim,
    # xrange, xdel, zadd, eval, set, get, aclose) over vertebra._redis.AsyncRedisClient
    ...
```

**Integration Flow:**
- `RedisQueueBackend(url)` probes `vertebra-core[redis]`; `ValkeyQueueBackend(url)` probes `vertebra-core[valkey]`; `client=` DI seam stays intact.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `redis.asyncio` grep | `git grep 'redis.asyncio' src/py/vertebra` | Zero matches | Third-party `redis-py` removed |
| Invalid queue URL | `memory://` or hostless `redis://` | Raises `ImproperConfigurationError` | Strict URL validation |
| `native-stubs.md` TOC | Zensical artifact verification | `heading_level: 4` keeps TOC `<= 14` and HTML `< 120_000` bytes | Docs gate green |

## 3. Documentation & OKF Knowledge Plan

Changelog, `AGENTS.md`, `product.md`, and OKF chapters complete in `ch08`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R17` | `t01` | `src/py/vertebra/framework/queues/redis.py` | `pytest src/py/tests/unit/framework/test_distributed_queues.py` |
| `R18` | `t02` | `.github/workflows/ci.yml`, `test_driver_conformance.py` | `pytest test_driver_conformance.py -k 'redis or valkey'` |
| `R19a` | `t03` | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md` | `make docs` |

## 5. Implementation Plan

- [ ] [t01-native-redis-and-valkey-queue-backends.md](tasks/t01-native-redis-and-valkey-queue-backends.md) — Implement _RedisStreamAdapter over AsyncRedisClient in framework/queues/redis.py and support valkey/valkeys/unix URLs.
- [ ] [t02-cache-conformance-and-ci-services.md](tasks/t02-cache-conformance-and-ci-services.md) — Add redis_service + valkey_service and make redis-test/redis-wheel-check to ci.yml and update test_driver_conformance.py.
- [ ] [t03-public-docs-and-native-stubs.md](tasks/t03-public-docs-and-native-stubs.md) — Document vertebra-core[redis]/[valkey], Sync*/Async* notification sources, queue URLs, and vertebra._redis stubs.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: Class names in `framework/queues/redis.py` obey `PY-07` (`_RedisStreamAdapter`, never `Native*`).
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/integration/db/test_driver_conformance.py -k 'redis or valkey'`

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database and cache driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — CI service containers and driver test gates.
[^docs-db]: `docs/usage/databases.md` — Database and cache drivers usage guide.
[^docs-queues]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
