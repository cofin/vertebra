---
type: Spec
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: 'Chapter 04: Native Redis/Valkey Queue Backends, Conformance CI, and Docs/OKF Synchronization'
description: Cut RedisQueueBackend and ValkeyQueueBackend over to vertebra._redis Streams/ZSETs, wire Redis 8.0 and Valkey 9.0 conformance in CI, and synchronize public docs, changelog, OKF chapters, and review skills.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- queues
- streams
- redis
- valkey
- ci
- docs
- okf
- limitations
sources:
- id: queue-redis
  resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
  title: RedisQueueBackend and ValkeyQueueBackend using redis.asyncio
- id: conformance-suite
  resource: src/py/tests/integration/db/test_driver_conformance.py
  title: Database and cache driver conformance suite
- id: ci-wf
  resource: .github/workflows/ci.yml
  title: CI service containers and driver test gates
- id: docs-db
  resource: docs/usage/databases.md
  title: Database and cache drivers usage guide
- id: docs-queues
  resource: docs/usage/queues-and-realtime.md
  title: Queues and realtime channels usage guide
- id: docs-stubs
  resource: docs/reference/native-stubs.md
  title: Native extension stub API reference
- id: docs-changelog
  resource: docs/changelog/index.md
  title: Release changelog
- id: agents-md
  resource: AGENTS.md
  title: Repository agent instructions and Makefile commands
- id: product-md
  resource: .agents/bundles/product/product.md
  title: OKF product definition and crate capability table
- id: okf-drivers
  resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  title: Database drivers and dialers OKF chapter
- id: okf-queues
  resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  title: Distributed queues and email OKF chapter
- id: review-pyo3-skill
  resource: .agents/skills/review-pyo3-bridge/SKILL.md
  title: PyO3 bridge review skill
- id: domains-idx
  resource: .agents/bundles/knowledge/domains/index.md
  title: OKF domains catalog index
- id: okf-workflow
  resource: .agents/bundles/knowledge/workflow.md
  title: Project Makefile and verification workflow chapter
---

# Chapter 04: Native Redis/Valkey Queue Backends, Conformance CI, and Docs/OKF Synchronization

<!-- truth: start -->
- `RedisQueueBackend` (`vertebra-core[redis]`) and `ValkeyQueueBackend` (`vertebra-core[valkey]`) replace `redis.asyncio` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient`, accepting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` while keeping `client: RedisStreamClient | None = None` injection [^queue-redis].
- `.github/workflows/ci.yml` runs both `redis_service` (Redis `7.2+`/`8.0`) and `valkey_service` (Valkey `8.0+`/`9.0`) plus `make redis-test` and `make redis-wheel-check`, and `test_driver_conformance.py` enforces fail-loud `vertebra._redis` checks [^ci-wf] [^conformance-suite].
- `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, and `docs/reference/native-stubs.md` (`heading_level: 4`) document `vertebra-core[redis]`/`[valkey]`, Streams queues, and `Sync*`/`Async*` notification sources [^docs-db] [^docs-queues] [^docs-stubs].
- `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` record `vertebra-redis` (`[redis]`/`[valkey]`), `SyncNotificationSource`/`AsyncNotificationSource`, and removal of `vertebra_db::resp3` [^docs-changelog] [^agents-md] [^product-md].
- `redis-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, and `distributed-queues-and-email.md` capture present-tense Redis/Valkey architecture and operational caveats [^okf-drivers] [^okf-queues].
- `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` list `vertebra-redis`, `redis-limitations.md`, `make redis-test`, and `make redis-wheel-check` [^review-pyo3-skill] [^domains-idx] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/framework/queues/redis.py` still imports third-party `redis.asyncio` and rejects `valkey://`, `valkeys://`, and `unix://` URLs [^queue-redis]. Cutting both queue backends onto `vertebra._redis.AsyncRedisClient` eliminates the external `redis-py` dependency and enables full CI conformance against live Redis and Valkey service containers [^ci-wf].

**Non-Goals:** OKF limitations and product manifest sync (`ch04:t04`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Queue backends | `src/py/vertebra/framework/queues/redis.py` [^queue-redis] | `_RedisStreamAdapter` (`PY-07` compliant) over `AsyncRedisClient` |
| Conformance & CI | `test_driver_conformance.py`, `.github/workflows/ci.yml` [^conformance-suite] [^ci-wf] | `redis_service` + `valkey_service`, `make redis-test`, `make redis-wheel-check` |
| Public docs | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md` [^docs-db] [^docs-queues] [^docs-stubs] | Tabbed install snippets + `heading_level: 4` stub docs |

**Knowledge Anchors:** [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

Removing `vertebra_db::resp3` changes the `vertebra-db` capability row in `.agents/bundles/product/product.md` [^product-md], and adding `vertebra-redis` + `make redis-test` / `make redis-wheel-check` requires synchronized updates across `AGENTS.md`, `workflow.md`, `review-pyo3-bridge/SKILL.md`, and a dedicated `redis-limitations.md` OKF chapter (`<= 200` lines) [^agents-md] [^okf-workflow].

**Non-Goals:** Distributed channel extensions owned by `queues-tasks-and-channels`.

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Changelog & product | `docs/changelog/index.md`, `AGENTS.md`, `product/product.md` [^docs-changelog] [^agents-md] [^product-md] | Release notes, crate inventory, and `vertebra-db` table accuracy |
| OKF chapters | `knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md` [^okf-drivers] [^okf-queues] | Present-tense OKF chapters (`<= 200` lines each) |
| Skills & indexes | `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` [^review-pyo3-skill] [^domains-idx] [^okf-workflow] | `DOC-08` `Makefile` parity and OKF index links |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Project Workflow](../../../../knowledge/workflow.md).

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

- **R19b**: Update `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` (`t01`).
- **R19c**: Author `.agents/bundles/knowledge/domains/data/redis-limitations.md` (`<= 200` lines) and update `database-drivers-and-dialers.md` and `distributed-queues-and-email.md` (`t02`).
- **R19d**: Update `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, and `.agents/bundles/knowledge/workflow.md` (`t03`).

```text
redis-limitations.md required caveats (<= 200 lines):
1. Dedicated socket per SyncRedisListener / AsyncRedisListener handle (subscribed sockets are isolated from multiplexed command pipelines).
2. Multi-key pipelines and Lua scripts on Redis/Valkey Cluster require matching {hash_tag} keys to avoid CROSSSLOT ProgrammingError.
3. Server-assisted CLIENT TRACKING invalidation requires RESP3 (protocol=3) on Redis 7.2+/8.0 or Valkey 8.0+/9.0.
4. unix:// sockets apply strictly to standalone single-node deployments, never Cluster or Sentinel TCP topologies.
```

**Integration Flow:**
- `make review-check && make docs` validates every link, `sources:` path, line budget (`<= 200`), and `Makefile` target.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Stale `product.md` row | `parse_resp3_frame` left in `vertebra-db` table | Updated to `vertebra-redis` in `t01` | `DOC-02` / `DOC-04` truth |
| Skill/OKF line cap | File exceeds `200` lines | Kept concise and `<= 200` lines | `ReviewSetupValidator` budget |

## 3. Documentation & OKF Knowledge Plan

Changelog, `AGENTS.md`, `product.md`, and OKF chapters complete in `ch04:t04`.

This chapter completes `[12/12]` documentation, skill, product, and OKF deliverables for `native-redis-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut RedisQueueBackend and ValkeyQueueBackend Over to vertebra._redis Streams and ZSETs | `src/py/vertebra/framework/queues/redis.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t02` | Wire Redis 8.0 and Valkey 9.0 Live Conformance and CI Services | `.github/workflows/ci.yml` | `src/py/tests/integration/db/test_driver_conformance.py` |
| `t03` | Update Public Databases/Installation Docs, Native Stubs Test, Changelog, and Tech Stack | `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (+3 more) | `make review-check` |
| `t04` | Author OKF redis-limitations.md and Synchronize Data/Framework Chapters and Review Skills | `.agents/bundles/knowledge/domains/data/redis-limitations.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` (+3 more) | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-native-redis-and-valkey-queue-backends.md](tasks/t01-native-redis-and-valkey-queue-backends.md) — Wire RedisQueueBackend and ValkeyQueueBackend onto native vertebra._redis stream consumer groups and delayed ZSETs.
- [ ] [t02-cache-conformance-and-ci-services.md](tasks/t02-cache-conformance-and-ci-services.md) — Register redis and valkey native_extension_module in conformance registry/gate and add Redis 8.0 and Valkey 9.0 CI jobs.
- [ ] [t03-public-docs-stubs-and-changelog.md](tasks/t03-public-docs-stubs-and-changelog.md) — Update docs/usage/databases.md, docs/getting-started/installation.md, test_native_stubs.py, docs/changelog/index.md, AGENTS.md, and product/tech-stack.md.
- [ ] [t04-okf-redis-limitations-and-skills-sync.md](tasks/t04-okf-redis-limitations-and-skills-sync.md) — Create redis-limitations.md and sync database-drivers-and-dialers.md, distributed-queues-and-email.md, packaging-and-pgo.md, polyglot-test-matrix.md, and review-python-framework.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Every OKF chapter and review skill stays `<= 200` lines with zero broken relative links or stale `resp3` references.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && make docs`

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database and cache driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — CI service containers and driver test gates.
[^docs-db]: `docs/usage/databases.md` — Database and cache drivers usage guide.
[^docs-queues]: `docs/usage/queues-and-realtime.md` — Queues and realtime channels usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^agents-md]: `AGENTS.md` — Repository agent instructions and Makefile commands.
[^product-md]: `.agents/bundles/product/product.md` — OKF product definition and crate capability table.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Distributed queues and email OKF chapter.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
