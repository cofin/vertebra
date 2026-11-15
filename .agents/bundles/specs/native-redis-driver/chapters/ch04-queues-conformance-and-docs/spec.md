---
type: Spec
flow_id: native-redis-driver
chapter_id: ch04-queues-conformance-and-docs
title: 'Chapter 04: Native Redis Queue Backend, Conformance CI, and Docs/OKF Synchronization'
description: Cut RedisQueueBackend over to vertebra._redis Streams/ZSETs, wire Redis 8.0 conformance in CI, and synchronize public docs, changelog, OKF chapters, and review skills.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch03-python-cutover-and-notifications
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
tags:
  - queues
  - streams
  - redis
  - ci
  - docs
  - okf
  - limitations
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: RedisQueueBackend using redis.asyncio
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::CacheConformanceSpec
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
current_task: "native-redis-driver:ch04-queues-conformance-and-docs:t01"
---

# Chapter 04: Native Redis Queue Backend, Conformance CI, and Docs/OKF Synchronization

<!-- truth: start -->
- `RedisQueueBackend` (`vertebra-core[redis]`) replaces `redis.asyncio` in `src/py/vertebra/framework/queues/redis.py` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient`, accepting `redis://`, `rediss://`, and `unix://` while keeping `client: RedisStreamClient | None = None` injection [^queue-redis].
- `.github/workflows/ci.yml` adds a `redis` entry (Redis `7.2+`/`8.0`) to the `test-db-conformance` matrix (existing `pytest-databases` `redis_service` fixture; `redis` joins `ConformanceGate.SUPPORTED_ENGINES`) and runs `make redis-test` and `make redis-wheel-check`, while `test_driver_conformance.py` (`CACHE_ENGINE_SPECS` for `redis`) enforces fail-loud `vertebra._redis` checks [^ci-wf] [^conformance-suite].
- `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` (`<= 200` lines) document `vertebra-redis` (`[redis]`), `SyncNotificationSource`/`AsyncNotificationSource`, and Redis Streams queues [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md].
- `redis-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` (`<= 200` lines each) capture present-tense Redis architecture, operational caveats, and `make redis-test` / `make redis-wheel-check` targets [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/framework/queues/redis.py` lazily loads third-party `redis.asyncio` (`ensure_module`) and rejects `unix://` URLs [^queue-redis]. Cutting `RedisQueueBackend` onto `vertebra._redis.AsyncRedisClient` eliminates the external `redis-py` dependency for Redis queues, enables live Redis service container CI conformance [^ci-wf], adds `vertebra-redis` to `.agents/bundles/product/product.md` [^product-md], and completes all `[12/12]` documentation, skill, and OKF deliverables including `redis-limitations.md` (`<= 200` lines) [^agents-md] [^okf-workflow]. (`ValkeyQueueBackend` cutover to `vertebra._valkey` and `valkey_service` CI are delivered in `native-valkey-driver`.)

**Non-Goals:** Valkey native wheel (`native-valkey-driver`) and distributed channel fan-out extensions (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Queue backend | `src/py/vertebra/framework/queues/redis.py` [^queue-redis] | `_RedisStreamAdapter` (`PY-07` compliant) over `AsyncRedisClient` |
| Conformance & CI | `test_driver_conformance.py` (`CACHE_ENGINE_SPECS`), `.github/workflows/ci.yml` [^conformance-suite] [^ci-wf] | `test-db-conformance` `redis` matrix entry, `make redis-test`, `make redis-wheel-check` |
| Public docs & product | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `product/product.md` [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md] | Tabbed installs, `heading_level: 4` stub docs, changelog, crate matrix |
| OKF & skills | `knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow] | Present-tense OKF chapters (`<= 200` lines each), `DOC-08` parity |

**Knowledge Anchors:** [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R17**: Replace `redis.asyncio` in `framework/queues/redis.py` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient` supporting `redis://`, `rediss://`, and `unix://`, keeping `client: RedisStreamClient | None = None` injection (`t01`).
- **R18**: Wire `vertebra._redis` fail-loud checks on `CACHE_ENGINE_SPECS` (`redis`) in `test_driver_conformance.py`, add `native_extension_module` to `CacheConformanceSpec` (widening its Redis-typed `config_cls`/driver annotations), add `redis` to `ConformanceGate.SUPPORTED_ENGINES`, and add a `redis` `test-db-conformance` matrix entry plus `make redis-test` and `make redis-wheel-check` to `.github/workflows/ci.yml` (`t02`).
- **R19a / R19b**: Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`), `docs/changelog/index.md` (`Unreleased`), `AGENTS.md`, and `.agents/bundles/product/product.md` (`t03`).
- **R19c / R19d**: Author `.agents/bundles/knowledge/domains/data/redis-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` (`t04`).

```python
class _RedisStreamAdapter(RedisStreamClient):
    async def xgroup_create(self, key: str, group: str, mkstream: bool = True) -> None: ...
    async def xadd(self, key: str, fields: Mapping[str, bytes | str]) -> str: ...
    async def xreadgroup(self, group: str, consumer: str, key: str, count: int, block_ms: int) -> list[Any]: ...
    async def xack(self, key: str, group: str, msg_id: str) -> int: ...
    async def xautoclaim(self, key: str, group: str, consumer: str, min_idle_ms: int, count: int) -> list[Any]: ...
    async def aclose(self) -> None: ...
```

```text
redis-limitations.md required caveats (<= 200 lines):
1. Dedicated socket per SyncRedisListener / AsyncRedisListener handle (subscribed sockets are isolated from multiplexed command pipelines).
2. Multi-key pipelines and Lua scripts on Redis Cluster require matching {hash_tag} keys to avoid CROSSSLOT ProgrammingError.
3. Server-assisted CLIENT TRACKING invalidation requires RESP3 (protocol=3) on Redis 7.2+/8.0.
4. unix:// sockets apply strictly to standalone single-node deployments, never Cluster or Sentinel TCP topologies.
```

**Integration Flow:**
- `RedisQueueBackend(url)` probes `vertebra-core[redis]`; injected `client: RedisStreamClient` bypasses wheel import for unit doubles.
- `make docs` runs `scan_public_docstrings`, Zensical build, and `tools/docs/verify_docs_artifacts.py`; `make review-check` verifies links, `sources:` paths, `DOC-08` `Makefile` parity, and `<= 200`-line budgets.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `redis.asyncio` probe | `git grep 'redis.asyncio' src/py/vertebra/framework/queues/redis.py` | Zero matches | Third-party `redis-py` removed from `queues/redis.py` |
| Invalid queue URL | `memory://`, `valkey://`, or hostless `redis://` | Raises `ImproperConfigurationError` | Strict URL validation |
| `native-stubs.md` TOC | `tools/docs/verify_docs_artifacts.py` | `heading_level: 4` keeps TOC `<= 14` and HTML `< 120_000` bytes | Docs gate green |
| `product.md` crate row | `vertebra-redis` row added | Updated in `t03` | `DOC-02` / `DOC-04` truth |
| Skill/OKF line cap | File exceeds `200` lines | Kept concise and `<= 200` lines | `ReviewSetupValidator` budget |

## 3. Documentation & OKF Knowledge Plan

Tasks `t03` and `t04` complete `[12/12]` documentation, skill, product, and OKF deliverables for `native-redis-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut RedisQueueBackend Over to vertebra._redis Streams and ZSETs | `src/py/vertebra/framework/queues/redis.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t02` | Wire Redis 8.0 Live Conformance and CI Service | `src/py/tests/integration/db/test_driver_conformance.py`, `.github/workflows/ci.yml` | `src/py/tests/integration/db/test_driver_conformance.py -k 'redis'` |
| `t03` | Update Public Docs, Native Stubs, Changelog, AGENTS.md, and Product Definition | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `.agents/bundles/product/product.md` | `python -m tools.docs.scan_public_docstrings`, `make review-check` |
| `t04` | Author OKF redis-limitations.md and Synchronize Data/Framework Chapters and Review Skills | `.agents/bundles/knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, `.agents/bundles/knowledge/workflow.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-native-redis-and-valkey-queue-backends.md](tasks/t01-native-redis-and-valkey-queue-backends.md) — Wire `RedisQueueBackend` onto native `vertebra._redis` stream consumer groups and delayed ZSETs.
- [ ] [t02-cache-conformance-and-ci-services.md](tasks/t02-cache-conformance-and-ci-services.md) — Enforce fail-loud `vertebra._redis` conformance on `CACHE_ENGINE_SPECS` (`redis`) in `test_driver_conformance.py` and add `redis_service`, `make redis-test`, and `make redis-wheel-check` to `.github/workflows/ci.yml`.
- [ ] [t03-public-docs-stubs-and-changelog.md](tasks/t03-public-docs-stubs-and-changelog.md) — Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md`.
- [ ] [t04-okf-redis-limitations-and-skills-sync.md](tasks/t04-okf-redis-limitations-and-skills-sync.md) — Create `redis-limitations.md` and sync `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `.agents/bundles/knowledge/workflow.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Every OKF chapter and review skill stays `<= 200` lines with zero broken links or stale `NotificationSource` references.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/integration/db/test_driver_conformance.py -k 'redis' -q && uv run --no-sync ruff check src/py/vertebra/framework/queues/redis.py && uv run --no-sync mypy src/py/vertebra/framework/queues/redis.py && uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check`

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend using redis.asyncio.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::CacheConformanceSpec` — Database and cache driver conformance suite.
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
