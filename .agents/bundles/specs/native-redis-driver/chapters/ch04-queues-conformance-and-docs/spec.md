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
depends_on_chapters:
- ch03-python-cutover-and-notifications
plan_commit: 1b191388
updated_at: '2026-11-14T23:16:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:16:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:16:00Z'
  commit: 1b191388
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
---

# Chapter 04: Native Redis/Valkey Queue Backends, Conformance CI, and Docs/OKF Synchronization

<!-- truth: start -->
- `RedisQueueBackend` (`vertebra-core[redis]`) and `ValkeyQueueBackend` (`vertebra-core[valkey]`) replace `redis.asyncio` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient`, accepting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` while keeping `client: RedisStreamClient | None = None` injection [^queue-redis].
- `.github/workflows/ci.yml` runs `redis_service` (Redis `7.2+`/`8.0`), `valkey_service` (Valkey `8.0+`/`9.0`), `make redis-test`, and `make redis-wheel-check`, while `test_driver_conformance.py` (`CACHE_ENGINE_SPECS`) enforces fail-loud `vertebra._redis` checks [^ci-wf] [^conformance-suite].
- `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` (`<= 200` lines) document `vertebra-redis` (`[redis]`/`[valkey]`), `SyncNotificationSource`/`AsyncNotificationSource`, Streams queues, and removal of `vertebra_db::resp3` [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md].
- `redis-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` (`<= 200` lines each) capture present-tense Redis/Valkey architecture, operational caveats, and `make redis-test` / `make redis-wheel-check` targets [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/vertebra/framework/queues/redis.py` imports third-party `redis.asyncio` and rejects `valkey://`, `valkeys://`, and `unix://` URLs [^queue-redis]. Cutting both queue backends onto `vertebra._redis.AsyncRedisClient` eliminates the external `redis-py` dependency, enables live Redis and Valkey service container CI conformance [^ci-wf], removes stale `vertebra_db::resp3` entries from `.agents/bundles/product/product.md` [^product-md], and completes all `[12/12]` documentation, skill, and OKF deliverables including `redis-limitations.md` (`<= 200` lines) [^agents-md] [^okf-workflow].

**Non-Goals:** Distributed channel fan-out extensions and Litestar/Django queue integrations (owned by `queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Queue backends | `src/py/vertebra/framework/queues/redis.py` [^queue-redis] | `_RedisStreamAdapter` (`PY-07` compliant) over `AsyncRedisClient` |
| Conformance & CI | `test_driver_conformance.py` (`CACHE_ENGINE_SPECS`), `.github/workflows/ci.yml` [^conformance-suite] [^ci-wf] | `redis_service` + `valkey_service`, `make redis-test`, `make redis-wheel-check` |
| Public docs & product | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `product/product.md` [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md] | Tabbed installs, `heading_level: 4` stub docs, changelog, crate matrix |
| OKF & skills | `knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow] | Present-tense OKF chapters (`<= 200` lines each), `DOC-08` parity |

**Knowledge Anchors:** [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R17**: Replace `redis.asyncio` in `framework/queues/redis.py` with `_RedisStreamAdapter` over `vertebra._redis.AsyncRedisClient` supporting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://`, keeping `client: RedisStreamClient | None = None` injection (`t01`).
- **R18**: Wire `vertebra._redis` fail-loud checks on `CACHE_ENGINE_SPECS` (`redis` and `valkey`) in `test_driver_conformance.py`, and add `redis_service` + `valkey_service`, `make redis-test`, and `make redis-wheel-check` to `.github/workflows/ci.yml` (`t02`).
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
2. Multi-key pipelines and Lua scripts on Redis/Valkey Cluster require matching {hash_tag} keys to avoid CROSSSLOT ProgrammingError.
3. Server-assisted CLIENT TRACKING invalidation requires RESP3 (protocol=3) on Redis 7.2+/8.0 or Valkey 8.0+/9.0.
4. unix:// sockets apply strictly to standalone single-node deployments, never Cluster or Sentinel TCP topologies.
```

**Integration Flow:**
- `RedisQueueBackend(url)` probes `vertebra-core[redis]`; `ValkeyQueueBackend(url)` probes `vertebra-core[valkey]`; injected `client: RedisStreamClient` bypasses wheel import for unit doubles.
- `make docs` runs `scan_public_docstrings`, Zensical build, and `tools/docs/verify_docs_artifacts.py`; `make review-check` verifies links, `sources:` paths, `DOC-08` `Makefile` parity, and `<= 200`-line budgets.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `redis.asyncio` probe | `git grep 'redis.asyncio' src/py/vertebra` | Zero matches | Third-party `redis-py` removed |
| Invalid queue URL | `memory://` or hostless `redis://` | Raises `ImproperConfigurationError` | Strict URL validation |
| `native-stubs.md` TOC | `tools/docs/verify_docs_artifacts.py` | `heading_level: 4` keeps TOC `<= 14` and HTML `< 120_000` bytes | Docs gate green |
| Stale `product.md` row | `parse_resp3_frame` left in `vertebra-db` table | Updated to `vertebra-redis` in `t03` | `DOC-02` / `DOC-04` truth |
| Skill/OKF line cap | File exceeds `200` lines | Kept concise and `<= 200` lines | `ReviewSetupValidator` budget |

## 3. Documentation & OKF Knowledge Plan

Tasks `t03` and `t04` complete `[12/12]` documentation, skill, product, and OKF deliverables for `native-redis-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut RedisQueueBackend and ValkeyQueueBackend Over to vertebra._redis Streams and ZSETs | `src/py/vertebra/framework/queues/redis.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t02` | Wire Redis 8.0 and Valkey 9.0 Live Conformance and CI Services | `src/py/tests/integration/db/test_driver_conformance.py`, `.github/workflows/ci.yml` | `src/py/tests/integration/db/test_driver_conformance.py -k 'redis or valkey'` |
| `t03` | Update Public Docs, Native Stubs, Changelog, AGENTS.md, and Product Definition | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `.agents/bundles/product/product.md` | `python -m tools.docs.scan_public_docstrings`, `make review-check` |
| `t04` | Author OKF redis-limitations.md and Synchronize Data/Framework Chapters and Review Skills | `.agents/bundles/knowledge/domains/data/{redis-limitations,database-drivers-and-dialers}.md`, `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, `.agents/bundles/knowledge/workflow.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-native-redis-and-valkey-queue-backends.md](tasks/t01-native-redis-and-valkey-queue-backends.md) — Wire `RedisQueueBackend` and `ValkeyQueueBackend` onto native `vertebra._redis` stream consumer groups and delayed ZSETs.
- [ ] [t02-cache-conformance-and-ci-services.md](tasks/t02-cache-conformance-and-ci-services.md) — Enforce fail-loud `vertebra._redis` conformance on `CACHE_ENGINE_SPECS` in `test_driver_conformance.py` and add `redis_service`, `valkey_service`, `make redis-test`, and `make redis-wheel-check` to `.github/workflows/ci.yml`.
- [ ] [t03-public-docs-stubs-and-changelog.md](tasks/t03-public-docs-stubs-and-changelog.md) — Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md`.
- [ ] [t04-okf-redis-limitations-and-skills-sync.md](tasks/t04-okf-redis-limitations-and-skills-sync.md) — Create `redis-limitations.md` and sync `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `.agents/bundles/knowledge/workflow.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Every OKF chapter and review skill stays `<= 200` lines with zero broken links or stale `resp3`/`NotificationSource` references.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/integration/db/test_driver_conformance.py -k 'redis or valkey' -q && uv run --no-sync ruff check src/py/vertebra/framework/queues/redis.py && uv run --no-sync mypy src/py/vertebra/framework/queues/redis.py && uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check`

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
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
