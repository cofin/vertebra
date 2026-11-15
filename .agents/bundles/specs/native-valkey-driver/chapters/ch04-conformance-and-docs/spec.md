---
type: Spec
flow_id: native-valkey-driver
chapter_id: ch04-conformance-and-docs
title: 'Chapter 04: Valkey 9.0 Conformance, CI Services, Public Docs, and OKF Synchronization'
description: Add Valkey 9.0 hash-field expiration and CLIENT CAPA redirect conformance tests, add the valkey conformance matrix entry in CI, and synchronize public docs, native-stubs.md, product.md, valkey-limitations.md, and review skills.
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
  - valkey
  - conformance
  - ci
  - docs
  - okf
  - limitations
sources:
  - id: test-drv
    resource: src/py/tests/unit/db/test_redis_valkey_driver.py
    title: Unit tests for Redis and Valkey drivers
  - id: test-resp3
    resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
    title: RESP3, tracking, and pub/sub unit tests
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
current_task: "native-valkey-driver:ch04-conformance-and-docs:t01"
---

# Chapter 04: Valkey 9.0 Conformance, CI Services, Public Docs, and OKF Synchronization

<!-- truth: start -->
- `test_redis_valkey_driver.py` and `test_redis_valkey_resp3.py` verify Valkey 9.0 hash-field expiration (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`), `CLIENT CAPA redirect`, `CLUSTER SHARDS` replica routing, and Google Cloud Memorystore for Valkey IAM [^test-drv] [^test-resp3].
- `.github/workflows/ci.yml` adds a `valkey` entry (Valkey `8.0+`/`9.0`) to the `test-db-conformance` matrix (existing `pytest-databases` `valkey_service` fixture; `valkey` joins `ConformanceGate.SUPPORTED_ENGINES`) and runs `make valkey-test` and `make valkey-wheel-check`, while `test_driver_conformance.py` (`CACHE_ENGINE_SPECS` for `valkey`) enforces fail-loud `vertebra._valkey` checks [^ci-wf] [^conformance-suite].
- `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md` (`<= 200` lines) document `vertebra-valkey` (`[valkey]`), Valkey 9.0 hash-field expiration, and removal of `vertebra_db::resp3` [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md].
- `valkey-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` (`<= 200` lines each) capture present-tense Valkey architecture, operational caveats, and `make valkey-test` / `make valkey-wheel-check` targets [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

With `vertebra-valkey` (`vertebra._valkey`), `SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource`, and `ValkeyQueueBackend` cut over in Chapter 03 and `vertebra_db::resp3` deleted, this chapter completes Valkey 9.0 conformance testing, CI service container gates, public documentation, `product.md` crate table updates, and `valkey-limitations.md` (`<= 200` lines) [^conformance-suite] [^ci-wf] [^product-md].

**Non-Goals:** Distributed `ChannelBus` and `ValkeyStore` consolidation (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Valkey 9.0 unit/loopback | `src/py/tests/unit/db/{test_redis_valkey_driver,test_redis_valkey_resp3}.py` [^test-drv] [^test-resp3] | `HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`, `CLIENT CAPA redirect`, `CLUSTER SHARDS` |
| Conformance & CI | `test_driver_conformance.py` (`CACHE_ENGINE_SPECS`), `.github/workflows/ci.yml` [^conformance-suite] [^ci-wf] | `test-db-conformance` `valkey` matrix entry, `make valkey-test`, `make valkey-wheel-check` |
| Public docs & product | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `product/product.md` [^docs-db] [^docs-queues] [^docs-stubs] [^docs-changelog] [^agents-md] [^product-md] | Tabbed `vertebra-core[valkey]` installs, `heading_level: 4` stub docs, `product.md` |
| OKF & skills | `knowledge/domains/data/{valkey-limitations,database-drivers-and-dialers}.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, `workflow.md` [^okf-drivers] [^okf-queues] [^review-pyo3-skill] [^domains-idx] [^okf-workflow] | Present-tense OKF chapters (`<= 200` lines each), `DOC-08` parity |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R16**: Add Valkey 9.0 hash-field expiration (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`), `CLIENT CAPA redirect`, and `CLUSTER SHARDS` replica routing tests in `test_redis_valkey_driver.py` and `test_redis_valkey_resp3.py` (`t01`).
- **R17**: Wire `vertebra._valkey` fail-loud checks on `CACHE_ENGINE_SPECS` (`valkey`) in `test_driver_conformance.py`, add `valkey` to `ConformanceGate.SUPPORTED_ENGINES`, and add a `valkey` `test-db-conformance` matrix entry (Valkey `8.0+`/`9.0`) plus `make valkey-test` and `make valkey-wheel-check` to `.github/workflows/ci.yml` (`t02`).
- **R18**: Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`), `docs/changelog/index.md` (`Unreleased`), `AGENTS.md`, and `.agents/bundles/product/product.md` (adding `vertebra-valkey` and removing `parse_resp3_frame`/`crc16_xmodem`/`redis_hash_slot` from `vertebra-db`) (`t03`).
- **R19**: Author `.agents/bundles/knowledge/domains/data/valkey-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md` (`t04`).

```text
valkey-limitations.md required caveats (<= 200 lines):
1. Hash-field expiration (HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT/HTTL/HPTTL/HPERSIST) requires Valkey 9.0+ (returns ERR unknown command -> ProgrammingError on Valkey 8.x).
2. Dedicated socket per SyncValkeyListener / AsyncValkeyListener handle (subscribed sockets are isolated from multiplexed command pipelines).
3. Multi-key pipelines and Lua scripts on Valkey Cluster require matching {hash_tag} keys to avoid CROSSSLOT ProgrammingError.
4. Server-assisted CLIENT TRACKING invalidation and CLIENT CAPA redirect require RESP3 (protocol=3) on Valkey 8.0+/9.0.
5. unix:// sockets apply strictly to standalone single-node deployments, never Cluster or Sentinel TCP topologies.
```

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `HEXPIRE` on Valkey 8.x | Server returns `-ERR unknown command 'HEXPIRE'` | Maps to `ProgrammingError` | Deterministic version boundary |
| `native-stubs.md` TOC | `tools/docs/verify_docs_artifacts.py` | `heading_level: 4` keeps TOC `<= 14` and HTML `< 120_000` bytes | Docs gate green |
| Stale `product.md` row | `parse_resp3_frame` left in `vertebra-db` table | Removed from `vertebra-db` and documented under `vertebra-redis` / `vertebra-valkey` | `DOC-02` / `DOC-04` truth |

## 3. Documentation & OKF Knowledge Plan

Tasks `t03` and `t04` complete all documentation, skill, product, and OKF deliverables for `native-valkey-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Add Valkey 9.0 Hash-Field Expiration and Redirect Loopback Tests | `src/py/tests/unit/db/{test_redis_valkey_driver,test_redis_valkey_resp3}.py` | `pytest src/py/tests/unit/db/test_redis_valkey_*.py` |
| `t02` | Wire Valkey 9.0 Live Conformance and CI Service | `src/py/tests/integration/db/test_driver_conformance.py`, `.github/workflows/ci.yml` | `src/py/tests/integration/db/test_driver_conformance.py -k 'valkey'` |
| `t03` | Update Public Docs, Native Stubs, Changelog, AGENTS.md, and Product Definition | `docs/usage/{databases,queues-and-realtime}.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `.agents/bundles/product/product.md` | `python -m tools.docs.scan_public_docstrings`, `make review-check` |
| `t04` | Author OKF valkey-limitations.md and Synchronize Data/Framework Chapters and Review Skills | `.agents/bundles/knowledge/domains/data/{valkey-limitations,database-drivers-and-dialers}.md`, `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, `.agents/bundles/knowledge/workflow.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-valkey-hash-ttl-and-redirect-tests.md](tasks/t01-valkey-hash-ttl-and-redirect-tests.md) — Add Valkey 9.0 hash-field expiration (`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`), `CLIENT CAPA redirect`, and `CLUSTER SHARDS` replica routing tests.
- [ ] [t02-valkey-conformance-and-ci-service.md](tasks/t02-valkey-conformance-and-ci-service.md) — Enforce fail-loud `vertebra._valkey` conformance on `CACHE_ENGINE_SPECS` (`valkey`) in `test_driver_conformance.py` and add the `valkey` `test-db-conformance` matrix entry, `make valkey-test`, and `make valkey-wheel-check` to `.github/workflows/ci.yml`.
- [ ] [t03-public-docs-stubs-and-changelog.md](tasks/t03-public-docs-stubs-and-changelog.md) — Update `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, and `.agents/bundles/product/product.md`.
- [ ] [t04-okf-valkey-limitations-and-skills-sync.md](tasks/t04-okf-valkey-limitations-and-skills-sync.md) — Create `valkey-limitations.md` and sync `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `.agents/bundles/knowledge/workflow.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Every OKF chapter and review skill stays `<= 200` lines with zero broken links or stale `resp3` references.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/integration/db/test_driver_conformance.py -k 'valkey' -q && uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check`

[^test-drv]: `src/py/tests/unit/db/test_redis_valkey_driver.py` — Unit tests for Redis and Valkey drivers.
[^test-resp3]: `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3, tracking, and pub/sub unit tests.
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
