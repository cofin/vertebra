---
type: Task
id: "broker-pool-sharing:ch02:t04"
chapter_id: ch02-pool-partitions-and-dbrpc
title: Remove in-memory transports and alchemy StatementCache
description: Delete SyncInMemoryBrokerTransport, AsyncInMemoryBrokerTransport, _BaseInMemoryBrokerTransport, BrokerPlugin(use_shm=False), and extensions/alchemy/broker.py::StatementCache, and migrate their tests to the SHM transports.
status: draft
state: pending
priority: P2
verification_strategy: regression_tdd
depends_on:
  - "broker-pool-sharing:ch02:t01"
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/alchemy/broker.py
  - src/py/tests/unit/extensions/test_broker_transport.py
  - src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/alchemy/broker.py
tests:
  - src/py/tests/unit/extensions/test_broker_transport.py
  - src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 200
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: inmem
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseInMemoryBrokerTransport
    title: In-memory transports
  - id: plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: BrokerPlugin use_shm switch
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/broker.py::StatementCache
    title: Python-side statement cache
tags:
  - cleanup
  - broker
---

# Task t04: Remove In-Memory Transports and Alchemy StatementCache

## Objective & Scope Boundary

1. Delete `_BaseInMemoryBrokerTransport`, `SyncInMemoryBrokerTransport`, `AsyncInMemoryBrokerTransport` from `transport.py` [^inmem] and their exports in `extensions/broker/__init__.py` and `extensions/sqlspec/__init__.py` re-export lists if present (edits limited to removing names).
2. Remove the `use_shm` parameter from `BrokerPlugin.__init__` [^plugin]; it always builds SHM transports (anonymous region when `shm_name` is unset).
3. Delete `StatementCache` from `extensions/alchemy/broker.py` [^alchemy-cache]; `_BaseSHMSessionBroker` uses the colocated `LeaseBroker.statement_cache()` (`ShmStatementCache`). `_BaseSHMBrokerTransport` raises `BrokerAttachError` when `statement_cache()` is `None` with `statement_cache_capacity > 0` instead of falling back to a Python cache.
4. Migrate tests: replace in-memory transport usages with `SyncSHMBrokerTransport(BrokerConfig(...))` / `AsyncSHMBrokerTransport`; delete `StatementCache`-only tests in `test_drop_in_names_and_generics.py`.

## Bounded Read Manifest

- The three source files and three test files listed in `read_set`.

## Symbol & Signature Contracts

- `BrokerPlugin(config: BrokerConfig | None = None, *, zpages: ZPages | None = None, otel_recorder: Any | None = None)`.
- No compatibility alias for removed names.

## Edge Cases & Error Semantics

- Passing `use_shm=` raises `TypeError` (unexpected keyword).
- Import of a removed name raises `ImportError`.
- macOS: anonymous SHM uses `ShmRegion` over `shm_open` with a unique name; tests run on both platforms.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_broker_transport.py`. Dependency category: in-process SHM.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Removed names | parametrize name over `("SyncInMemoryBrokerTransport", "AsyncInMemoryBrokerTransport")`; `getattr(transport_module, name)` | `AttributeError` |
| Plugin keyword | `BrokerPlugin(use_shm=False)` | `TypeError` |
| Plugin SHM | `BrokerPlugin(config=BrokerConfig(pool_size=1))` | `plugin.transport` is `SyncSHMBrokerTransport` |
| Migrated lease TTL | Existing TTL expiry test on `SyncSHMBrokerTransport` | Lease expires and `total_expired == 1` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions -q`.
- `! git grep -n "InMemoryBrokerTransport\|use_shm=" -- src/py docs`.
- `make lint`.

## Acceptance Criteria

- [ ] In-memory transports, `use_shm`, and the alchemy `StatementCache` are gone with no aliases (R7).

## Notes & Discoveries

- [2026-11-12 01:40] Tests referencing removed names: `test_broker_transport.py`, `test_drop_in_names_and_generics.py`, `test_shm_broker.py`.

[^inmem]: `src/py/vertebra/extensions/broker/transport.py::_BaseInMemoryBrokerTransport` — In-memory transports.
[^plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — Plugin.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/broker.py::StatementCache` — Statement cache.
