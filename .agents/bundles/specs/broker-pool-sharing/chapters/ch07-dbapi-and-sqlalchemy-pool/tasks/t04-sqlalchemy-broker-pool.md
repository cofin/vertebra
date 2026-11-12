---
type: Task
id: "broker-pool-sharing:ch07:t04"
chapter_id: ch07-dbapi-and-sqlalchemy-pool
title: Add BrokerPool for SQLAlchemy and Advanced Alchemy
description: Add extensions/sqlalchemy/pool.py with BrokerPool, AsyncBrokerPool, and broker_creator so SQLAlchemy and Advanced Alchemy sync/async engines lease connections from the shared broker pool keyed by pool_key, rolling back on return and quarantining on uncertain outcomes.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch07:t03"
  - "broker-pool-sharing:ch05:t01"
  - "broker-pool-sharing:ch06:t01"
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/vertebra/db/dbapi/_sync.py
  - src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch07-dbapi-and-sqlalchemy-pool/spec.md
files:
  - src/py/vertebra/extensions/sqlalchemy/pool.py
  - src/py/vertebra/extensions/sqlalchemy/__init__.py
  - src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py
estimated_loc: 240
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
  - id: shm-transport
    resource: src/py/vertebra/extensions/broker/transport.py::SyncSHMBrokerTransport
    title: SHM broker transport and lease acquisition
  - id: lease-source
    resource: src/py/vertebra/extensions/sqlspec/provider.py::SyncBrokerLeaseSource
    title: Typed lease source Protocol (ch06 t01)
  - id: sync-conn
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: DB-API connection accepting a leased driver (ch07 t01)
tags:
  - sqlalchemy
  - advanced-alchemy
  - broker
  - pool
---

# Task t04: Add `BrokerPool` for SQLAlchemy and Advanced Alchemy

## Objective & Scope Boundary

1. `src/py/vertebra/extensions/sqlalchemy/pool.py`:
   - `BrokerPool(sqlalchemy.pool.Pool)`: `_do_get` acquires a lease from a `SyncBrokerLeaseSource` [^lease-source] using `pool_name = BrokerPool.pool_name_for(config)` (hex of `stable_hash64` over `repr(config.pool_key())`) and wraps the leased driver in `SyncDBAPIConnection(driver=...)` [^sync-conn]; `_do_return_conn` rolls back, then releases; rollback failure quarantines and raises `LeaseQuarantinedError`. `status()`, `dispose()`, and `recreate()` follow the SQLAlchemy `Pool` contract.
   - `AsyncBrokerPool(sqlalchemy.pool.Pool)` with `_is_asyncio = True` for `AsyncAdaptedQueuePool`-style engines, leasing through `AsyncBrokerLeaseSource`.
   - `broker_creator(url, *, broker)` returns a zero-arg `creator` for `create_engine(..., creator=...)`.
2. `extensions/sqlalchemy/__init__.py` exports `BrokerPool`, `AsyncBrokerPool`, `broker_creator`.
3. `src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py` (new canonical module).

Out of scope: lease table internals, multi-process acceptance (`ch08 / t03`).

## Bounded Read Manifest

- `transport.py` — `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, `SyncConnectionLease` [^shm-transport].
- `provider.py` — `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource` (`ch06`) [^lease-source].
- `db/dbapi/_sync.py`, `async_adapter.py`.
- Chapter spec contracts.

## Symbol & Signature Contracts

```python
class BrokerPool(Pool):
    def __init__(self, creator: Callable[[], SyncDBAPIConnection], *, broker: SyncBrokerLeaseSource, config: BaseDatabaseConfig, **kw: Any) -> None: ...
    @staticmethod
    def pool_name_for(config: BaseDatabaseConfig) -> str: ...
    def status(self) -> str: ...

class AsyncBrokerPool(Pool):
    _is_asyncio: ClassVar[bool] = True
    def __init__(self, creator: Callable[[], Any], *, broker: AsyncBrokerLeaseSource, config: BaseDatabaseConfig, **kw: Any) -> None: ...

def broker_creator(url: str, *, broker: SyncBrokerLeaseSource) -> Callable[[], SyncDBAPIConnection]: ...
```

Advanced Alchemy wiring: `SQLAlchemyAsyncConfig(connection_string=url, engine_config=EngineConfig(poolclass=AsyncBrokerPool), ...)` with pool kwargs passed through `engine_config` (verify `EngineConfig` field names in the installed `advanced_alchemy` before writing the test).

## Edge Cases & Error Semantics

- `broker is None` → `ImproperConfigurationError("BrokerPool requires a broker lease source")`.
- Broker acquire timeout → SQLAlchemy `TimeoutError` chained from the broker error.
- Rollback failure on return → `LeaseQuarantinedError`; lease never released.
- `pool_key()` returning `None` (non-shareable config) → `ImproperConfigurationError` naming the config type.
- Same `pool_key` in two engines → identical `pool_name_for` value (deterministic across processes).

## Public-Seam Test Blueprint

Use a recording fake lease source satisfying the `ch06` Protocols.

| Test | Input | Expected |
| --- | --- | --- |
| `test_broker_pool_checkout_return` | engine on `BrokerPool`, `connect()` then close | one acquire, one rollback, one release |
| `test_broker_pool_rollback_failure_quarantines` | leased driver whose rollback raises | `LeaseQuarantinedError`; `quarantined == [lease]`; `released == []` |
| `test_broker_pool_requires_broker` | `broker=None` | `ImproperConfigurationError` |
| `test_pool_name_for_is_deterministic[sqlite, postgres]` | two equal configs | equal names; differing configs differ |
| `test_pool_name_for_rejects_unshareable` | config with `pool_key() is None` | `ImproperConfigurationError` |
| `test_async_broker_pool_checkout_return` | async engine on `AsyncBrokerPool` | one acquire, one release |
| `test_advanced_alchemy_async_config_uses_broker_pool` | `SQLAlchemyAsyncConfig` with `EngineConfig(poolclass=AsyncBrokerPool)` | `config.get_engine().pool` is `AsyncBrokerPool` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py -q`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/pool.py && uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy`

## Acceptance Criteria

- SQLAlchemy and Advanced Alchemy sync and async engines run on broker leases; matrix tests pass.

## Notes & Discoveries

- None yet.
