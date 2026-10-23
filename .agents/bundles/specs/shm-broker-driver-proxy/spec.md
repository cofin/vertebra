---
type: Spec
flow_id: shm-broker-driver-proxy
title: "Universal Shared-Memory Broker Proxy for Any SQLSpec or Advanced Alchemy Driver"
state: closed
plan_revision: 1
plan_commit: null
state_revision: 1
current_task: null
last_operation: finish
operation_targets:
  - "1.1"
  - "1.2"
  - "2.1"
last_verified_checkpoint: "make py-test && make lint-all"
created_at: 2026-10-23T20:18:00Z
updated_at: 2026-10-23T22:23:00Z
description: "Wire ProxySyncConfig[SyncConfigT], ProxyAsyncConfig[AsyncConfigT], SqlSpecProvider, and AlchemyPlugin so any existing driver supported in SQLSpec or Advanced Alchemy executes across shared-memory rings with threshold-based ShmRegion Arrow IPC zero-copy result transfer."
tags: [feature, sqlspec, advanced-alchemy, shm, broker, arrow, proxy]
parent_prd: shm-native-drivers-and-broker-proxy
research: []
---

# Flow: Universal Shared-Memory Broker Proxy for Any SQLSpec or Advanced Alchemy Driver

**Flow ID:** `shm-broker-driver-proxy`

## Specification

### Code Analysis Summary

- **[`src/py/vertebra/extensions/sqlspec/plugin.py`](../../../../src/py/vertebra/extensions/sqlspec/plugin.py#L272-L452)**:
  - `ProxySyncConfig[SyncConfigT]` and `ProxyAsyncConfig[AsyncConfigT]` accept `config: SyncConfigT | AsyncConfigT` and `broker: BrokerTransport | None = None`, but `provide_session()`, `provide_connection()`, and `create_pool()` currently ignore `self.broker` and delegate directly to `self.config` in-process.
- **Historical `a5918811:src/py/vertebra/extensions/sqlspec/{proxy,provider}.py` & `cofin/litecore` (`mvp/4`)**:
  - In `a5918811` and `litecore`, `VertebraProxySyncConfig[SyncConfigT]` / `VertebraProxyAsyncConfig[AsyncConfigT]` paired with `SqlSpecProvider` (`ProviderHost`):
    - A single provider host owns the real upstream `sqlspec` connection pool (`oracledb`, `spanner`, `bigquery`, `duckdb`, `adbc`, `asyncpg`, `psycopg`, `psqlpy`, `aiosqlite`, etc.).
    - Worker processes obtain a `ProxySyncDriver` / `ProxyAsyncDriver` session that sends `execute`, `execute_many`, `select_to_arrow`, `begin`, `commit`, and `rollback` envelopes (`msgspec` msgpack frames) over shared-memory request/response rings (`SpscRing`/`MpmcRing`) while holding a `ConnectionLease` from `ShmBrokerTransport`.
    - When a query result exceeds `arrow_threshold_rows` (default `1000`) or `arrow_threshold_bytes` (default `65536` / `1_048_576`), `SqlSpecProvider` writes the result as an Arrow IPC stream into a `ShmRegion` (`_write_arrow_region`) and returns `{"shm_arrow": region_name, "size": len(ipc_bytes), "rows": num_rows}`; the worker maps the `ShmRegion` zero-copy (`pa.ipc.open_stream(pa.py_buffer(buf))`) and sends `_arrow_release` once consumed.
- **[`src/py/vertebra/extensions/alchemy/broker.py`](../../../../src/py/vertebra/extensions/alchemy/broker.py) & [`src/py/vertebra/extensions/alchemy/plugin.py`](../../../../src/py/vertebra/extensions/alchemy/plugin.py)**:
  - `ShmSessionBroker` hooks SQLAlchemy pool `checkout`/`checkin` events to acquire/release a `ConnectionLease`, and `AlchemyPlugin` registers `self.broker`, but `AlchemyPlugin` does not yet automatically attach `ShmSessionBroker` to registered engines or provide proxied session execution over `ShmBrokerTransport`.

### Relevant Patterns

- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)
- [Shared-Memory Connection Broker & Statement Cache](../../knowledge/domains/data/broker-and-shm-leases.md)
- [Python Module Structure, Class Layout & PEP 695 Typing](../../knowledge/patterns/python/module-and-class-layout.md)

### Requirements

1. **`SqlSpecProvider` Host & Shared-Memory RPC Envelope Protocol**:
   - Implement `SqlSpecProvider` in `src/py/vertebra/extensions/sqlspec/provider.py` that hosts one or more upstream `sqlspec` configurations (`DatabaseConfigProtocol`) and services worker requests over `ShmBrokerTransport` / `SpscRing` request-response pairs.
   - Support operations: `execute`, `execute_many`, `select_to_arrow`, `begin`, `commit`, `rollback`, and `_arrow_release`, plus transaction-pinned connection leases so multi-statement transactions (`begin` -> `execute` -> `commit`/`rollback`) execute on the same upstream connection.
2. **Threshold-Based `ShmRegion` Arrow IPC Zero-Copy Transfer**:
   - Configure `arrow_threshold_rows: int = 1000` and `arrow_threshold_bytes: int = 65_536` on `ProxySyncConfig` and `ProxyAsyncConfig`.
   - For result sets below the threshold, return compact MessagePack row tuples directly in the ring slot.
   - For result sets at or above the threshold (or explicit `select_to_arrow` calls), serialize into a shared-memory `ShmRegion` via Arrow IPC (`pa.ipc.new_stream`) and read zero-copy in the worker via `pa.py_buffer` / `memoryview`, releasing the region deterministically.
3. **Wire `ProxySyncConfig[SyncConfigT]`, `ProxyAsyncConfig[AsyncConfigT]` & `AlchemyPlugin`**:
   - Update `ProxySyncConfig` and `ProxyAsyncConfig` in [`src/py/vertebra/extensions/sqlspec/plugin.py`](../../../../src/py/vertebra/extensions/sqlspec/plugin.py) so when `broker` is attached (or inherited from `SQLSpecPlugin(broker=...)`), `provide_session()` and `provide_connection()` return broker-proxied sessions/connections backed by `ShmBrokerTransport` and `ShmStatementCache`, while preserving the static `SyncConfigT` / `AsyncConfigT` type parameter for Litestar DI and Mypy/Pyright.
   - Update `AlchemyPlugin` in [`src/py/vertebra/extensions/alchemy/plugin.py`](../../../../src/py/vertebra/extensions/alchemy/plugin.py) so when `broker` is configured, engines automatically attach `ShmSessionBroker` and route statement compilation through `ShmBrokerTransport.get_or_compile_statement`.

## Implementation Plan

### Phase 1: `SqlSpecProvider` & Threshold-Based `ShmRegion` Arrow IPC Transport
- [x] 1.1 Implement `SqlSpecProvider`, transaction-pinned lease execution, and threshold-based `ShmRegion` Arrow IPC result spilling (`_write_arrow_region` / `_arrow_release`) in `src/py/vertebra/extensions/sqlspec/provider.py`
- [x] 1.2 Implement `ProxySyncDriver` and `ProxyAsyncDriver` in `src/py/vertebra/extensions/sqlspec/proxy.py` and wire `ProxySyncConfig[SyncConfigT]`, `ProxyAsyncConfig[AsyncConfigT]`, and `SQLSpecPlugin`

### Phase 2: Advanced Alchemy Broker Engine Wiring & Multi-Driver Verification
- [x] 2.1 Wire `AlchemyPlugin` and `ShmSessionBroker` to auto-attach SHM lease checkout/checkin and `ShmStatementCache` across SQLAlchemy sync/async engines, and verify with upstream SQLSpec and Advanced Alchemy drivers

## Continuity Snapshot

- **Active Flow**: `shm-broker-driver-proxy` (`closed`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `make py-test && make lint-all`
- **Decisions**:
  - Any existing driver supported in `sqlspec` (e.g., `oracledb`, `spanner`, `bigquery`, `duckdb`, `adbc`, `asyncpg`, `psycopg`) or `advanced-alchemy` is supported over shared memory via `ProxySyncConfig[ConfigT]`, `ProxyAsyncConfig[ConfigT]`, `SqlSpecProvider`, and `AlchemyPlugin(broker=...)` without writing bespoke per-database Python driver wrappers in `vertebra.db`.
- **Recent Discoveries**:
  - `ProxySyncConfig` and `ProxyAsyncConfig` in `src/py/vertebra/extensions/sqlspec/plugin.py` already have the PEP 695 generic signatures and `broker` slot, making it clean to wire `ProxySyncDriver` / `ProxyAsyncDriver` without breaking public typing.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Activate flow after `shm-database-broker` or in parallel to implement `SqlSpecProvider` and `ProxySyncDriver`/`ProxyAsyncDriver`.
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
