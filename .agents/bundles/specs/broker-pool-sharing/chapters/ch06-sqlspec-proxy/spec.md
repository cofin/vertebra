---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch06-sqlspec-proxy
title: "Chapter 6: SQLSpec Proxy"
description: Typed sync and async lease and session seams in the broker-side SQLSpec provider with quarantine on uncertain release, and worker-side proxy drivers that subclass the upstream SQLSpec adapter bases.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-lease-safety-and-host
replan_trigger: null
current_task: "broker-pool-sharing:ch06:t01"
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
    title: Broker-side SQLSpec provider (acquire, invoke, release paths)
  - id: sqlspec-proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: Worker-side proxy drivers and session contexts
  - id: sqlspec-plugin
    resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig
    title: Proxy SQLSpec configs wrapping upstream configs
  - id: broker-lease
    resource: src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease
    title: Sync and async connection lease objects
  - id: sqlspec-sync-base
    resource: .venv/lib/python3.12/site-packages/sqlspec/driver/_sync.py::SyncDriverAdapterBase
    title: Upstream SQLSpec sync driver adapter base
  - id: sqlspec-async-base
    resource: .venv/lib/python3.12/site-packages/sqlspec/driver/_async.py::AsyncDriverAdapterBase
    title: Upstream SQLSpec async driver adapter base
tags:
  - sqlspec
  - broker
  - quarantine
---

# Chapter 6: SQLSpec Proxy

<!-- truth: start -->
- `_BaseSQLSpecProvider._call_broker_acquire` and `_call_with_params` duck-type through `inspect.signature`; `_release_session_sync`, `_release_session_async`, `_release_broker_lease`, and `_release_broker_lease_async` wrap rollback, context exit, and release in `contextlib.suppress(Exception)` and then release the lease normally ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].
- `SyncConnectionLease.release` and `AsyncConnectionLease.release` are the only lease release paths the provider calls after this chapter; an uncertain outcome calls the transport quarantine added in `ch05 / t01` and raises `LeaseQuarantinedError` [^broker-lease].
- `ProxySyncDriver` and `ProxyAsyncDriver` are plain classes at HEAD; this chapter makes them subclasses of `sqlspec.driver.SyncDriverAdapterBase` and `sqlspec.driver.AsyncDriverAdapterBase` [^sqlspec-proxy] [^sqlspec-sync-base] [^sqlspec-async-base].
- `ProxySyncConfig` and `ProxyAsyncConfig` keep the wrapped upstream config's `statement_config` and parameter style [^sqlspec-plugin].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The broker-side provider swallows every rollback and context-exit failure, so a connection with an open transaction or a broken socket returns to the shared pool and the next worker inherits it ([#98](https://github.com/cofin/vertebra/issues/98)). Acquire and execute paths guess call shapes by introspecting signatures, which hides contract drift between the provider, the transport, and upstream SQLSpec sessions [^sqlspec-provider]. Worker-side proxy drivers are not SQLSpec drivers, so SQLSpec features that check `isinstance(driver, SyncDriverAdapterBase)` (statement compilation, parameter conversion, result shaping) do not apply [^sqlspec-proxy].

This chapter replaces duck-typing with typed Protocols, makes uncertain releases quarantine and raise, and rebases the proxy drivers on the upstream adapter bases.

### 1.2 Non-Goals

- Quarantine state in the lease table and the transport quarantine call (owned by `ch05 / t01`).
- Session affinity and streaming frames (owned by `ch05 / t04`).
- Crash reclamation and quarantined connection disposal (owned by `ch08 / t02`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Acquire | `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._call_broker_acquire`, `::_extract_lease_coords` [^sqlspec-provider] | In-process broker transport. |
| Release | `provider.py::_BaseSQLSpecProvider._release_session_sync`, `::_release_session_async`, `::_release_broker_lease`, `::_release_broker_lease_async` [^sqlspec-provider] | Upstream SQLSpec session context managers (remote-owned database). |
| Invoke | `provider.py::_BaseSQLSpecProvider._invoke_sync`, `::_invoke_async`, `::_call_with_params` [^sqlspec-provider] | Upstream SQLSpec driver `execute` / `execute_many`. |
| Leases | `src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease`, `::AsyncConnectionLease` [^broker-lease] | Typed lease objects with `slot_index`, `generation`, `release()`. |
| Proxy | `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver`, `::ProxyAsyncDriver`; `plugin.py::ProxySyncConfig`, `::ProxyAsyncConfig` [^sqlspec-proxy] [^sqlspec-plugin] | Upstream `sqlspec.driver` adapter bases [^sqlspec-sync-base] [^sqlspec-async-base]. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md), [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).
- Skills: `sqlspec`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/review-tests/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R15a**: Sync provider acquire, invoke, and release use typed Protocols; a failed rollback or context exit quarantines the lease and raises `LeaseQuarantinedError` (`t01`).
- **R15b**: Async provider paths follow the same contract (`t02`).
- **R16**: `ProxySyncDriver` / `ProxyAsyncDriver` subclass the upstream adapter bases and preserve the wrapped config's statement config and parameter style (`t03`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class SyncBrokerLeaseSource(Protocol):
    def acquire(self, *, pool_name: str, worker_pid: int) -> SyncConnectionLease: ...
    def quarantine(self, lease: SyncConnectionLease) -> None: ...

class AsyncBrokerLeaseSource(Protocol):
    async def acquire(self, *, pool_name: str, worker_pid: int) -> AsyncConnectionLease: ...
    async def quarantine(self, lease: AsyncConnectionLease) -> None: ...

class SyncLeasedSession(Protocol):
    def execute(self, statement: str, /, *parameters: Any) -> Any: ...
    def execute_many(self, statement: str, parameters: Sequence[Any], /) -> Any: ...
    def rollback(self) -> None: ...

class AsyncLeasedSession(Protocol):
    async def execute(self, statement: str, /, *parameters: Any) -> Any: ...
    async def execute_many(self, statement: str, parameters: Sequence[Any], /) -> Any: ...
    async def rollback(self) -> None: ...

class ProxySyncDriver(SyncDriverAdapterBase): ...
class ProxyAsyncDriver(AsyncDriverAdapterBase): ...
```

`LeaseQuarantinedError(VertebraError)` comes from `vertebra.extensions.broker.errors` (`ch05 / t01`); its message names the pool, slot index, generation, and the original exception type, and it chains the original with `raise ... from exc`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SQLSpecSyncProvider` acquire → `SyncBrokerLeaseSource.acquire(pool_name=..., worker_pid=...)` → `SyncSHMBrokerTransport` → `SyncConnectionLease`.
- `SQLSpecSyncProvider` execute → `SyncLeasedSession.execute(sql, *params)` or `execute_many(sql, params)` → upstream SQLSpec driver.
- `_release_session_sync` → `SyncLeasedSession.rollback()` (when in transaction) → context `__exit__` → `SyncConnectionLease.release()`; on failure → `SyncBrokerLeaseSource.quarantine(lease)` → `raise LeaseQuarantinedError(...) from exc`.
- Worker `ProxyAsyncDriver.execute` (via `AsyncDriverAdapterBase.execute`) → `_execute_statement` override → broker RPC frame → provider.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Clean release | No transaction, context exit succeeds | `lease.release()` once; no quarantine | Capacity returns |
| Rollback raises | `session.rollback()` raises `OSError` | `quarantine(lease)` once; `release()` never called; `LeaseQuarantinedError` chained from `OSError` | No dirty connection re-leased |
| Context exit raises | `__exit__` raises after successful rollback | Quarantine and raise as above | Broken connection removed |
| Quarantine raises | `quarantine` itself raises | Quarantine error propagates with rollback error as `__context__`; lease stays leased for `ch08` reclamation | Never silently released |
| Release raises | `lease.release()` raises | Error propagates unchanged | Fail loud |
| Missing method | Session lacks `execute_many` | `TypeError` at Protocol `isinstance` check naming the session type | Contract enforced at the seam |
| Params shapes | `None`, `()`, `dict`, `tuple`, `list` | `None`/empty → `execute(sql)`; `dict` → `execute(sql, params)`; sequence → `execute(sql, *params)` | Deterministic dispatch |
| Upstream compiler | Wrapped `AsyncpgConfig` with numeric parameter style | Proxy driver `statement_config` equals wrapped config's | Same SQL as direct driver |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a pinned session whose rollback raises, when the provider releases it, then `LeaseQuarantinedError` reaches the caller, the transport records one quarantine, and the lease is never released normally.
- Given a `ProxyAsyncConfig` wrapping an upstream async config, when `isinstance(driver, AsyncDriverAdapterBase)` is checked, then it is `True` and `driver.statement_config` matches the wrapped config.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Seam typing | `typing.Protocol` with `runtime_checkable` at construction | Signature introspection: hides drift and costs a reflection call per acquire. |
| Release failure | Quarantine then raise | Log and release: returns open transactions to the pool. |
| Proxy base | Subclass upstream adapter bases | Composition wrapper: SQLSpec `isinstance` checks and compiler pipeline do not apply. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; quarantine semantics are documented in `ch09 / t01` (`docs/usage/extensions.md`) and `ch09 / t02` (OKF).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R15a | [t01](tasks/t01-typed-sync-provider-seams.md) | `regression_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |
| R15b | [t02](tasks/t02-typed-async-provider-seams.md) | `regression_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |
| R16 | [t03](tasks/t03-adapter-base-proxy-drivers.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch06:t01 — Replace sync provider duck-typing and suppressed release errors with typed seams and quarantine (tasks/t01-typed-sync-provider-seams.md)
- [ ] broker-pool-sharing:ch06:t02 — Apply typed seams and quarantine to async provider paths (tasks/t02-typed-async-provider-seams.md)
- [ ] broker-pool-sharing:ch06:t03 — Rebase proxy drivers on SQLSpec adapter bases (tasks/t03-adapter-base-proxy-drivers.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec` exits `0`, and `git grep -n "inspect.signature\|contextlib.suppress" src/py/vertebra/extensions/sqlspec` prints nothing.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`0/3` completed).
- **Active Invariants**: No `contextlib.suppress(Exception)` or `inspect.signature` in `extensions/sqlspec`; a lease is either released or quarantined, never both.

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider.
[^sqlspec-proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver` — Worker-side proxy drivers.
[^sqlspec-plugin]: `src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig` — Proxy SQLSpec configs.
[^broker-lease]: `src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease` — Connection lease objects.
[^sqlspec-sync-base]: `sqlspec/driver/_sync.py::SyncDriverAdapterBase` — Upstream sync adapter base.
[^sqlspec-async-base]: `sqlspec/driver/_async.py::AsyncDriverAdapterBase` — Upstream async adapter base.
