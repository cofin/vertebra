---
type: Task
id: "broker-pool-sharing:ch06:t02"
chapter_id: ch06-sqlspec-proxy
title: Apply typed seams and quarantine to async provider paths
description: Introduce AsyncBrokerLeaseSource and AsyncLeasedSession Protocols, make _release_session_async / _release_broker_lease_async quarantine and raise, type _invoke_async, and remove the remaining inspect/contextlib usage and _extract_lease_coords from the provider.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "broker-pool-sharing:ch06:t01"
read_set:
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/errors.py
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch06-sqlspec-proxy/spec.md
files:
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
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
  - id: release-async
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._release_session_async
    title: Async session release with suppressed rollback and exit errors
  - id: release-lease-async
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._release_broker_lease_async
    title: Async broker lease release with fallbacks
  - id: invoke-async
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._invoke_async
    title: Async method-name probing invoke helper
  - id: async-lease
    resource: src/py/vertebra/extensions/broker/transport.py::AsyncConnectionLease
    title: Async connection lease
tags:
  - sqlspec
  - quarantine
  - async
---

# Task t02: Apply Typed Seams and Quarantine to Async Provider Paths

## Objective & Scope Boundary

In `src/py/vertebra/extensions/sqlspec/provider.py`:

1. Add `@runtime_checkable` `AsyncBrokerLeaseSource` and `AsyncLeasedSession` Protocols (chapter spec section 2.2) to `__all__`.
2. Async acquire awaits `source.acquire(pool_name=..., worker_pid=...)`; delete the `inspect.isawaitable` branching on acquire results.
3. `_invoke_async(session: AsyncLeasedSession, operation, sql, params)` awaits `execute` / `execute_many` through the shared `_dispatch_execute` shape from `t01` [^invoke-async].
4. `_release_session_async` and `_release_broker_lease_async`: rollback → `__aexit__` (async context managers only; sync `__exit__` fallback removed because upstream async sessions are async context managers) → `await lease.release()`. Any rollback or exit failure awaits `source.quarantine(lease)` and raises `LeaseQuarantinedError(...) from exc` [^release-async] [^release-lease-async] [^async-lease].
5. Delete `_extract_lease_coords`, the `contextlib` and `inspect` imports, and the `_next_generation` counter once unused.

Out of scope: sync paths (`t01`), proxy drivers (`t03`).

## Bounded Read Manifest

- `provider.py` — async acquire helper, `_release_session_async`, `_release_broker_lease_async`, `_invoke_async`, `_exec_rollback_async`, `SQLSpecAsyncProvider`, `_extract_lease_coords` [^release-async] [^invoke-async].
- `transport.py` — `AsyncConnectionLease`, `AsyncSHMBrokerTransport` quarantine (`ch05 / t01`) [^async-lease].
- `extensions/broker/errors.py` — `LeaseQuarantinedError`.
- `test_sqlspec_shm_proxy.py` — fakes added in `t01`.

## Symbol & Signature Contracts

```python
@runtime_checkable
class AsyncBrokerLeaseSource(Protocol):
    async def acquire(self, *, pool_name: str, worker_pid: int) -> AsyncConnectionLease: ...
    async def quarantine(self, lease: AsyncConnectionLease) -> None: ...

@runtime_checkable
class AsyncLeasedSession(Protocol):
    async def execute(self, statement: str, /, *parameters: Any) -> Any: ...
    async def execute_many(self, statement: str, parameters: Sequence[Any], /) -> Any: ...
    async def rollback(self) -> None: ...

async def _release_session_async(self, key: tuple[int, int], in_tx: bool) -> None: ...
async def _release_broker_lease_async(self, lease: AsyncConnectionLease, key: tuple[int, int]) -> None: ...
```

## Edge Cases & Error Semantics

- Rollback raises → one awaited `quarantine`, zero `release`, `LeaseQuarantinedError` chained.
- `__aexit__` raises → same.
- Cancellation during rollback (`asyncio.CancelledError`) → quarantine is awaited inside `asyncio.shield`, then `CancelledError` re-raises unchanged (the outcome is uncertain, so the connection never returns clean).
- `quarantine` raises → propagates; no `release`.
- Context manager exposing only `__exit__` → `TypeError` naming the type at pin time.

## Public-Seam Test Blueprint

Extend `test_sqlspec_shm_proxy.py` with `RecordingAsyncSource` and `FailingAsyncSession`; tests run under the repo's `anyio`/`pytest-asyncio` marker already used in the module.

| Test | Input | Expected |
| --- | --- | --- |
| `test_async_release_clean_releases_once` | no failures | `released == [lease]` |
| `test_async_release_quarantines_on_failure[rollback]` | rollback raises `OSError` | `LeaseQuarantinedError`, `quarantined == [lease]`, `released == []` |
| `test_async_release_quarantines_on_failure[aexit]` | `__aexit__` raises | same |
| `test_async_release_cancelled_rollback_quarantines` | rollback raises `CancelledError` | `CancelledError` raised; `quarantined == [lease]` |
| `test_async_dispatch_execute[params]` | `None`, `{"a": 1}`, `(1, 2)` | awaited calls `("q",)`, `("q", {"a": 1})`, `("q", 1, 2)` |
| `test_async_pin_rejects_sync_only_context` | session context with `__exit__` only | `TypeError` naming the type |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec`
- `git grep -n "inspect\.\|contextlib" src/py/vertebra/extensions/sqlspec/provider.py` prints nothing.

## Acceptance Criteria

- `provider.py` contains no `contextlib.suppress`, `inspect.signature`, or `_extract_lease_coords`.
- All matrix tests pass alongside `t01` tests.

## Notes & Discoveries

- HEAD `_release_session_async` falls back to `__exit__` when `__aexit__` is absent; this task makes that a pin-time `TypeError` instead of a release-time branch.
