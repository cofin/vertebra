---
type: Task
id: "broker-pool-sharing:ch06:t01"
chapter_id: ch06-sqlspec-proxy
title: Replace sync provider duck-typing and suppressed release errors with typed seams and quarantine
description: Introduce SyncBrokerLeaseSource and SyncLeasedSession Protocols, delete inspect.signature dispatch in _call_broker_acquire and _call_with_params, and make _release_session_sync / _release_broker_lease quarantine and raise LeaseQuarantinedError on uncertain outcomes.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "broker-pool-sharing:ch05:t01"
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
estimated_loc: 220
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
  - id: call-acquire
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._call_broker_acquire
    title: Signature-introspecting broker acquire dispatch
  - id: release-sync
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._release_session_sync
    title: Sync session release with suppressed rollback and exit errors
  - id: call-params
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._call_with_params
    title: Signature-introspecting parameter dispatch
  - id: sync-lease
    resource: src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease
    title: Sync connection lease
tags:
  - sqlspec
  - quarantine
  - fail-loud
---

# Task t01: Replace Sync Provider Duck-Typing and Suppressed Release Errors

## Objective & Scope Boundary

In `src/py/vertebra/extensions/sqlspec/provider.py`:

1. Add `SyncBrokerLeaseSource` and `SyncLeasedSession` as `@runtime_checkable` `typing.Protocol` classes exported in `__all__` (chapter spec section 2.2).
2. Replace `_BaseSQLSpecProvider._call_broker_acquire` with a direct `source.acquire(pool_name=pool_name, worker_pid=pid)` call; the sync acquire path checks `isinstance(source, SyncBrokerLeaseSource)` once at provider construction and raises `ImproperConfigurationError` naming the broker type otherwise [^call-acquire].
3. Replace `_call_with_params` with a static dispatch: `None`, `()`, `[]` → `execute(sql)`; `dict` → `execute(sql, params)`; `tuple`/`list` → `execute(sql, *params)`; any other type → `execute(sql, params)` [^call-params]. `_invoke_sync` takes a `SyncLeasedSession` and calls `execute` or `execute_many` by operation, removing the `method_names` probing loop.
4. Rewrite `_release_session_sync` and `_release_broker_lease` so rollback and context-exit failures call `source.quarantine(lease)` and raise `LeaseQuarantinedError(...) from exc`; the normal path calls `lease.release()` only [^release-sync] [^sync-lease]. Delete the `release_lease`/`transport.release` fallbacks.
5. Remove `inspect` and `contextlib` imports when no longer used by sync paths; async paths remain for `t02`.

Out of scope: async paths (`t02`), proxy drivers (`t03`), quarantine bookkeeping in the transport (`ch05 / t01`).

## Bounded Read Manifest

- `provider.py` — `_BaseSQLSpecProvider` acquire helpers, `_extract_lease_coords`, `_release_session_sync`, `_release_broker_lease`, `_invoke_sync`, `_call_with_params`, `_exec_rollback_sync`, `SQLSpecSyncProvider` [^call-acquire] [^release-sync] [^call-params].
- `transport.py` — `SyncConnectionLease`, `SyncSHMBrokerTransport` quarantine method from `ch05 / t01` [^sync-lease].
- `extensions/broker/errors.py` — `LeaseQuarantinedError` (`ch05 / t01`).
- `test_sqlspec_shm_proxy.py` — existing provider fakes and fixtures.

## Symbol & Signature Contracts

```python
@runtime_checkable
class SyncBrokerLeaseSource(Protocol):
    def acquire(self, *, pool_name: str, worker_pid: int) -> SyncConnectionLease: ...
    def quarantine(self, lease: SyncConnectionLease) -> None: ...

@runtime_checkable
class SyncLeasedSession(Protocol):
    def execute(self, statement: str, /, *parameters: Any) -> Any: ...
    def execute_many(self, statement: str, parameters: Sequence[Any], /) -> Any: ...
    def rollback(self) -> None: ...

def _release_session_sync(self, key: tuple[int, int], in_tx: bool) -> None: ...
def _release_broker_lease(self, lease: SyncConnectionLease, key: tuple[int, int]) -> None: ...
@staticmethod
def _dispatch_execute(session: SyncLeasedSession, sql: str, params: Any) -> Any: ...
```

`LeaseQuarantinedError` message: `f"lease slot={lease.slot_index} generation={lease.generation} pool={lease.bind_key!r} quarantined after {type(exc).__name__}"`.

## Edge Cases & Error Semantics

- Rollback raises → one `quarantine(lease)`, zero `release()`, `LeaseQuarantinedError.__cause__` is the rollback exception.
- Rollback succeeds, `__exit__` raises → same as above.
- `quarantine` raises → that exception propagates; `__context__` is the original failure; no `release()`.
- `lease.release()` raises on the clean path → propagates unchanged.
- No pinned session for `key` → no-op (existing behavior retained).
- Broker missing `quarantine` → `ImproperConfigurationError` at construction, not at release.

## Public-Seam Test Blueprint

Add to `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py` a `RecordingSyncSource` fake implementing the Protocol (records `acquired`, `quarantined`, `released`) and a `FailingSession` parametrized by failure point.

| Test | Input | Expected |
| --- | --- | --- |
| `test_sync_release_clean_releases_once` | in_tx=False, no failures | `released == [lease]`, `quarantined == []` |
| `test_sync_release_quarantines_on_failure[rollback]` | rollback raises `OSError` | `LeaseQuarantinedError`, `__cause__` is `OSError`, `quarantined == [lease]`, `released == []` |
| `test_sync_release_quarantines_on_failure[exit]` | `__exit__` raises `RuntimeError` | same with `RuntimeError` |
| `test_sync_release_quarantine_failure_propagates` | rollback and quarantine raise | quarantine exception raised; `released == []` |
| `test_sync_dispatch_execute[params]` | `None`, `()`, `{"a": 1}`, `(1, 2)`, `[1, 2]` | recorded calls `("q",)`, `("q",)`, `("q", {"a": 1})`, `("q", 1, 2)`, `("q", 1, 2)` |
| `test_sync_provider_rejects_untyped_broker` | broker without `quarantine` | `ImproperConfigurationError` naming broker type |

Use `@pytest.mark.parametrize` for failure points and parameter shapes.

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py -q`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/provider.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/provider.py`
- `git grep -n "inspect.signature" src/py/vertebra/extensions/sqlspec/provider.py` prints nothing.

## Acceptance Criteria

- Sync release paths contain no `contextlib.suppress`.
- `_call_broker_acquire` and `_call_with_params` no longer exist.
- Every test in the matrix passes; existing proxy tests stay green.

## Notes & Discoveries

- `_extract_lease_coords` accepts tuples and mints generations when absent; with typed leases the provider reads `lease.slot_index` and `lease.generation` directly, and `t02` removes the helper once async paths no longer need it.
