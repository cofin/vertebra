---
type: Task
id: "broker-pool-sharing:ch06:t03"
chapter_id: ch06-sqlspec-proxy
title: Rebase proxy drivers on SQLSpec adapter bases
description: Make ProxySyncDriver and ProxyAsyncDriver subclass sqlspec.driver.SyncDriverAdapterBase / AsyncDriverAdapterBase, implement the abstract dispatch, transaction, cursor, exception-handler, and data-dictionary members over broker RPC, and make ProxySyncConfig / ProxyAsyncConfig pass the wrapped config's statement_config.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch06:t02"
  - "broker-pool-sharing:ch05:t04"
read_set:
  - src/py/vertebra/extensions/sqlspec/proxy.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - .venv/lib/python3.12/site-packages/sqlspec/driver/_sync.py
  - .venv/lib/python3.12/site-packages/sqlspec/driver/_async.py
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch06-sqlspec-proxy/spec.md
files:
  - src/py/vertebra/extensions/sqlspec/proxy.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
estimated_loc: 250
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
  - id: proxy-sync
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxySyncDriver
    title: Worker-side sync proxy driver
  - id: proxy-async
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxyAsyncDriver
    title: Worker-side async proxy driver
  - id: proxy-config
    resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxySyncConfig
    title: Proxy configs wrapping upstream SQLSpec configs
  - id: sync-base
    resource: .venv/lib/python3.12/site-packages/sqlspec/driver/_sync.py::SyncDriverAdapterBase
    title: Upstream sync adapter base and abstract members
  - id: async-base
    resource: .venv/lib/python3.12/site-packages/sqlspec/driver/_async.py::AsyncDriverAdapterBase
    title: Upstream async adapter base and abstract members
tags:
  - sqlspec
  - proxy
---

# Task t03: Rebase Proxy Drivers on SQLSpec Adapter Bases

## Objective & Scope Boundary

1. `ProxySyncDriver(SyncDriverAdapterBase)` and `ProxyAsyncDriver(AsyncDriverAdapterBase)` in `src/py/vertebra/extensions/sqlspec/proxy.py` [^proxy-sync] [^proxy-async] [^sync-base] [^async-base]. `__init__` calls `super().__init__(connection=self._channel, statement_config=statement_config, driver_features=driver_features)` where `_channel` is the broker RPC channel object (`SyncBrokerChannel` / `AsyncBrokerChannel`, small private classes wrapping the existing `_transport_fn`).
2. Implement every abstract member listed by `SyncDriverAdapterBase.__abstractmethods__` / `AsyncDriverAdapterBase.__abstractmethods__` at the pinned SQLSpec version: `dispatch_execute`, `dispatch_execute_many`, `begin`, `commit`, `rollback`, `with_cursor`, `handle_database_exceptions`, `data_dictionary`, and any further members the executor finds through `__abstractmethods__`. Dispatch sends the compiled SQL (`statement.compile()` output) and converted parameters in the `dbrpc` `execute` / `execute_many` frame; results map to `ExecutionResult` through `self.create_execution_result`.
3. `ProxySyncConfig` / `ProxyAsyncConfig` in `plugin.py` default `statement_config` to `self.config.statement_config` and pass it into the driver; an explicit `statement_config` argument overrides [^proxy-config].
4. Existing public attributes (`pool_name`, `worker_pid`, `last_response`, `provider`, `broker`) remain available; `__slots__` on the subclasses drops names already defined by the base.

Out of scope: provider internals (`t01`, `t02`), dbrpc frame definitions (`ch02 / t03`), session affinity (`ch05 / t04`).

## Bounded Read Manifest

- `proxy.py` — `ProxySyncDriver`, `ProxyAsyncDriver`, `ProxySyncSessionContext`, `ProxyAsyncSessionContext` [^proxy-sync] [^proxy-async].
- `plugin.py` — `_BaseProxyConfig`, `ProxySyncConfig.__init__`, `ProxyAsyncConfig.__init__`, `provide_session` [^proxy-config].
- `sqlspec/driver/_sync.py` — `SyncDriverAdapterBase` abstract members; `sqlspec/driver/_async.py` — `AsyncDriverAdapterBase` [^sync-base] [^async-base].
- `test_sqlspec_shm_proxy.py`.

## Symbol & Signature Contracts

```python
class ProxySyncDriver(SyncDriverAdapterBase):
    def __init__(
        self,
        provider: SQLSpecSyncProvider | Callable[[bytes], bytes],
        broker: SyncBrokerLeaseSource | None = None,
        *,
        statement_config: StatementConfig,
        pool_name: str = "default",
        worker_pid: int | None = None,
        driver_features: dict[str, Any] | None = None,
    ) -> None: ...
    def dispatch_execute(self, cursor: SyncBrokerChannel, statement: SQL) -> ExecutionResult: ...
    def dispatch_execute_many(self, cursor: SyncBrokerChannel, statement: SQL) -> ExecutionResult: ...

class ProxyAsyncDriver(AsyncDriverAdapterBase): ...
```

Import path verified at HEAD: `from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase` (re-exported in `sqlspec/driver/__init__.py`).

## Edge Cases & Error Semantics

- Missing `statement_config` on both the proxy config and the wrapped config → `ImproperConfigurationError` naming the wrapped config type.
- Broker RPC error response → mapped by `handle_database_exceptions` to the SQLSpec exception hierarchy (`sqlspec.exceptions.SQLSpecError` subclasses) with the broker message preserved.
- `LeaseQuarantinedError` from the provider → propagates unchanged (not remapped), so callers can distinguish uncertain outcomes.
- `slot_idx` / `generation` constructor arguments are removed; leases come from the broker.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_proxy_driver_is_adapter_base[sync]` | `ProxySyncConfig(sqlite upstream)` session | `isinstance(driver, SyncDriverAdapterBase)` |
| `test_proxy_driver_is_adapter_base[async]` | `ProxyAsyncConfig(aiosqlite upstream)` | `isinstance(driver, AsyncDriverAdapterBase)` |
| `test_proxy_preserves_statement_config[numeric]` | upstream config with `ParameterStyle.NUMERIC` | `driver.statement_config.parameter_config.default_parameter_style` matches upstream |
| `test_proxy_execute_round_trip` | `select 1 as x` through in-process provider | `result.get_data() == [{"x": 1}]` |
| `test_proxy_execute_many_rowcount` | `insert` with 3 parameter sets | `rows_affected == 3` |
| `test_proxy_quarantine_error_not_remapped` | provider raises `LeaseQuarantinedError` | same exception type at caller |
| `test_proxy_missing_statement_config` | wrapped config lacking `statement_config` | `ImproperConfigurationError` |

Parametrize sync/async variants via a fixture with `params=["sync", "async"]`.

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec`

## Acceptance Criteria

- Both proxy drivers instantiate without `TypeError: Can't instantiate abstract class`.
- Matrix tests pass; existing proxy tests updated to the new constructor without `slot_idx`/`generation`.

## Notes & Discoveries

- `SyncDriverAdapterBase.__init__` (via `CommonDriverAttributesMixin`) requires `connection` and `statement_config`; the RPC channel object fills the `connection` role so SQLSpec's `with_cursor` contract stays meaningful.
