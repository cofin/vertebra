---
type: Task
id: "driver-fail-loud:ch02:t03"
chapter_id: ch02-duckdb-sqlalchemy-queue-fail-loud
title: OracleQueueBackend requires oracledb and an injected executor
description: Delete the production in-memory Oracle queue executor, raise MissingDependencyError when oracledb is absent and ImproperConfigurationError when no executor is supplied, and move the executor fake into the queue test module.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/framework/queues/oracle.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/tests/unit/framework/test_distributed_queues.py
files:
  - src/py/vertebra/framework/queues/oracle.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 230
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:08:00Z"
last_verified_commit: af67511d
verification_evidence: "pytest test_distributed_queues.py (74 passed); ruff and mypy passed"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T03:08:00Z"
commit: af67511d
sources:
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend with in-memory executor fallback
  - id: oracle-fake
    resource: src/py/vertebra/framework/queues/oracle.py::_InMemoryOracleQueueExecutor
    title: In-memory simulation of the Oracle queue SQL used as the fallback executor
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: oracle:// queue URL routes to OracleQueueBackend(config)
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: errors
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error (ValueError subclass) exported from vertebra.typing
  - id: queue-tests
    resource: src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url
    title: Queue contract test backend factory
tags:
  - queues
  - oracle
  - fail-loud
---

# Task t03: `OracleQueueBackend` Requires `oracledb` and an Injected Executor

## Objective & Scope Boundary

`OracleQueueBackend.__init__` binds a supplied `executor` and otherwise raises: `MissingDependencyError` (`package="oracledb"`, `install_package="oracledb"`) when `oracledb` is absent, `ImproperConfigurationError` when it is present.[^oracle-queue][^module-loader][^errors] `_InMemoryOracleQueueExecutor` and `_build_executor` are deleted from `src/py/vertebra/framework/queues/oracle.py`; today `_build_executor` returns the in-memory executor even when `oracledb` is installed, so no live executor exists to fall back to.[^oracle-fake] The executor class moves verbatim into `src/py/tests/unit/framework/test_distributed_queues.py` as `_FakeOracleQueueExecutor`, and `_create_backend_by_url` injects it for `oracle://` URLs.[^queue-tests]

`estimated_loc: 230` counts net-new lines; `_InMemoryOracleQueueExecutor` (~180 lines) moves verbatim from `framework/queues/oracle.py` into the test module and is not counted.

Out of scope: a live Oracle queue executor (owned by `queues-tasks-and-channels`, which builds the durable `OracleQueueStore` and `PostgresQueueStore` queue stores), the `_InMemoryPostgreSQLQueueExecutor`, `_InMemoryRedisStreamClient`, and `_InMemorySpannerQueueExecutor` fallbacks in sibling queue modules (owned by [ch10-queue-backend-fail-loud](../../ch10-queue-backend-fail-loud/spec.md)), `QueueService._build_backend` routing, and queue docs (owned by [ch12-queue-docs-and-knowledge](../../ch12-queue-docs-and-knowledge/spec.md)).

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/oracle.py` — `OracleQueueExecutor`, `_InMemoryOracleQueueExecutor`, `OracleQueueBackend.__init__`, `OracleQueueBackend._build_executor`.[^oracle-queue][^oracle-fake]
- `src/py/vertebra/framework/queues/base.py` — `QueueService._build_backend` (`oracle` scheme branch).[^queue-factory]
- `src/py/vertebra/utils/module_loader.py` — `ensure_module`, `reset_dependency_cache` re-export.[^module-loader]
- `src/py/vertebra/_typing.py` — `MissingDependencyError`, `ImproperConfigurationError`.[^errors]
- `src/py/tests/unit/framework/test_distributed_queues.py` — `_create_backend_by_url` and the five tests parametrized with `"oracle://localhost:1521/FREEPDB1"`.[^queue-tests]

## Symbol & Signature Contracts

- `src/py/vertebra/framework/queues/oracle.py` imports:
  - remove `from vertebra.utils.module_loader import import_optional`;
  - add `from vertebra.typing import ImproperConfigurationError` and `from vertebra.utils.module_loader import ensure_module`.
  - `asyncio`, `time`, `uuid`, and `msgspec` stay (used by `OracleQueueBackend.dequeue`).
- `OracleQueueBackend.__init__(self, config: QueueConfig | None = None, *, oracle_config: Any = None, executor: OracleQueueExecutor | None = None) -> None`:

```python
self._config = config or QueueConfig()
self._oracle_config = oracle_config
if executor is None:
    ensure_module("oracledb", package="oracledb", install_package="oracledb")
    msg = "OracleQueueBackend requires an OracleQueueExecutor; pass executor= to bind an oracledb cursor executor"
    raise ImproperConfigurationError(msg)
self._executor: OracleQueueExecutor = executor
```

  Google docstring: summary, `Args:` (`config`, `oracle_config`, `executor`), `Raises:` (`MissingDependencyError` when `executor` is None and `oracledb` is not installed; `ImproperConfigurationError` when `executor` is None and `oracledb` is installed).
- Class docstring: `"""Oracle Database queue backend using FOR UPDATE SKIP LOCKED over an injected OracleQueueExecutor."""`
- `_build_executor` and `_InMemoryOracleQueueExecutor` are deleted; `__all__` is unchanged (neither name is exported).
- Test module: `class _FakeOracleQueueExecutor` is the deleted class body moved verbatim (same `__slots__`, `execute`, `fetch_all`, `fetch_one`, `commit`, `close`); add `import uuid` and `import sys`, `import types`; import `OracleQueueExecutor` from `vertebra.framework.queues`, and `ImproperConfigurationError`, `MissingDependencyError`, `reset_dependency_cache` from `vertebra.typing`.
- `_create_backend_by_url`: `if url.startswith("oracle"): return OracleQueueBackend(cfg, executor=_FakeOracleQueueExecutor())`; docstring becomes `"""Instantiate queue backends, injecting the Oracle executor fake."""`.

## Edge Cases & Error Semantics

- `sys.modules["oracledb"] = None`, no `executor`: `MissingDependencyError` with `.package == "oracledb"`, `.install_package == "oracledb"`, `.hint == "Install it with: uv add oracledb"`.
- `sys.modules["oracledb"] = types.ModuleType("oracledb")`, no `executor`: `ImproperConfigurationError` matching `"requires an OracleQueueExecutor"`. Tests call `reset_dependency_cache("oracledb")` in `finally`, because `import_optional` caches the stub module in `_module_cache` and would return it after `monkeypatch` restores `sys.modules`.
- `QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` with `oracledb` absent: `MissingDependencyError` propagates from `QueueService._build_backend`.
- `executor=` supplied: no `ensure_module` call; construction succeeds whether or not `oracledb` is installed.
- `executor` falsy but not None: bound as given (`is None` check replaces `executor or ...`).
- `oracledb` is not in `uv.lock`, so the default test environment exercises the missing-module branch unless a test injects a stub.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/framework/test_distributed_queues.py`.
- **Dependency category**: external boundary (`oracledb`); presence and absence simulated through `sys.modules`; queue SQL behavior exercised through the moved `_FakeOracleQueueExecutor` on the `OracleQueueExecutor` seam.
- **New test**: `test_oracle_queue_backend_requires_oracledb_and_executor(monkeypatch: pytest.MonkeyPatch) -> None`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `oracledb` absent | `monkeypatch.setitem(sys.modules, "oracledb", None)`; `reset_dependency_cache("oracledb")`; `OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` | `MissingDependencyError`; `err.package == "oracledb"`; `err.install_package == "oracledb"` |
| `oracledb` absent via service | Same `sys.modules`; `QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` | `MissingDependencyError` with `err.package == "oracledb"` |
| `oracledb` present, no executor | `monkeypatch.setitem(sys.modules, "oracledb", types.ModuleType("oracledb"))`; `reset_dependency_cache("oracledb")`; `OracleQueueBackend(QueueConfig())` | `pytest.raises(ImproperConfigurationError, match="requires an OracleQueueExecutor")` |
| Injected executor | `fake = _FakeOracleQueueExecutor()`; `OracleQueueBackend(QueueConfig(), executor=fake)` with `sys.modules["oracledb"] = None` | Constructs; `isinstance(fake, OracleQueueExecutor)` is `True` |
| Queue contract on fake | Existing `test_queue_backends_fifo_and_lane_isolation["oracle://localhost:1521/FREEPDB1"]` and the four other `oracle://` cases | Pass unchanged through `_create_backend_by_url` |

## Verification

- Initial Red baseline (new test written, production unchanged; absent and present cases fail because the in-memory executor is returned): `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "requires_oracledb" -q`
- Final Green:
  - `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -q`
  - `uv run ruff check src/py/vertebra/framework/queues/oracle.py src/py/tests/unit/framework/test_distributed_queues.py`
  - `uv run mypy src/py/vertebra/framework/queues/oracle.py`
  - `! grep -nE "_InMemoryOracleQueueExecutor|_build_executor|import_optional" src/py/vertebra/framework/queues/oracle.py`
  - `! grep -rn "_InMemoryOracleQueueExecutor" src/py/vertebra`

## Acceptance Criteria

- [x] `OracleQueueBackend` without `executor` raises `MissingDependencyError` naming `oracledb` when it is absent (R5).
- [x] `OracleQueueBackend` without `executor` raises `ImproperConfigurationError` when `oracledb` is present (R5.1).
- [x] `_InMemoryOracleQueueExecutor` and `_build_executor` are absent from `src/py/vertebra`; the fake lives in `test_distributed_queues.py` and every `oracle://` contract case passes (R5.1).
- [x] `OracleQueueBackend.__init__` has a Google docstring with `Raises:`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] `OracleQueueBackend._build_executor` contains `if oracledb is not None: pass` and returns `_InMemoryOracleQueueExecutor()` on both branches; no live `oracledb` executor exists in the repository.
- [2026-11-07 00:00] `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend._build_executor`, `redis.py::RedisQueueBackend`, and `spanner.py::SpannerQueueBackend` carry the same in-memory fallback pattern; [ch10-queue-backend-fail-loud](../../ch10-queue-backend-fail-loud/spec.md) removes them with the same executor-or-raise contract.

[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend with in-memory executor fallback.
[^oracle-fake]: `src/py/vertebra/framework/queues/oracle.py::_InMemoryOracleQueueExecutor` — In-memory simulation of the Oracle queue SQL used as the fallback executor.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — `oracle://` queue URL routes to `OracleQueueBackend(config)`.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^errors]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error (`ValueError` subclass) exported from `vertebra.typing`.
[^queue-tests]: `src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url` — Queue contract test backend factory.
