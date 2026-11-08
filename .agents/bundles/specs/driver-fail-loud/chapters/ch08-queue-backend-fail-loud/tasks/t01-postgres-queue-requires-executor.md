---
type: Task
id: "driver-fail-loud:ch08:t01"
chapter_id: ch08-queue-backend-fail-loud
title: PostgreSQLQueueBackend requires psycopg and an injected executor
description: Delete the production in-memory PostgreSQL queue executor, raise MissingDependencyError when psycopg is absent and ImproperConfigurationError when no executor is supplied, and move the executor fake into the queue test module.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/tests/unit/framework/test_distributed_queues.py
files:
  - src/py/vertebra/framework/queues/postgres.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 230
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend whose _build_executor always returns the in-memory executor
  - id: pg-fake
    resource: src/py/vertebra/framework/queues/postgres.py::_InMemoryPostgreSQLQueueExecutor
    title: In-memory simulation of PostgreSQL queue SQL, SKIP LOCKED, and LISTEN/NOTIFY
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: postgresql:// and postgres:// queue URLs route to PostgreSQLQueueBackend(config)
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: errors
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error (ValueError subclass) exported from vertebra.typing
  - id: queue-tests
    resource: src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url
    title: Queue contract test backend factory
  - id: queues-flow
    resource: .agents/bundles/specs/queues-tasks-and-channels/spec.md
    title: Native queue flow owning PostgresQueueStore and OracleQueueStore live queue executors
tags:
  - queues
  - postgres
  - fail-loud
---

# Task t01: `PostgreSQLQueueBackend` Requires `psycopg` and an Injected Executor

## Objective & Scope Boundary

`PostgreSQLQueueBackend.__init__` binds a supplied `executor` and otherwise raises: `MissingDependencyError` (`package="psycopg"`, `install_package="psycopg[binary,pool]"`) when `psycopg` is absent, `ImproperConfigurationError` when it is present.[^pg-queue][^module-loader][^errors] `_InMemoryPostgreSQLQueueExecutor` and `_build_executor` are deleted from `src/py/vertebra/framework/queues/postgres.py`; today `_build_executor` returns the in-memory executor on every path (`if psycopg is not None and "localhost" not in self._direct_dsn: pass`), so no live executor exists to keep.[^pg-fake] The executor class moves verbatim into `src/py/tests/unit/framework/test_distributed_queues.py` as `_FakePostgreSQLQueueExecutor`, injected by `_create_backend_by_url` for `postgres` URLs and by `test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn`.[^queue-tests]

`estimated_loc` counts net-new lines only; the 212-line `_InMemoryPostgreSQLQueueExecutor` class moves verbatim from `framework/queues/postgres.py` into `test_distributed_queues.py` and is not counted (roughly 210–220 lines move in each of `t01`, `t02`, and `t03`).

Out of scope: a live PostgreSQL queue executor (owned by the `queues-tasks-and-channels` flow as `PostgresQueueStore`[^queues-flow]), Redis and Spanner queue modules (`t02`, `t03`), `QueueService._build_backend` routing, removing `from __future__ import annotations`, and queue docs (`ch10-queue-docs-and-knowledge`).

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/postgres.py` — imports, `PostgreSQLQueueExecutor`, `_InMemoryPostgreSQLQueueExecutor`, `PostgreSQLQueueBackend.__init__`, `PostgreSQLQueueBackend._build_executor`.[^pg-queue][^pg-fake]
- `src/py/vertebra/framework/queues/base.py` — `QueueService.__init__`, `QueueService._build_backend` (`postgresql` / `postgres` branch).[^queue-factory]
- `src/py/vertebra/utils/module_loader.py` — `ensure_module`, `import_optional` (`sys.modules[name] is None` returns `None`; a cached module is returned from `_module_cache`).[^module-loader]
- `src/py/vertebra/_typing.py` — `MissingDependencyError`, `ImproperConfigurationError`, `reset_dependency_cache`.[^errors]
- `src/py/tests/unit/framework/test_distributed_queues.py` — `_create_backend_by_url`, the five tests parametrized with `"postgresql://localhost:5432/vtb"`, and `test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn`.[^queue-tests]

## Symbol & Signature Contracts

- `src/py/vertebra/framework/queues/postgres.py` imports:
  - remove `import asyncio`, `import collections`, and `from vertebra.utils.module_loader import import_optional` (used only by the deleted class and method);
  - add `from vertebra.typing import ImproperConfigurationError` and `from vertebra.utils.module_loader import ensure_module`;
  - `time`, `uuid`, `msgspec`, and the serializer imports stay (used by `PostgreSQLQueueBackend`).
- Class docstring: `"""PostgreSQL queue backend using FOR UPDATE SKIP LOCKED and LISTEN/NOTIFY over an injected PostgreSQLQueueExecutor."""`
- `PostgreSQLQueueBackend.__init__(self, config: QueueConfig | None = None, *, postgres_config: PostgreSQLConfig | None = None, executor: PostgreSQLQueueExecutor | None = None) -> None`:

```python
self._config = config or QueueConfig()
self._postgres_config = postgres_config
self._direct_dsn = postgres_config.effective_direct_dsn if postgres_config else self._config.backend_url
if executor is None:
    ensure_module("psycopg", package="psycopg", install_package="psycopg[binary,pool]")
    msg = (
        "PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor; "
        "pass executor= to bind a psycopg connection executor"
    )
    raise ImproperConfigurationError(msg)
self._executor: PostgreSQLQueueExecutor = executor
```

  Google docstring: summary `"""Bind the queue backend to an injected PostgreSQLQueueExecutor."""`, `Args:` (`config`, `postgres_config` — source of `direct_dsn` for LISTEN/NOTIFY, `executor`), `Raises:` (`MissingDependencyError` when `executor` is None and `psycopg` is not installed; `ImproperConfigurationError` when `executor` is None and `psycopg` is installed).
- `_build_executor` and `_InMemoryPostgreSQLQueueExecutor` are deleted; `__all__` is unchanged.
- Test module (on top of the `ch02 / t03` imports `sys`, `types`, `uuid`, `ImproperConfigurationError`, `MissingDependencyError`, `reset_dependency_cache`):
  - add `import collections`; add `PostgreSQLQueueExecutor` to the `vertebra.framework.queues` import;
  - `class _FakePostgreSQLQueueExecutor` is the deleted class body moved verbatim (same `__slots__`, `__init__`, `execute`, `fetch_all`, `fetch_one`, `notify`, `wait_for_notify`, `close`), class docstring `"""In-memory PostgreSQL queue executor simulating SKIP LOCKED claims and LISTEN/NOTIFY."""`;
  - `_create_backend_by_url`: `if url.startswith("postgres"): return PostgreSQLQueueBackend(cfg, executor=_FakePostgreSQLQueueExecutor())`; docstring `"""Instantiate queue backends, injecting the Oracle and PostgreSQL executor fakes."""`;
  - `test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn`: `PostgreSQLQueueBackend(postgres_config=pg_config, executor=_FakePostgreSQLQueueExecutor())`.

## Edge Cases & Error Semantics

- `sys.modules["psycopg"] = None`, no `executor`: `MissingDependencyError` with `.package == "psycopg"`, `.install_package == "psycopg[binary,pool]"`, `.hint == "Install it with: uv add psycopg[binary,pool]"`. No `extra=` is passed: `postgres` is not a declared extra (`ch07` gate).
- `sys.modules["psycopg"] = types.ModuleType("psycopg")`, no `executor`: `ImproperConfigurationError` matching `"requires a PostgreSQLQueueExecutor"`. The test calls `reset_dependency_cache("psycopg")` after each `sys.modules` change and in `finally`, because `import_optional` caches a stub in `_module_cache`.
- `QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))` with `psycopg` absent: `MissingDependencyError` propagates from `QueueService._build_backend`.
- `executor=` supplied: no `ensure_module` call; construction succeeds whether or not `psycopg` is installed; `direct_dsn` resolves from `postgres_config.effective_direct_dsn` as before.
- `executor` falsy but not None: bound as given (`is None` replaces `executor or ...`).
- `psycopg` is not in `uv.lock`, so the default environment exercises the missing-module branch unless a test injects a stub.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/framework/test_distributed_queues.py`.
- **Dependency category**: external boundary (`psycopg`), simulated through `sys.modules`; queue SQL behavior exercised through `_FakePostgreSQLQueueExecutor` on the `PostgreSQLQueueExecutor` seam.
- **New test**: `test_postgres_queue_backend_requires_psycopg_and_executor(monkeypatch: pytest.MonkeyPatch) -> None`, wrapping its body in `try: ... finally: reset_dependency_cache("psycopg")`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `psycopg` absent | `monkeypatch.setitem(sys.modules, "psycopg", None)`; `reset_dependency_cache("psycopg")`; `PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))` | `MissingDependencyError`; `err.package == "psycopg"`; `err.install_package == "psycopg[binary,pool]"`; `err.hint == "Install it with: uv add psycopg[binary,pool]"` |
| `psycopg` absent via service | Same `sys.modules`; `QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))` | `MissingDependencyError` with `err.package == "psycopg"` |
| Injected executor, `psycopg` absent | `fake = _FakePostgreSQLQueueExecutor()`; `PostgreSQLQueueBackend(postgres_config=PostgreSQLConfig(dsn="postgresql://pooler:6432/db", direct_dsn="postgresql://primary:5432/db"), executor=fake)` | Constructs; `backend.direct_dsn == "postgresql://primary:5432/db"`; `isinstance(fake, PostgreSQLQueueExecutor)` is `True` |
| `psycopg` present, no executor | `monkeypatch.setitem(sys.modules, "psycopg", types.ModuleType("psycopg"))`; `reset_dependency_cache("psycopg")`; `PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))` | `pytest.raises(ImproperConfigurationError, match="requires a PostgreSQLQueueExecutor")` |
| Queue contract on fake | Existing `test_queue_backends_*["postgresql://localhost:5432/vtb"]` cases and `test_postgres_listen_notify_zero_poll_wakeup_uses_direct_dsn` | Pass unchanged with the injected fake |

## Verification

- Initial Red baseline (fake copied into the test module and new test written; production unchanged, so the absent and present cases fail because the in-memory executor is returned): `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "requires_psycopg" -q`
- Final Green:
  - `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -q`
  - `uv run ruff check src/py/vertebra/framework/queues/postgres.py src/py/tests/unit/framework/test_distributed_queues.py`
  - `uv run mypy src/py/vertebra/framework/queues/postgres.py`
  - `! grep -nE "_InMemoryPostgreSQLQueueExecutor|_build_executor|import_optional" src/py/vertebra/framework/queues/postgres.py`
  - `! grep -rn "_InMemoryPostgreSQLQueueExecutor" src/py/vertebra`

## Acceptance Criteria

- [ ] `PostgreSQLQueueBackend` without `executor` raises `MissingDependencyError` naming `psycopg` with hint `uv add psycopg[binary,pool]` when it is absent (R14).
- [ ] `PostgreSQLQueueBackend` without `executor` raises `ImproperConfigurationError` when `psycopg` is present (R14.1).
- [ ] `_InMemoryPostgreSQLQueueExecutor` and `_build_executor` are absent from `src/py/vertebra`; `_FakePostgreSQLQueueExecutor` lives in `test_distributed_queues.py` and every `postgresql://` contract case plus the LISTEN/NOTIFY test pass (R14.1).
- [ ] `PostgreSQLQueueBackend.__init__` has a Google docstring with `Raises:`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-08 00:00] `PostgreSQLQueueBackend._build_executor` contains `if psycopg is not None and "localhost" not in self._direct_dsn: pass` and returns `_InMemoryPostgreSQLQueueExecutor()` on every branch; no live PostgreSQL queue executor exists in the repository.
- [2026-11-08 00:00] `postgres.py` keeps `from __future__ import annotations`; `PostgreSQLConfig` is a `TYPE_CHECKING`-only annotation, so removal is a separate cleanup.

[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend whose `_build_executor` always returns the in-memory executor.
[^pg-fake]: `src/py/vertebra/framework/queues/postgres.py::_InMemoryPostgreSQLQueueExecutor` — In-memory simulation of PostgreSQL queue SQL, SKIP LOCKED, and LISTEN/NOTIFY.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — `postgresql://` and `postgres://` queue URLs route to `PostgreSQLQueueBackend(config)`.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^errors]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error (`ValueError` subclass) exported from `vertebra.typing`.
[^queue-tests]: `src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url` — Queue contract test backend factory.
[^queues-flow]: `.agents/bundles/specs/queues-tasks-and-channels/spec.md` — Native queue flow owning `PostgresQueueStore` and `OracleQueueStore` live queue executors.
