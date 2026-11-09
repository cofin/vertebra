---
type: Task
id: "driver-fail-loud:ch10:t03"
chapter_id: ch10-queue-backend-fail-loud
title: SpannerQueueBackend requires the Spanner extension and an injected executor
description: Delete the production in-memory and driver-wrapped Spanner queue executors, the driver= parameter, and the memory scheme allowance, raise MissingDependencyError when vertebra._spanner is absent and ImproperConfigurationError when no executor is supplied, and move the executor fake into the Spanner queue test module.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/framework/queues/spanner.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
files:
  - src/py/vertebra/framework/queues/spanner.py
  - src/py/vertebra/framework/queues/base.py
tests:
  - src/py/tests/unit/db/test_spanner_queue_and_cdc.py
estimated_loc: 240
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T15:00:30Z"
last_verified_commit: f4d9361d
verification_evidence: "pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q passed; ruff and mypy clean"
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-09T15:00:30Z"
commit: f4d9361d
sources:
  - id: spanner-queue
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: Spanner queue backend selecting an injected, driver-wrapped, or in-memory executor
  - id: spanner-fake
    resource: src/py/vertebra/framework/queues/spanner.py::_InMemorySpannerQueueExecutor
    title: In-memory simulation of hash-sharded Spanner queue SQL
  - id: spanner-driver-exec
    resource: src/py/vertebra/framework/queues/spanner.py::_DriverSpannerQueueExecutor
    title: Driver wrapper that runs DDL once and routes every DML and query to an in-memory mirror
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: spanner, spanner+googlesql, spangres, and spanner+spangres queue URLs route to SpannerQueueBackend(config)
  - id: queue-config
    resource: src/py/vertebra/framework/queues/base.py::QueueConfig
    title: Queue configuration struct whose docstring docs/reference/queues.md renders
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: errors
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error whose install_package defaults to vertebra-core[extra]
  - id: spanner-adapter
    resource: src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver
    title: Spanner driver resolving vertebra._spanner with package vertebra-core[spanner] and extra spanner
  - id: spanner-tests
    resource: src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_queue_backend_lifecycle_dlq_and_leases
    title: Spanner queue lifecycle test using driver= and a spanner+googlesql QueueService
tags:
  - queues
  - spanner
  - fail-loud
---

# Task t03: `SpannerQueueBackend` Requires the Spanner Extension and an Injected Executor

## Objective & Scope Boundary

`SpannerQueueBackend.__init__` validates the URL scheme, then binds a supplied `executor` and otherwise raises: `MissingDependencyError` (`package="vertebra-core[spanner]"`, `install_package="vertebra-core[spanner]"`) when `vertebra._spanner` is absent, `ImproperConfigurationError` when it is present.[^spanner-queue][^module-loader][^errors] No live Spanner executor exists: `_DriverSpannerQueueExecutor` runs DDL once with every exception swallowed, runs a discarded `SELECT` for claims with every exception swallowed, and answers every `execute`, `fetch_all`, and `fetch_one` from an embedded `_InMemorySpannerQueueExecutor`.[^spanner-driver-exec][^spanner-fake] Both classes, the `driver=` parameter, the `_driver` slot, and the `memory` scheme allowance are deleted from `src/py/vertebra/framework/queues/spanner.py`. The in-memory class moves verbatim into `src/py/tests/unit/db/test_spanner_queue_and_cdc.py` as `_FakeSpannerQueueExecutor`, injected into the lifecycle test's backend and its `QueueService`.[^spanner-tests]

`estimated_loc` counts net-new lines only; the 210-line `_InMemorySpannerQueueExecutor` class moves verbatim from `framework/queues/spanner.py` into `test_spanner_queue_and_cdc.py` and is not counted (roughly 210–220 lines move in each of `t01`, `t02`, and `t03`).

The task also updates the `QueueConfig` class docstring in `src/py/vertebra/framework/queues/base.py` to name Spanner, because `docs/reference/queues.md` renders it through `::: vertebra.framework.queues.base.QueueConfig`.[^queue-config]

Out of scope: a live executor over `SyncSpannerDriver.run_in_transaction` (no owning flow; follow-up spec via `/flow:plan spanner-queue-executor`), `SpannerChangeStreamSource` and the CDC test, `QueueService._build_backend` routing, DDL and SQL constants, removing `from __future__ import annotations`, and queue docs (`ch12-queue-docs-and-knowledge`).

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/spanner.py` — imports, `SpannerQueueExecutor`, `_InMemorySpannerQueueExecutor`, `_DriverSpannerQueueExecutor`, `SpannerQueueBackend.__slots__`, `SpannerQueueBackend.__init__`.[^spanner-queue][^spanner-fake][^spanner-driver-exec]
- `src/py/vertebra/framework/queues/base.py` — `QueueService.__init__(config, *, backend=None)`, `QueueService._build_backend` (Spanner scheme branch).[^queue-factory]
- `src/py/vertebra/utils/module_loader.py` — `ensure_module`, `import_optional`.[^module-loader]
- `src/py/vertebra/_typing.py` — `MissingDependencyError` (post-`ch08` default `install_package = f"vertebra-core[{extra}]"`), `ImproperConfigurationError`, `reset_dependency_cache`.[^errors]
- `src/py/tests/unit/db/test_spanner_queue_and_cdc.py` — imports and `test_spanner_queue_backend_lifecycle_dlq_and_leases`; `FakeSpannerClient`, `SyncSpannerDriver`, and `SpannerConfig` stay imported for `test_spanner_change_stream_source_partitions_and_object_store_spill`.[^spanner-tests]

## Symbol & Signature Contracts

- `src/py/vertebra/framework/queues/base.py::QueueConfig` — class docstring becomes `"""Configuration for QueueService across memory, Redis, Valkey, PostgreSQL, Oracle, and Spanner backends."""`; fields and every other line of `base.py` are unchanged.[^queue-config]
- `src/py/vertebra/framework/queues/spanner.py` imports:
  - remove `from contextlib import suppress` (used only by `_DriverSpannerQueueExecutor`);
  - add `from vertebra.utils.module_loader import ensure_module`;
  - `asyncio` (used by `dequeue`), `hashlib`, `time`, `urllib.parse`, `msgspec`, `ImproperConfigurationError`, `uuid4`, and the serializer imports stay.
- `SpannerQueueBackend.__slots__ = ("_cleanup_on_ack", "_config", "_dialect", "_executor", "_receipt_index", "_shard_count")`.
- `SpannerQueueBackend.__init__(self, config: QueueConfig | None = None, *, executor: SpannerQueueExecutor | None = None, dialect: Literal["googlesql", "spangres"] = "googlesql", shard_count: int = 16, cleanup_on_ack: bool = True) -> None`:

```python
resolved_config = config or QueueConfig(backend_url="spanner://projects/local/instances/inst/databases/db")
scheme = urllib.parse.urlparse(resolved_config.backend_url).scheme.lower()
if scheme and scheme not in {"spanner", "spanner+googlesql", "spangres", "spanner+spangres"}:
    raise ImproperConfigurationError(
        f"SpannerQueueBackend requires a spanner:// backend_url, got {resolved_config.backend_url!r}"
    )
if scheme in {"spangres", "spanner+spangres"}:
    dialect = "spangres"
self._config = resolved_config
self._dialect: Literal["googlesql", "spangres"] = dialect
self._shard_count = max(1, shard_count)
self._cleanup_on_ack = cleanup_on_ack
self._receipt_index: dict[str, TaskEnvelope] = {}
if executor is None:
    ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")
    msg = (
        "SpannerQueueBackend requires a SpannerQueueExecutor; "
        "pass executor= to bind a Spanner transaction executor"
    )
    raise ImproperConfigurationError(msg)
self._executor: SpannerQueueExecutor = executor
```

  Google docstring: existing summary and `Args:` minus `driver`, with `executor: SpannerQueueExecutor bound as given; required.`; add `Raises:` (`ImproperConfigurationError` when `backend_url` is not a Spanner scheme, or when `executor` is None and `vertebra._spanner` is installed; `MissingDependencyError` when `executor` is None and `vertebra._spanner` is not installed).
- The `ensure_module` literals match `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver`; `spanner` is a declared extra, so the `ch08` gate passes.[^spanner-adapter]
- `_InMemorySpannerQueueExecutor` and `_DriverSpannerQueueExecutor` are deleted; `__all__` is unchanged.
- Test module `test_spanner_queue_and_cdc.py`:
  - add `import asyncio`, `import sys`, `import types`; add `SpannerQueueExecutor` to the `vertebra.framework.queues` import; add `from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache` and `from vertebra.utils.uuids import uuid4`;
  - `class _FakeSpannerQueueExecutor` is the deleted `_InMemorySpannerQueueExecutor` body moved verbatim (same `__slots__`, `__init__`, `execute`, `fetch_all`, `fetch_one`, `close`), class docstring `"""In-memory Spanner queue executor simulating hash-sharded FOR UPDATE claims."""`;
  - `test_spanner_queue_backend_lifecycle_dlq_and_leases`: drop the `driver = SyncSpannerDriver(...)` line and the trailing `driver.close()`; construct `backend = SpannerQueueBackend(cfg, executor=_FakeSpannerQueueExecutor(), shard_count=16)`; replace the service line with `service_cfg = QueueConfig(backend_url="spanner+googlesql://projects/p/instances/i/databases/d")` and `service = QueueService(service_cfg, backend=SpannerQueueBackend(service_cfg, executor=_FakeSpannerQueueExecutor()))`; docstring becomes `"""Verify SpannerQueueBackend hash sharding, DDL, enqueue/dequeue/ack/nack, DLQ, and CronLease on an injected executor."""`.

## Edge Cases & Error Semantics

- `sys.modules["vertebra._spanner"] = None`, no `executor`: `MissingDependencyError` with `.package == "vertebra-core[spanner]"`, `.install_package == "vertebra-core[spanner]"`, `.hint == "Install it with: uv add vertebra-core[spanner]"` (the `install_package` value depends on the `ch08` default).
- `sys.modules["vertebra._spanner"] = types.ModuleType("vertebra._spanner")`, no `executor`: `ImproperConfigurationError` matching `"requires a SpannerQueueExecutor"`. `make dev` builds the real extension, so both cases stub `sys.modules`; the test calls `reset_dependency_cache("vertebra._spanner")` after each change and in `finally`, and `monkeypatch` restores the real module.
- `QueueConfig()` (`memory://`) with an injected executor: `ImproperConfigurationError` matching `"requires a spanner:// backend_url"`, raised before any module resolution, so the result does not depend on the environment.
- `QueueConfig(backend_url="")`: empty scheme passes the scheme check (unchanged).
- `spangres://` and `spanner+spangres://` URLs set `dialect == "spangres"` regardless of the `dialect` argument (unchanged).
- `driver=` keyword: `TypeError` from Python; no caller under `src/py/vertebra` passes it.
- `executor` falsy but not None: bound as given (`is None`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_spanner_queue_and_cdc.py`.
- **Dependency category**: external boundary (`vertebra._spanner`, `spanner` extra), simulated through `sys.modules`; queue SQL behavior exercised through `_FakeSpannerQueueExecutor` on the `SpannerQueueExecutor` seam.
- **New test**: `test_spanner_queue_backend_requires_spanner_extension_and_executor(monkeypatch: pytest.MonkeyPatch) -> None`, wrapping its body in `try: ... finally: reset_dependency_cache("vertebra._spanner")`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Extension absent | `monkeypatch.setitem(sys.modules, "vertebra._spanner", None)`; `reset_dependency_cache("vertebra._spanner")`; `SpannerQueueBackend(QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d"))` | `MissingDependencyError`; `err.package == "vertebra-core[spanner]"`; `err.install_package == "vertebra-core[spanner]"`; `err.hint == "Install it with: uv add vertebra-core[spanner]"` |
| Extension absent via service | Same `sys.modules`; `QueueService(QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d"))` | `MissingDependencyError` with `err.package == "vertebra-core[spanner]"` |
| Injected executor, extension absent, spangres URL | `fake = _FakeSpannerQueueExecutor()`; `SpannerQueueBackend(QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d"), executor=fake)` | Constructs; `backend.dialect == "spangres"`; `isinstance(fake, SpannerQueueExecutor)` is `True` |
| Extension present, no executor | `monkeypatch.setitem(sys.modules, "vertebra._spanner", types.ModuleType("vertebra._spanner"))`; `reset_dependency_cache("vertebra._spanner")`; `SpannerQueueBackend()` | `pytest.raises(ImproperConfigurationError, match="requires a SpannerQueueExecutor")` |
| `memory://` URL | `SpannerQueueBackend(QueueConfig(), executor=_FakeSpannerQueueExecutor())` | `pytest.raises(ImproperConfigurationError, match="requires a spanner:// backend_url")` |
| Lifecycle on fake | Existing `test_spanner_queue_backend_lifecycle_dlq_and_leases` with the injected executor | Passes: one claim after a duplicate idempotent enqueue, `attempt == 2` on reclaim, DLQ replay, lease contention, and `QueueService.drain_once(("cpu",)) == [49]` |

## Verification

- Initial Red baseline (fake copied into the test module and new test written; production unchanged, so the absent, present, and `memory://` cases fail because the in-memory executor is returned): `uv run pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -k "requires_spanner_extension" -q`
- Final Green:
  - `uv run pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q`
  - `uv run ruff check src/py/vertebra/framework/queues/spanner.py src/py/vertebra/framework/queues/base.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py`
  - `uv run mypy src/py/vertebra/framework/queues/spanner.py src/py/vertebra/framework/queues/base.py`
  - `grep -n 'Configuration for QueueService across memory, Redis, Valkey, PostgreSQL, Oracle, and Spanner backends.' src/py/vertebra/framework/queues/base.py` prints one line
  - `! grep -nE "_InMemorySpannerQueueExecutor|_DriverSpannerQueueExecutor|_driver|\"memory\"|suppress" src/py/vertebra/framework/queues/spanner.py`
  - `! grep -rnE "_InMemorySpannerQueueExecutor|_DriverSpannerQueueExecutor" src/py/vertebra`

## Acceptance Criteria

- [x] `SpannerQueueBackend` without `executor` raises `MissingDependencyError` naming `vertebra-core[spanner]` when `vertebra._spanner` is absent (R16).
- [x] `SpannerQueueBackend` without `executor` raises `ImproperConfigurationError` when `vertebra._spanner` is present, and rejects `memory://` URLs (R16.1).
- [x] `_InMemorySpannerQueueExecutor`, `_DriverSpannerQueueExecutor`, `driver=`, and `_driver` are absent from `src/py/vertebra`; `_FakeSpannerQueueExecutor` lives in `test_spanner_queue_and_cdc.py` and the lifecycle test passes on it (R16.1).
- [x] `QueueConfig`'s docstring names memory, Redis, Valkey, PostgreSQL, Oracle, and Spanner backends.
- [x] `SpannerQueueBackend.__init__` has a Google docstring with `Raises:`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-08 00:00] `_DriverSpannerQueueExecutor.fetch_all` runs `SELECT shard_id, task_id ... LIMIT @limit` inside `run_in_transaction`, discards the rows, and returns the in-memory mirror's claim; `execute` and `fetch_one` never call the driver. Passing `driver=` produced in-process queue semantics identical to passing nothing.
- [2026-11-08 00:00] `test_spanner_queue_backend_lifecycle_dlq_and_leases` built `SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())` only to satisfy `driver=`; the CDC test in the same module keeps those imports.

[^spanner-queue]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend selecting an injected, driver-wrapped, or in-memory executor.
[^spanner-fake]: `src/py/vertebra/framework/queues/spanner.py::_InMemorySpannerQueueExecutor` — In-memory simulation of hash-sharded Spanner queue SQL.
[^spanner-driver-exec]: `src/py/vertebra/framework/queues/spanner.py::_DriverSpannerQueueExecutor` — Driver wrapper that runs DDL once and routes every DML and query to an in-memory mirror.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — `spanner`, `spanner+googlesql`, `spangres`, and `spanner+spangres` queue URLs route to `SpannerQueueBackend(config)`.
[^queue-config]: `src/py/vertebra/framework/queues/base.py::QueueConfig` — Queue configuration struct whose docstring `docs/reference/queues.md` renders.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^errors]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error whose `install_package` defaults to `vertebra-core[extra]`.
[^spanner-adapter]: `src/py/vertebra/db/adapters/spanner/driver.py::SyncSpannerDriver` — Spanner driver resolving `vertebra._spanner` with package `vertebra-core[spanner]` and extra `spanner`.
[^spanner-tests]: `src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_queue_backend_lifecycle_dlq_and_leases` — Spanner queue lifecycle test using `driver=` and a `spanner+googlesql` `QueueService`.
