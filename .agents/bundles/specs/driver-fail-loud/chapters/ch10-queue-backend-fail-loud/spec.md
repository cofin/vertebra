---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch10-queue-backend-fail-loud
title: "Chapter 10: PostgreSQL, Redis, and Spanner Queue Backend Fail-Loud Paths"
description: PostgreSQLQueueBackend, RedisQueueBackend / ValkeyQueueBackend, and SpannerQueueBackend raise MissingDependencyError or ImproperConfigurationError instead of substituting in-memory executors or clients, and the fakes move into the queue test modules as injected doubles.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-duckdb-sqlalchemy-queue-fail-loud
  - ch08-install-hints-and-extras-gate
replan_trigger: null
current_task: "driver-fail-loud:ch10:t01"
plan_revision: 1
plan_commit: 1e1f028a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend whose _build_executor always returns the in-memory executor
  - id: pg-fake
    resource: src/py/vertebra/framework/queues/postgres.py::_InMemoryPostgreSQLQueueExecutor
    title: In-memory simulation of PostgreSQL queue SQL, SKIP LOCKED, and LISTEN/NOTIFY
  - id: redis-queue
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client
    title: Redis client factory that substitutes the in-memory client when redis.asyncio is missing or the URL has no host
  - id: redis-fake
    resource: src/py/vertebra/framework/queues/redis.py::_InMemoryRedisStreamClient
    title: In-memory emulator of Redis Streams, sorted sets, and the queue Lua scripts
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
    title: Queue URL scheme routing to backend constructors without an executor or client
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: errors
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and ImproperConfigurationError exported from vertebra.typing
  - id: extras
    resource: pyproject.toml::project.optional-dependencies
    title: Declared extras; spanner is declared, redis and postgres are not
  - id: queue-tests
    resource: src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url
    title: Queue contract test backend factory
  - id: email-tests
    resource: src/py/tests/unit/framework/test_email_providers.py::test_email_service_enqueue_and_enqueue_batch_via_queue_service
    title: Email service queue test parametrized over memory:// and redis://
  - id: spanner-tests
    resource: src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_queue_backend_lifecycle_dlq_and_leases
    title: Spanner queue lifecycle test using driver= and a spanner+googlesql QueueService
  - id: oracle-task
    resource: .agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t03-oracle-queue-requires-oracledb.md
    title: Oracle queue fail-loud worksheet whose executor-or-raise contract this chapter mirrors
  - id: queue-docs
    resource: docs/usage/queues-and-realtime.md
    title: Queue backend table listing PostgreSQL, Redis, Valkey, Oracle, and Cloud Spanner
  - id: queues-flow
    resource: .agents/bundles/specs/queues-tasks-and-channels/spec.md
    title: Native queue flow owning PostgresQueueStore and OracleQueueStore live queue executors
  - id: queue-knowledge
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: OKF chapter describing queue backends and their executors
tags:
  - queues
  - postgres
  - redis
  - valkey
  - spanner
  - fail-loud
---

# Chapter 10: PostgreSQL, Redis, and Spanner Queue Backend Fail-Loud Paths

<!-- truth: start -->
- **PostgreSQL queue requires an executor**: `PostgreSQLQueueBackend` without `executor=` calls `ensure_module("psycopg", package="psycopg", install_package="psycopg[binary,pool]")` and then raises `ImproperConfigurationError`; `_InMemoryPostgreSQLQueueExecutor` and `_build_executor` are absent from `src/py/vertebra`.[^pg-queue][^pg-fake][^module-loader]
- **Redis and Valkey queues bind a live client or raise**: `RedisQueueBackend._build_client` calls `ensure_module("redis.asyncio", package="redis", install_package="redis")`, raises `ImproperConfigurationError` when `backend_url` has no hostname, and otherwise returns `redis.asyncio.from_url(backend_url)`; `_InMemoryRedisStreamClient` is absent from `src/py/vertebra`.[^redis-queue][^redis-fake]
- **Spanner queue requires an executor**: `SpannerQueueBackend` without `executor=` calls `ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")` and then raises `ImproperConfigurationError`; `_InMemorySpannerQueueExecutor`, `_DriverSpannerQueueExecutor`, the `driver=` parameter, and the `memory` scheme allowance are deleted.[^spanner-queue][^spanner-fake][^spanner-driver-exec]
- **Injected doubles are bound as given**: an `executor=` or `client=` that is not `None` skips module resolution and URL checks (`is None`, never truthiness).[^pg-queue][^redis-queue][^spanner-queue]
- **Fakes live in tests**: `_FakePostgreSQLQueueExecutor` and `FakeRedisStreamClient` live in `test_distributed_queues.py`; `_FakeSpannerQueueExecutor` lives in `test_spanner_queue_and_cdc.py`; `test_email_providers.py` imports `FakeRedisStreamClient`.[^queue-tests][^email-tests][^spanner-tests]
- **No undeclared extras**: `install_package` names pip packages for `psycopg` and `redis`; only Spanner passes `extra="spanner"`, which `[project.optional-dependencies]` declares.[^extras]
- **`memory://` stays real**: `QueueService` routes `""` and `memory` schemes to `MemoryQueueBackend`, unchanged.[^queue-factory]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Three queue backends substitute an in-process simulation when their live dependency or configuration is missing, so a production `QueueConfig(backend_url=...)` can run on a dictionary that loses every task at process exit:

- `PostgreSQLQueueBackend._build_executor` imports `psycopg`, discards it (`if psycopg is not None and "localhost" not in self._direct_dsn: pass`), and returns `_InMemoryPostgreSQLQueueExecutor()` on every path.[^pg-queue][^pg-fake]
- `RedisQueueBackend._build_client` returns `redis.asyncio.from_url(backend_url)` only when `redis.asyncio` imports and the URL has a hostname; otherwise it returns `_InMemoryRedisStreamClient()`. `ValkeyQueueBackend` inherits this, and its default `QueueConfig` (`backend_url="memory://"`) always selects the emulator.[^redis-queue][^redis-fake]
- `SpannerQueueBackend` uses `_InMemorySpannerQueueExecutor` when neither `executor=` nor `driver=` is given. With `driver=`, it uses `_DriverSpannerQueueExecutor`, which issues DDL once (swallowing every exception), runs a discarded `SELECT` inside `run_in_transaction` for claims (swallowing every exception), and routes every `execute`, `fetch_all`, and `fetch_one` result to an embedded `_InMemorySpannerQueueExecutor`. No row reaches Spanner.[^spanner-queue][^spanner-fake][^spanner-driver-exec]

`QueueService._build_backend` constructs each backend from the URL alone, so `postgresql://`, `redis://`, `valkey://`, and `spanner://` queue URLs reach these substitutes.[^queue-factory] This chapter applies the `ch02` Oracle queue contract to all three: bind what the caller injects, otherwise raise `MissingDependencyError` for the missing package and `ImproperConfigurationError` for the missing executor or host.[^oracle-task] Redis keeps its live `redis.asyncio` path; PostgreSQL and Spanner have no live executor, so `executor=` becomes mandatory.

### 1.2 Non-Goals

- Live PostgreSQL and Oracle queue executors: the `queues-tasks-and-channels` flow owns them as `PostgresQueueStore` (`vertebra/db/adapters/postgres/queue.py`) and `OracleQueueStore` (`vertebra/db/adapters/oracle/queue.py`).[^queues-flow]
- A live Spanner queue executor over `SyncSpannerDriver` / `AsyncSpannerDriver` transactions: no flow owns it; it is a follow-up spec opened with `/flow:plan spanner-queue-executor`.
- Mapping `valkey://` / `valkeys://` to schemes `redis.asyncio.from_url` accepts, and `unix://` socket URLs for `RedisQueueBackend` (recorded in `t02` Notes as a follow-up).
- `OracleQueueBackend` fail-loud construction (owned by `ch02 / t03`) and `MemoryQueueBackend` (`memory://`, preserved).
- Removing `from __future__ import annotations` from touched modules (separate cleanup; `TYPE_CHECKING`-only annotations need quoting first).
- User docs and OKF prose (owned by [ch12-queue-docs-and-knowledge](../ch12-queue-docs-and-knowledge/spec.md); see Section 3).

### 1.3 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent flow [`driver-fail-loud`](../../spec.md).
- Depends on `ch02-duckdb-sqlalchemy-queue-fail-loud`: `ch02 / t03` adds `_FakeOracleQueueExecutor`, `import sys`, `import types`, `import uuid`, and the `ImproperConfigurationError` / `MissingDependencyError` / `reset_dependency_cache` imports to `test_distributed_queues.py`, which `t01` and `t02` extend.[^oracle-task]
- Depends on [ch08-install-hints-and-extras-gate](../ch08-install-hints-and-extras-gate/spec.md): `ch08 / t01` defaults `MissingDependencyError.install_package` to `vertebra-core[<extra>]`, and the `ch08 / t04` extras gate in `test_packaging_and_pgo.py` rejects undeclared `extra=` literals and whitespace in `package=` / `install_package=` literals under `src/py/vertebra`, so every literal this chapter adds runs through that gate.[^errors][^extras]
- Does not depend on `ch09-conformance-outcomes`: no file overlaps; this chapter edits no conformance or integration fixture file.
- `uv.lock` resolves none of `psycopg`, `redis`, or `oracledb`; the default test environment exercises the missing-module branches. The debug `vertebra._spanner` extension is built by `make dev`, so Spanner tests stub both presence and absence through `sys.modules`.

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| PostgreSQL queue | `framework/queues/postgres.py::PostgreSQLQueueBackend`, `_InMemoryPostgreSQLQueueExecutor`[^pg-queue][^pg-fake] | External boundary (`psycopg`); `PostgreSQLQueueExecutor` protocol is the injected seam |
| Redis / Valkey queue | `framework/queues/redis.py::RedisQueueBackend._build_client`, `_InMemoryRedisStreamClient`, `ValkeyQueueBackend`[^redis-queue][^redis-fake] | External boundary (`redis.asyncio`); `RedisStreamClient` protocol is the injected seam; live path via `from_url` |
| Spanner queue | `framework/queues/spanner.py::SpannerQueueBackend`, `_InMemorySpannerQueueExecutor`, `_DriverSpannerQueueExecutor`[^spanner-queue][^spanner-fake][^spanner-driver-exec] | External boundary (`vertebra._spanner`, `spanner` extra); `SpannerQueueExecutor` protocol is the injected seam |
| URL routing | `framework/queues/base.py::QueueService._build_backend`, `QueueService.__init__(config, *, backend=None)`[^queue-factory] | In-process; `backend=` injects a pre-built backend |
| Optional modules | `utils/module_loader.py::ensure_module`[^module-loader], `_typing.py::MissingDependencyError`, `ImproperConfigurationError`, `reset_dependency_cache`[^errors] | `sys.modules[name] = None` yields absence; a `types.ModuleType` stub yields presence; `reset_dependency_cache(name)` clears `_module_cache` |
| Tests | `test_distributed_queues.py`[^queue-tests], `test_email_providers.py`[^email-tests], `test_spanner_queue_and_cdc.py`[^spanner-tests] | Canonical module test files; no new test modules |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Distributed queues and email](../../../../knowledge/domains/framework/distributed-queues-and-email.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [`python-framework` skill](../../../../../skills/python-framework/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R14**: `PostgreSQLQueueBackend` without `executor=` raises `MissingDependencyError` (`package="psycopg"`, `install_package="psycopg[binary,pool]"`) when `psycopg` is absent.
- **R14.1**: With `psycopg` importable and no `executor=`, `PostgreSQLQueueBackend` raises `ImproperConfigurationError`; `_InMemoryPostgreSQLQueueExecutor` and `_build_executor` are deleted from production and the executor fake lives in `test_distributed_queues.py`.
- **R15**: `RedisQueueBackend` and `ValkeyQueueBackend` without `client=` raise `MissingDependencyError` (`package="redis"`, `install_package="redis"`) when `redis.asyncio` is absent.
- **R15.1**: With `redis.asyncio` importable and no `client=`, a `backend_url` without a hostname raises `ImproperConfigurationError`, and a `backend_url` with a hostname binds `redis.asyncio.from_url(backend_url)`; `_InMemoryRedisStreamClient` is deleted from production and the client fake lives in `test_distributed_queues.py`.
- **R16**: `SpannerQueueBackend` without `executor=` raises `MissingDependencyError` (`package="vertebra-core[spanner]"`, `install_package="vertebra-core[spanner]"`) when `vertebra._spanner` is absent.
- **R16.1**: With `vertebra._spanner` importable and no `executor=`, `SpannerQueueBackend` raises `ImproperConfigurationError`; `_InMemorySpannerQueueExecutor`, `_DriverSpannerQueueExecutor`, the `driver=` parameter, and the `memory` scheme allowance are deleted, and the executor fake lives in `test_spanner_queue_and_cdc.py`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class PostgreSQLQueueBackend:
    """PostgreSQL queue backend using FOR UPDATE SKIP LOCKED and LISTEN/NOTIFY over an injected PostgreSQLQueueExecutor."""

    __slots__ = ("_config", "_direct_dsn", "_executor", "_postgres_config")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        postgres_config: PostgreSQLConfig | None = None,
        executor: PostgreSQLQueueExecutor | None = None,
    ) -> None:
        """Raises:
            MissingDependencyError: When ``executor`` is None and ``psycopg`` is not installed.
            ImproperConfigurationError: When ``executor`` is None and ``psycopg`` is installed.
        """
```

```python
class RedisQueueBackend:
    __slots__ = ("_client", "_config", "_prefix")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        client: RedisStreamClient | None = None,
    ) -> None:
        """Raises:
            MissingDependencyError: When ``client`` is None and ``redis`` is not installed.
            ImproperConfigurationError: When ``client`` is None and ``config.backend_url`` has no host.
        """

    @staticmethod
    def _build_client(config: QueueConfig) -> RedisStreamClient: ...
```

`ValkeyQueueBackend.__init__(self, config: QueueConfig | None = None, *, client: RedisStreamClient | None = None) -> None` keeps its body and inherits the contract.

```python
class SpannerQueueBackend:
    __slots__ = ("_cleanup_on_ack", "_config", "_dialect", "_executor", "_receipt_index", "_shard_count")

    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        executor: SpannerQueueExecutor | None = None,
        dialect: Literal["googlesql", "spangres"] = "googlesql",
        shard_count: int = 16,
        cleanup_on_ack: bool = True,
    ) -> None:
        """Raises:
            ImproperConfigurationError: When ``backend_url`` is not a Spanner scheme, or when ``executor``
                is None and ``vertebra._spanner`` is installed.
            MissingDependencyError: When ``executor`` is None and ``vertebra._spanner`` is not installed.
        """
```

Accepted Spanner schemes: `""`, `spanner`, `spanner+googlesql`, `spangres`, `spanner+spangres`. Protocols `PostgreSQLQueueExecutor`, `RedisStreamClient`, and `SpannerQueueExecutor` and every module `__all__` are unchanged.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `QueueService(QueueConfig(backend_url="postgresql://..."))` → `QueueService._build_backend` → `PostgreSQLQueueBackend(config)` → `ensure_module("psycopg")` → `MissingDependencyError`, else `ImproperConfigurationError`.
- `QueueService(QueueConfig(backend_url="redis://host:6379/0"))` / `valkey://...` → `RedisQueueBackend(config)` / `ValkeyQueueBackend(config)` → `_build_client` → `ensure_module("redis.asyncio")` → hostname check → `redis.asyncio.from_url(backend_url)`.
- `QueueService(QueueConfig(backend_url="spanner://..."))` / `spangres://...` → `SpannerQueueBackend(config)` → scheme check → `ensure_module("vertebra._spanner")` → `ImproperConfigurationError`.
- `QueueService(config, backend=Backend(config, executor=fake | client=fake))` → binds the injected double with no module resolution.
- `EmailService(email_config, queue_service=q_service)` → `QueueService.enqueue` → injected `RedisQueueBackend(client=FakeRedisStreamClient())` in `test_email_providers.py`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| PostgreSQL, `psycopg` absent | `sys.modules["psycopg"] = None`; `PostgreSQLQueueBackend(QueueConfig(backend_url="postgresql://localhost:5432/vtb"))` | `MissingDependencyError`; `.package == "psycopg"`, `.install_package == "psycopg[binary,pool]"`, `.hint == "Install it with: uv add psycopg[binary,pool]"` | No in-memory executor |
| PostgreSQL via service | Same; `QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))` | `MissingDependencyError` with `.package == "psycopg"` | `memory://` unchanged |
| PostgreSQL, present, no executor | `sys.modules["psycopg"] = types.ModuleType("psycopg")`; `PostgreSQLQueueBackend(postgres_config=PostgreSQLConfig(dsn="postgresql://pooler:6432/db", direct_dsn="postgresql://primary:5432/db"))` | `ImproperConfigurationError` matching `"requires a PostgreSQLQueueExecutor"` | No in-memory executor |
| PostgreSQL, injected executor | `PostgreSQLQueueBackend(postgres_config=pg_config, executor=_FakePostgreSQLQueueExecutor())` | Constructs; `direct_dsn == "postgresql://primary:5432/db"` | `direct_dsn` resolution unchanged |
| Redis, `redis` absent | `sys.modules["redis.asyncio"] = None`; `RedisQueueBackend(QueueConfig(backend_url="redis://localhost:6379/0"))` | `MissingDependencyError`; `.package == "redis"`, `.install_package == "redis"`, `.hint == "Install it with: uv add redis"` | No emulator |
| Valkey via service, `redis` absent | `QueueService(QueueConfig(backend_url="valkey://localhost:6379/0"))` | `MissingDependencyError` with `.package == "redis"` | No emulator |
| Redis, present, live URL | Stub `redis.asyncio` whose `from_url` records its argument and returns a sentinel; `RedisQueueBackend(QueueConfig(backend_url="redis://cache.internal:6380/2"))` | `backend.client is sentinel`; recorded URL `["redis://cache.internal:6380/2"]` | Live path preserved |
| Redis, present, hostless URL | `RedisQueueBackend(QueueConfig(backend_url="redis:///0"))` | `ImproperConfigurationError` matching `"has no host"` | No emulator |
| Valkey default config, present | `ValkeyQueueBackend()` (`backend_url="memory://"`) | `ImproperConfigurationError` matching `"has no host"` | No emulator |
| Redis / Valkey injected client | `ValkeyQueueBackend(client=fake)` with `redis.asyncio` absent | Constructs; `backend.client is fake` | No module resolution |
| Spanner, extension absent | `sys.modules["vertebra._spanner"] = None`; `SpannerQueueBackend(QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d"))` | `MissingDependencyError`; `.package == "vertebra-core[spanner]"`, `.install_package == "vertebra-core[spanner]"` | No in-memory executor |
| Spanner via service | Same; `QueueService(QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d"))` | `MissingDependencyError` | No in-memory executor |
| Spanner, present, no executor | `sys.modules["vertebra._spanner"] = types.ModuleType("vertebra._spanner")`; `SpannerQueueBackend()` | `ImproperConfigurationError` matching `"requires a SpannerQueueExecutor"` | No driver wrapper |
| Spanner `memory://` URL | `SpannerQueueBackend(QueueConfig(), executor=_FakeSpannerQueueExecutor())` | `ImproperConfigurationError` matching `"requires a spanner:// backend_url"` before any module check | `memory://` routes only to `MemoryQueueBackend` |
| Spanner `driver=` keyword | `SpannerQueueBackend(cfg, driver=d)` | `TypeError` (unexpected keyword) | No shim |
| Spanner injected executor, spangres URL | `SpannerQueueBackend(QueueConfig(backend_url="spangres://projects/p/instances/i/databases/d"), executor=fake)` | `dialect == "spangres"` | Dialect inference unchanged |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `psycopg` absent, when `QueueService(QueueConfig(backend_url="postgres://localhost:5432/vtb"))` is constructed, then `MissingDependencyError` hints `uv add psycopg[binary,pool]`.
- Given `redis` installed, when `RedisQueueBackend(QueueConfig(backend_url="redis://cache.internal:6380/2"))` is constructed, then its client is `redis.asyncio.from_url("redis://cache.internal:6380/2")`.
- Given `redis` installed, when `ValkeyQueueBackend()` is constructed, then `ImproperConfigurationError` reports the `memory://` URL has no host.
- Given `vertebra._spanner` absent, when `QueueService(QueueConfig(backend_url="spanner://projects/p/instances/i/databases/d"))` is constructed, then `MissingDependencyError` hints `uv add vertebra-core[spanner]`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| Keep `_DriverSpannerQueueExecutor` as the live Spanner path | Its DML and queries return in-memory mirror results and swallow every driver exception | Rejected: a silent substitute behind a real driver; deleted with the fake |
| Keep `driver=` on `SpannerQueueBackend` and raise when given without `executor=` | Leaves a parameter with no consumer | Rejected: product guidelines forbid compatibility shims; `TypeError` names the removed keyword |
| Keep the `memory` scheme on `SpannerQueueBackend` | `memory://` existed to opt into the in-memory executor | Rejected: `QueueService` routes `memory://` to `MemoryQueueBackend` |
| Build a live Spanner executor over `SyncSpannerDriver.run_in_transaction` now | Claim, lease, and DLQ SQL need emulator coverage outside unit scope | Rejected: follow-up spec via `/flow:plan spanner-queue-executor` |
| Build a `psycopg` executor for PostgreSQL now | Pool, LISTEN connection, and notify loop with no PostgreSQL container in unit scope | Rejected: the `queues-tasks-and-channels` flow owns the live executor as `PostgresQueueStore`[^queues-flow] |
| Redis: check the hostname before resolving `redis.asyncio` | Makes the configuration error environment-independent | Rejected: mirrors the `ch02 / t03` order (dependency, then configuration) so every queue backend reports a missing package first |
| Redis: pass `extra="redis"` | `redis` is not a declared extra; the `ch08` gate fails | Rejected: `install_package="redis"` |
| Move all fakes to a new shared helper module | Adds a module for three doubles whose consumers are already known | Rejected: fakes live in the canonical test modules; `test_email_providers.py` imports `FakeRedisStreamClient` the way `test_spanner_queue_and_cdc.py` imports `FakeSpannerClient` |
| `NotImplementedError` when the package is present but no executor exists | Not caught by `except ValueError` configuration handling | Rejected: matches `ch02 / t03` `ImproperConfigurationError` |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/4 files documented]`
- `[ ]` `src/py/vertebra/framework/queues/postgres.py` — `PostgreSQLQueueBackend` class docstring and `__init__` Google docstring with `Args:` and `Raises:` (t01).
- `[ ]` `src/py/vertebra/framework/queues/redis.py` — `RedisQueueBackend.__init__` Google docstring with `Args:` and `Raises:`; `_build_client` summary (t02).
- `[ ]` `src/py/vertebra/framework/queues/spanner.py` — `SpannerQueueBackend.__init__` `Args:` without `driver` and `Raises:` (t03).
- `[ ]` `src/py/vertebra/framework/queues/base.py` — `QueueConfig` class docstring `"""Configuration for QueueService across memory, Redis, Valkey, PostgreSQL, Oracle, and Spanner backends."""`, rendered in `docs/reference/queues.md` through `::: vertebra.framework.queues.base.QueueConfig` (t03).
- Handed to [ch12-queue-docs-and-knowledge](../ch12-queue-docs-and-knowledge/spec.md) (not edited in this chapter):
  - `docs/usage/queues-and-realtime.md` — PostgreSQL row (requires `executor=`; `psycopg[binary,pool]`), Redis and Valkey rows (require the `redis` package and a `redis://host` URL, or `client=`), Cloud Spanner row (requires `executor=`; `vertebra-core[spanner]`; the "wound-wait claim transactions" claim describes no shipped executor).[^queue-docs]
  - `docs/reference/queues.md` — Storage Backends row lists four network backends without Spanner and without the executor or client requirement (the rendered `QueueConfig` docstring is updated by `t03`).
  - `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — backend table: Redis row says list and `ZPOPMIN` (code uses Streams consumer groups and a scheduled sorted set), PostgreSQL row says `vertebra_tasks` (code uses `vertebra_queue_jobs`), Spanner row claims multiplexed sessions; add the executor-or-raise and client-or-raise rules.[^queue-knowledge]

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R14, R14.1 | `ch10 / t01` | `behavior_tdd` | `src/py/tests/unit/framework/test_distributed_queues.py::test_postgres_queue_backend_requires_psycopg_and_executor` |
| R15, R15.1 | `ch10 / t02` | `behavior_tdd` | `src/py/tests/unit/framework/test_distributed_queues.py::test_redis_queue_backend_requires_redis_and_host`; `src/py/tests/unit/framework/test_email_providers.py::test_email_service_enqueue_and_enqueue_batch_via_queue_service` |
| R16, R16.1 | `ch10 / t03` | `behavior_tdd` | `src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_queue_backend_requires_spanner_extension_and_executor` |
| Queue docs and OKF rows | `ch12-queue-docs-and-knowledge` | `documentation_validation` | `make docs`, OKF frontmatter and footnote checks |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch10:t01 — `PostgreSQLQueueBackend` requires `psycopg` and an injected executor (tasks/t01-postgres-queue-requires-executor.md)
- [ ] driver-fail-loud:ch10:t02 — `RedisQueueBackend` binds a live `redis.asyncio` client or raises (tasks/t02-redis-queue-requires-live-client.md)
- [ ] driver-fail-loud:ch10:t03 — `SpannerQueueBackend` requires the Spanner extension and an injected executor; `QueueConfig` docstring names every backend (tasks/t03-spanner-queue-requires-executor.md)
- **Size counting rule**: `estimated_loc` on every task in this chapter counts net-new lines only. Each fake class (`_InMemoryPostgreSQLQueueExecutor`, `_InMemoryRedisStreamClient`, `_InMemorySpannerQueueExecutor`, roughly 210–220 lines each) moves verbatim from its production module into the queue test module and is not counted; the moved body keeps its methods unchanged and takes a test-side name and docstring.
- **Chapter / Phase Recovery Checkpoint**: `uv run ruff check src/py/vertebra/framework/queues/ src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/framework/test_email_providers.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py && uv run mypy src/py/vertebra/framework/queues/postgres.py src/py/vertebra/framework/queues/redis.py src/py/vertebra/framework/queues/spanner.py src/py/vertebra/framework/queues/base.py && make py-test`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `0acdf576` — chapter planned; `0/3` tasks completed.
- **Active Task**: `driver-fail-loud:ch10:t01`, after `ch02-duckdb-sqlalchemy-queue-fail-loud` and `ch08-install-hints-and-extras-gate` complete.
- **Remaining Plan**: t01, t02, t03; t02 depends on t01 (both edit `test_distributed_queues.py`); t03 is independent.
- **Documentation & OKF Sync**: `[0/4 files documented]`; user docs and OKF rows go to `ch12-queue-docs-and-knowledge`.
- **Active Invariants & Triggers**: `MemoryQueueBackend` and `memory://` routing are untouched; no queue module imports `psycopg`, `redis`, or `vertebra._spanner` at module level; no new `extra=` literal other than the declared `spanner`.

[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend whose `_build_executor` always returns the in-memory executor.
[^pg-fake]: `src/py/vertebra/framework/queues/postgres.py::_InMemoryPostgreSQLQueueExecutor` — In-memory simulation of PostgreSQL queue SQL, SKIP LOCKED, and LISTEN/NOTIFY.
[^redis-queue]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client` — Redis client factory that substitutes the in-memory client when `redis.asyncio` is missing or the URL has no host.
[^redis-fake]: `src/py/vertebra/framework/queues/redis.py::_InMemoryRedisStreamClient` — In-memory emulator of Redis Streams, sorted sets, and the queue Lua scripts.
[^spanner-queue]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend selecting an injected, driver-wrapped, or in-memory executor.
[^spanner-fake]: `src/py/vertebra/framework/queues/spanner.py::_InMemorySpannerQueueExecutor` — In-memory simulation of hash-sharded Spanner queue SQL.
[^spanner-driver-exec]: `src/py/vertebra/framework/queues/spanner.py::_DriverSpannerQueueExecutor` — Driver wrapper that runs DDL once and routes every DML and query to an in-memory mirror.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing to backend constructors without an executor or client.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^errors]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and `ImproperConfigurationError` exported from `vertebra.typing`.
[^extras]: `pyproject.toml::project.optional-dependencies` — Declared extras; `spanner` is declared, `redis` and `postgres` are not.
[^queue-tests]: `src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url` — Queue contract test backend factory.
[^email-tests]: `src/py/tests/unit/framework/test_email_providers.py::test_email_service_enqueue_and_enqueue_batch_via_queue_service` — Email service queue test parametrized over `memory://` and `redis://`.
[^spanner-tests]: `src/py/tests/unit/db/test_spanner_queue_and_cdc.py::test_spanner_queue_backend_lifecycle_dlq_and_leases` — Spanner queue lifecycle test using `driver=` and a `spanner+googlesql` `QueueService`.
[^oracle-task]: `.agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t03-oracle-queue-requires-oracledb.md` — Oracle queue fail-loud worksheet whose executor-or-raise contract this chapter mirrors.
[^queue-docs]: `docs/usage/queues-and-realtime.md` — Queue backend table listing PostgreSQL, Redis, Valkey, Oracle, and Cloud Spanner.
[^queues-flow]: `.agents/bundles/specs/queues-tasks-and-channels/spec.md` — Native queue flow owning `PostgresQueueStore` and `OracleQueueStore` live queue executors.
[^queue-knowledge]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — OKF chapter describing queue backends and their executors.
