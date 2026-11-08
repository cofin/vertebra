---
type: Task
id: "driver-fail-loud:ch10:t02"
chapter_id: ch10-queue-backend-fail-loud
title: RedisQueueBackend binds a live redis.asyncio client or raises
description: Delete the production in-memory Redis Streams client, raise MissingDependencyError when redis.asyncio is absent and ImproperConfigurationError when backend_url has no host, keep the redis.asyncio.from_url path, and move the client fake into the queue test module for the contract and email tests.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch10:t01"
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/_typing.py
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/framework/test_email_providers.py
files:
  - src/py/vertebra/framework/queues/redis.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
  - src/py/tests/unit/framework/test_email_providers.py
estimated_loc: 250
plan_revision: 1
plan_commit: 1e1f028a
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
  - id: redis-queue
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client
    title: Redis client factory that substitutes the in-memory client when redis.asyncio is missing or the URL has no host
  - id: redis-fake
    resource: src/py/vertebra/framework/queues/redis.py::_InMemoryRedisStreamClient
    title: In-memory emulator of Redis Streams, sorted sets, and the queue Lua scripts
  - id: valkey-queue
    resource: src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend
    title: Valkey subclass whose default QueueConfig has backend_url memory://
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: redis, rediss, valkey, and valkeys queue URLs route to RedisQueueBackend / ValkeyQueueBackend(config)
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: errors
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error (ValueError subclass) exported from vertebra.typing
  - id: queue-tests
    resource: src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url
    title: Queue contract test backend factory
  - id: email-tests
    resource: src/py/tests/unit/framework/test_email_providers.py::test_email_service_enqueue_and_enqueue_batch_via_queue_service
    title: Email service queue test parametrized over memory:// and redis://localhost:6379/0
tags:
  - queues
  - redis
  - valkey
  - fail-loud
---

# Task t02: `RedisQueueBackend` Binds a Live `redis.asyncio` Client or Raises

## Objective & Scope Boundary

`RedisQueueBackend.__init__` binds a supplied `client` and otherwise calls `_build_client`, which raises `MissingDependencyError` (`package="redis"`, `install_package="redis"`) when `redis.asyncio` is absent, raises `ImproperConfigurationError` when `backend_url` has no hostname, and returns `redis.asyncio.from_url(backend_url)` otherwise.[^redis-queue][^module-loader][^errors] The live `from_url` path is kept; only the `_InMemoryRedisStreamClient` substitution is removed.[^redis-fake] `ValkeyQueueBackend` inherits the contract unchanged; its default `QueueConfig(stream_prefix="vtb:valkey:queue")` carries `backend_url="memory://"`, so a bare `ValkeyQueueBackend()` now raises.[^valkey-queue] The client class moves verbatim into `src/py/tests/unit/framework/test_distributed_queues.py` as the public `FakeRedisStreamClient`, injected by `_create_backend_by_url` for `redis` and `valkey` URLs and imported by `test_email_providers.py` for its `redis://` case.[^queue-tests][^email-tests]

`estimated_loc` counts net-new lines only; the 220-line `_InMemoryRedisStreamClient` class moves verbatim from `framework/queues/redis.py` into `test_distributed_queues.py` and is not counted (roughly 210–220 lines move in each of `t01`, `t02`, and `t03`).

Out of scope: rewriting `valkey://` / `valkeys://` for `from_url`, `unix://` socket URLs, the native `vertebra.db.adapters.redis` drivers (owned by `ch04` and `native-redis-driver`), `QueueService._build_backend` routing, removing `from __future__ import annotations`, and queue docs (`ch12-queue-docs-and-knowledge`).

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` — imports, `RedisStreamClient`, `_InMemoryRedisStreamClient`, `RedisQueueBackend.__init__`, `RedisQueueBackend._build_client`, `ValkeyQueueBackend.__init__`.[^redis-queue][^redis-fake][^valkey-queue]
- `src/py/vertebra/framework/queues/base.py` — `QueueConfig.backend_url` default, `QueueService.__init__(config, *, backend=None)`, `QueueService._build_backend`.[^queue-factory]
- `src/py/vertebra/utils/module_loader.py` — `ensure_module`, `import_optional` (reads `sys.modules[name]` first, so a stub or `None` entry for `redis.asyncio` decides presence).[^module-loader]
- `src/py/vertebra/_typing.py` — `MissingDependencyError`, `ImproperConfigurationError`, `reset_dependency_cache`.[^errors]
- `src/py/tests/unit/framework/test_distributed_queues.py` — `_create_backend_by_url` and the five tests parametrized with `"redis://localhost:6379/0"` / `"valkey://localhost:6379/0"`.[^queue-tests]
- `src/py/tests/unit/framework/test_email_providers.py` — imports and `test_email_service_enqueue_and_enqueue_batch_via_queue_service`.[^email-tests]

## Symbol & Signature Contracts

- `src/py/vertebra/framework/queues/redis.py` imports:
  - remove `import collections` and `from vertebra.utils.module_loader import import_optional`;
  - add `from vertebra.typing import ImproperConfigurationError` and `from vertebra.utils.module_loader import ensure_module`;
  - `contextlib`, `time`, `urllib.parse`, `uuid`, `msgspec`, and the serializer imports stay (used by `RedisQueueBackend`).
- `RedisQueueBackend.__init__(self, config: QueueConfig | None = None, *, client: RedisStreamClient | None = None) -> None`:

```python
self._config = config or QueueConfig()
self._prefix = self._config.stream_prefix
self._client: RedisStreamClient = client if client is not None else self._build_client(self._config)
```

  Google docstring: summary `"""Bind the queue backend to a Redis Streams client."""`, `Args:` (`config` — `backend_url` names the Redis or Valkey server when `client` is None; `client` — pre-built `RedisStreamClient` bound as given), `Raises:` (`MissingDependencyError` when `client` is None and `redis` is not installed; `ImproperConfigurationError` when `client` is None and `config.backend_url` has no host).
- `RedisQueueBackend._build_client(config: QueueConfig) -> RedisStreamClient` (staticmethod):

```python
aioredis = ensure_module("redis.asyncio", package="redis", install_package="redis")
if not urllib.parse.urlparse(config.backend_url).hostname:
    msg = (
        f"Redis queue backend_url {config.backend_url!r} has no host; "
        "pass a redis://host:port URL or client= to bind a RedisStreamClient"
    )
    raise ImproperConfigurationError(msg)
client: RedisStreamClient = aioredis.from_url(config.backend_url)
return client
```

  Docstring: `"""Build a redis.asyncio client from ``config.backend_url``."""`
- `_InMemoryRedisStreamClient` is deleted; `__all__` and `ValkeyQueueBackend` are unchanged.
- Test module `test_distributed_queues.py`:
  - add `import collections` (present after `t01`), `import time` (present), `from vertebra.utils.serializers import decode_json`, and `import msgspec`; add `RedisStreamClient` to the `vertebra.framework.queues` import; import `Mapping` from `collections.abc` inside an `if TYPE_CHECKING:` block (the module keeps `from __future__ import annotations`), adding `TYPE_CHECKING` to the `typing` import;
  - `class FakeRedisStreamClient` is the deleted class body moved verbatim (same `__slots__`, `__init__`, `xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`, `xrange`, `xdel`, `zadd`, `eval`, `set`, `get`, `aclose`), class docstring `"""In-memory Redis Streams client emulating consumer groups, sorted sets, and the queue Lua scripts."""`. The name is public because `test_email_providers.py` imports it;
  - `_create_backend_by_url`: `if url.startswith("redis"): return RedisQueueBackend(cfg, client=FakeRedisStreamClient())` and `if url.startswith("valkey"): return ValkeyQueueBackend(cfg, client=FakeRedisStreamClient())`; docstring `"""Instantiate queue backends, injecting the Redis client and the Oracle and PostgreSQL executor fakes."""`.
- Test module `test_email_providers.py`:
  - import `RedisQueueBackend` alongside `QueueConfig, QueueService` from `vertebra.framework.queues`, and `from tests.unit.framework.test_distributed_queues import FakeRedisStreamClient`;
  - in `test_email_service_enqueue_and_enqueue_batch_via_queue_service`, replace `q_service = QueueService(q_config)` with:

```python
q_service = (
    QueueService(q_config, backend=RedisQueueBackend(q_config, client=FakeRedisStreamClient()))
    if backend_url.startswith("redis")
    else QueueService(q_config)
)
```

## Edge Cases & Error Semantics

- `sys.modules["redis.asyncio"] = None`, no `client`: `MissingDependencyError` with `.package == "redis"`, `.install_package == "redis"`, `.hint == "Install it with: uv add redis"`. No `extra=` is passed: `redis` is not a declared extra (`ch08` gate).
- Dependency resolution precedes the hostname check (same order as `ch02 / t03`): with `redis` absent, `RedisQueueBackend(QueueConfig(backend_url="redis:///0"))` raises `MissingDependencyError`.
- `redis.asyncio` present, `backend_url="redis:///0"` (`urlparse(...).hostname is None`): `ImproperConfigurationError` matching `"has no host"`.
- `redis.asyncio` present, `ValkeyQueueBackend()` (`backend_url="memory://"`): `ImproperConfigurationError` matching `"has no host"`.
- `redis.asyncio` present, `backend_url="redis://cache.internal:6380/2"`: `from_url` is called once with that exact string and its return value is bound as `backend.client`.
- `client=` supplied: no `ensure_module` call; construction succeeds with `redis` absent.
- `client` falsy but not None: bound as given (`is not None` replaces `client or ...`).
- The test stubs `redis.asyncio` with `types.ModuleType("redis.asyncio")` plus `monkeypatch.setattr(stub, "from_url", _from_url, raising=False)`, and calls `reset_dependency_cache("redis.asyncio")` after each `sys.modules` change and in `finally`.
- `redis` is not in `uv.lock`, so the default environment exercises the missing-module branch; the email test's `redis://` case passes only through the injected client.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/framework/test_distributed_queues.py` (new test, contract factory), `src/py/tests/unit/framework/test_email_providers.py` (injected client for the `redis://` case).
- **Dependency category**: external boundary (`redis.asyncio`), simulated through `sys.modules`; stream behavior exercised through `FakeRedisStreamClient` on the `RedisStreamClient` seam.
- **New test**: `test_redis_queue_backend_requires_redis_and_host(monkeypatch: pytest.MonkeyPatch) -> None`, with a nested `def _from_url(url: str) -> FakeRedisStreamClient` that appends `url` to `captured: list[str]` and returns `live = FakeRedisStreamClient()`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `redis` absent | `monkeypatch.setitem(sys.modules, "redis.asyncio", None)`; `reset_dependency_cache("redis.asyncio")`; `RedisQueueBackend(QueueConfig(backend_url="redis://localhost:6379/0"))` | `MissingDependencyError`; `err.package == "redis"`; `err.install_package == "redis"`; `err.hint == "Install it with: uv add redis"` |
| `redis` absent via Valkey service | Same `sys.modules`; `QueueService(QueueConfig(backend_url="valkey://localhost:6379/0"))` | `MissingDependencyError` with `err.package == "redis"` |
| Injected client, `redis` absent | `fake = FakeRedisStreamClient()`; `ValkeyQueueBackend(client=fake)` | Constructs; `backend.client is fake`; `isinstance(fake, RedisStreamClient)` is `True` |
| `redis` present, live URL | `stub = types.ModuleType("redis.asyncio")`; `monkeypatch.setattr(stub, "from_url", _from_url, raising=False)`; `monkeypatch.setitem(sys.modules, "redis.asyncio", stub)`; `reset_dependency_cache("redis.asyncio")`; `RedisQueueBackend(QueueConfig(backend_url="redis://cache.internal:6380/2"))` | `backend.client is live`; `captured == ["redis://cache.internal:6380/2"]` |
| `redis` present, hostless URL | Same stub; `RedisQueueBackend(QueueConfig(backend_url="redis:///0"))` | `pytest.raises(ImproperConfigurationError, match="has no host")`; `captured` unchanged |
| `redis` present, Valkey default config | Same stub; `ValkeyQueueBackend()` | `pytest.raises(ImproperConfigurationError, match="has no host")` |
| Queue contract on fake | Existing `test_queue_backends_*` cases for `redis://localhost:6379/0` and `valkey://localhost:6379/0` | Pass unchanged with the injected fake |
| Email queue integration | `test_email_service_enqueue_and_enqueue_batch_via_queue_service["redis://localhost:6379/0"]` | `drain_once(lanes=("io",))` returns 3 results with the injected client |

## Verification

- Initial Red baseline (fake copied into the test module and new test written; production unchanged, so the absent and hostless cases fail because the in-memory client is returned): `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "requires_redis" -q`
- Final Green:
  - `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/framework/test_email_providers.py -q`
  - `uv run ruff check src/py/vertebra/framework/queues/redis.py src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/framework/test_email_providers.py`
  - `uv run mypy src/py/vertebra/framework/queues/redis.py`
  - `! grep -nE "_InMemoryRedisStreamClient|import_optional" src/py/vertebra/framework/queues/redis.py`
  - `! grep -rn "_InMemoryRedisStreamClient" src/py/vertebra`

## Acceptance Criteria

- [ ] `RedisQueueBackend` and `ValkeyQueueBackend` without `client` raise `MissingDependencyError` naming `redis` with hint `uv add redis` when `redis.asyncio` is absent (R15).
- [ ] With `redis.asyncio` present and no `client`, a hostless `backend_url` raises `ImproperConfigurationError` and a hosted `backend_url` binds `redis.asyncio.from_url(backend_url)` (R15.1).
- [ ] `_InMemoryRedisStreamClient` is absent from `src/py/vertebra`; `FakeRedisStreamClient` lives in `test_distributed_queues.py`, every `redis://` / `valkey://` contract case passes, and the email test's `redis://` case passes through the injected client (R15.1).
- [ ] `RedisQueueBackend.__init__` has a Google docstring with `Raises:`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-08 00:00] `RedisQueueBackend._build_client` returns `redis.asyncio.from_url(backend_url)` when `redis.asyncio` imports and the URL has a hostname; this live path is the one kept.
- [2026-11-08 00:00] `redis.asyncio.from_url` parses URLs with `redis.connection.parse_url`, which accepts `redis://`, `rediss://`, and `unix://` only; `valkey://` and `valkeys://` URLs routed by `QueueService._build_backend` raise `ValueError` from `redis-py` once the emulator no longer intercepts them. Scheme rewriting is a follow-up for `/flow:plan`; `redis` is not installed locally, so this is sourced from `redis-py`, not executed.
- [2026-11-08 00:00] `unix:///path/redis.sock` has no hostname and raises `ImproperConfigurationError`; `QueueService` never routes `unix://` to `RedisQueueBackend`, so only direct construction is affected.

[^redis-queue]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client` — Redis client factory that substitutes the in-memory client when `redis.asyncio` is missing or the URL has no host.
[^redis-fake]: `src/py/vertebra/framework/queues/redis.py::_InMemoryRedisStreamClient` — In-memory emulator of Redis Streams, sorted sets, and the queue Lua scripts.
[^valkey-queue]: `src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend` — Valkey subclass whose default `QueueConfig` has `backend_url` `memory://`.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — `redis`, `rediss`, `valkey`, and `valkeys` queue URLs route to `RedisQueueBackend` / `ValkeyQueueBackend(config)`.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^errors]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error (`ValueError` subclass) exported from `vertebra.typing`.
[^queue-tests]: `src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url` — Queue contract test backend factory.
[^email-tests]: `src/py/tests/unit/framework/test_email_providers.py::test_email_service_enqueue_and_enqueue_batch_via_queue_service` — Email service queue test parametrized over `memory://` and `redis://localhost:6379/0`.
