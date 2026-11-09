---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch03-remove-memory-fakes
title: "Chapter 03: PostgreSQL/MySQL memory:// Removal and RESP3 Loopback Test Fixture"
description: PostgreSQL and MySQL drivers reject memory:// with ImproperConfigurationError and lose their in-process row fakes, and a threaded loopback RESP3 server fixture replaces every memory:// Redis and Valkey construction in the driver, store, Advanced Alchemy, framework, and Starlette unit tests.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch01-oracle-mssql-fail-loud
  - ch02-duckdb-sqlalchemy-queue-fail-loud
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
last_verified_checkpoint: b04ded81
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T03:19:43Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T00:00:00Z"
sources:
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: PostgreSQL sync and async drivers with _in_memory_rows branches
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL sync and async drivers with _in_memory_rows branches
  - id: improper-config
    resource: src/py/vertebra/_typing.py::ImproperConfigurationError
    title: Configuration error subclassing VertebraError and ValueError
  - id: redis-driver
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Redis sync and async RESP3 drivers whose wire commands the loopback fixture serves
  - id: resp3-parser
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: RESP3 frame parser reused by the loopback test server
  - id: kv-store
    resource: src/py/vertebra/framework/stores.py::KeyValueStore
    title: Framework key-value store with from_valkey and from_redis factories
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore
    title: Litestar store with from_valkey and from_redis factories
  - id: starlette-store
    resource: src/py/vertebra/extensions/starlette/store.py::CacheStore
    title: Starlette cache store with from_valkey and from_redis factories
  - id: alchemy-cache
    resource: src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url
    title: Advanced Alchemy cache whose from_url memory:// selects the local L1 cache
  - id: sqlite-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect.get_pool_class
    title: "SQLite dialects mapping memory:// and :memory: databases to StaticPool"
tags:
  - database
  - drivers
  - fail-loud
  - postgres
  - mysql
  - redis
  - valkey
  - testing

---

# Chapter 03: PostgreSQL/MySQL memory:// Removal and RESP3 Loopback Test Fixture

<!-- truth: start -->
- **PostgreSQL and MySQL reject memory://**: `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, `SyncMySQLDriver`, and `AsyncMySQLDriver` raise `ImproperConfigurationError` from `__init__` when the DSN starts with `memory://` (case-insensitive); `_in_memory_rows` and `_tx_snapshot` do not exist.[^pg-driver][^mysql-driver][^improper-config]
- **PostgreSQL never returns empty rows from a pool it cannot drive**: a pool without `connection()`, `execute_with_description()`, or `execute()` raises `ImproperConfigurationError`.[^pg-driver]
- **Loopback RESP3 fixture**: `src/py/tests/unit/conftest.py::resp3_server` yields a `Resp3FakeServer` on `127.0.0.1` that parses frames with `Resp3Parser`, serves sync and async drivers from one thread-based server, sends every reply and push frame under one state lock, and answers unknown commands with `-ERR unknown command`.[^resp3-parser][^redis-driver]
- **Fixture command surface**: handshake (`HELLO`, `CLIENT`, `PING`), key-value and TTL (`GET`, `SET`, `DEL`, `UNLINK`, `PTTL`, `SCAN`, `FLUSHDB`), pub/sub (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `SPUBLISH`), and scripting (`EVAL`, `EVALSHA`); `PUBLISH` never reaches `smessage` subscribers.
- **Consumer tests use the wire**: no test in `test_redis_valkey_driver.py`, `test_alchemy_cache_backend.py`, `test_framework_subdomains.py`, or `test_starlette_plugin.py` constructs a `memory://` Redis or Valkey config.[^kv-store][^litestar-store][^starlette-store]
- **Redis production code is unchanged in this chapter**: the Redis driver memory emulation and `RedisConfig.url_schemes` stay as they are until `ch04-redis-valkey-memory-removal`.[^redis-driver]
- **Preserved memory backends**: `SQLAlchemyCacheBackend.from_url("memory://")` local L1 cache (asserted by a new test), the SQLite dialect `memory://` StaticPool alias (`get_pool_class`), and SQLite `:memory:` are unchanged.[^alchemy-cache][^sqlite-dialect]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

PostgreSQL and MySQL drivers accept a `memory://` DSN and answer every query from a `_in_memory_rows` list, so a typo in a DSN returns empty result sets instead of an error.[^pg-driver][^mysql-driver] This chapter deletes those fakes and makes both drivers raise `ImproperConfigurationError` (a `ValueError` subclass) for `memory://`.[^improper-config]

The Redis and Valkey unit tests construct `memory://` configs and depend on the driver's in-process command emulation.[^redis-driver] Before `ch04-redis-valkey-memory-removal` deletes that emulation, this chapter adds a threaded loopback RESP3 server fixture and moves every such test in `test_redis_valkey_driver.py`, `test_alchemy_cache_backend.py`, `test_framework_subdomains.py`, and `test_starlette_plugin.py` onto it, so the tests exercise the real encoding and parsing path of `SyncRedisDriver` and `AsyncRedisDriver`.[^resp3-parser] The store factories under test (`KeyValueStore.from_valkey`, Litestar `MemoryStore.from_valkey`, Starlette `CacheStore`) are unchanged.[^kv-store][^litestar-store][^starlette-store]

Preserved: the SQLite dialect `memory://` StaticPool alias (`get_pool_class`), SQLite `:memory:`, queue and object storage `memory://`, and `SQLAlchemyCacheBackend.from_url("memory://")`.[^sqlite-dialect][^alchemy-cache]

**Non-goals**: Redis and Valkey driver memory removal, `RedisConfig.url_schemes`, `test_redis_valkey_resp3.py`, `test_native_db_drivers.py` Redis cases, and conformance cache cases (owned by `ch04-redis-valkey-memory-removal`); Redis queue `_InMemoryRedisStreamClient` fallback in `framework/queues/redis.py` (owned by `ch10-queue-backend-fail-loud`); RESP3 codec removal (owned by `native-redis-driver`); removing `from __future__ import annotations` from touched modules (requires quoting `TYPE_CHECKING`-only annotations and is a separate cleanup); documentation and OKF edits (owned by `ch11-docs-and-okf` and `ch13-agent-knowledge-and-embeddings-docs`).

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md).
- Runs after `ch01-oracle-mssql-fail-loud` and `ch02-duckdb-sqlalchemy-queue-fail-loud` because those chapters edit `src/py/tests/unit/db/test_enterprise_olap_drivers.py` and `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, which `t01` also edits.
- `ch04-redis-valkey-memory-removal` depends on this chapter: it retargets `test_redis_valkey_resp3.py` and `test_native_db_drivers.py` onto `resp3_server` and then deletes the emulation these tests no longer use.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| PostgreSQL | `postgres/driver.py::SyncPostgreSQLDriver`, `::AsyncPostgreSQLDriver`[^pg-driver] | External boundary (psycopg / asyncpg); tests inject `pool=` ports exposing `execute`, `execute_with_description`, or `execute_tuples` |
| MySQL | `mysql/driver.py::SyncMySQLDriver`, `::AsyncMySQLDriver`[^mysql-driver] | External boundary (pymysql / asyncmy); tests inject `connect_factory=` / `pool=`; no test uses MySQL `memory://` |
| Redis / Valkey wire | `redis/driver.py::SyncRedisDriver`, `::AsyncRedisDriver`[^redis-driver]; `redis/core.py::Resp3Parser`[^resp3-parser] | External boundary (TCP); sync driver uses blocking sockets, async driver opens `asyncio` streams; one thread-based loopback server serves both |
| Store consumers under test | `framework/stores.py::KeyValueStore`[^kv-store], `extensions/litestar/store.py::MemoryStore`[^litestar-store], `extensions/starlette/store.py::CacheStore`[^starlette-store], `extensions/alchemy/cache.py::SQLAlchemyCacheBackend`[^alchemy-cache] | Use driver `get`, `set`, `delete`, `pttl`, `scan`, `unlink`; tests pass `resp3_server` URLs |
| Test seam | `src/py/tests/unit/conftest.py::resp3_server` (new) | Function-scoped fixture; tests annotate `resp3_server: Resp3FakeServer` through a `TYPE_CHECKING` import from `tests.unit.conftest` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [python-framework skill](../../../../../skills/python-framework/SKILL.md)
- [review-tests skill](../../../../../skills/review-tests/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R6 (`t01`)**: PostgreSQL and MySQL drivers reject `memory://` DSNs with `ImproperConfigurationError` at construction; `_in_memory_rows`, `_tx_snapshot`, and every memory branch are deleted; a PostgreSQL pool exposing no execution method raises `ImproperConfigurationError`.
- **R7a (`t02`, `t03`)**: A threaded loopback RESP3 server fixture (`resp3_server`) exists for unit tests, and every Redis, Valkey, store, Advanced Alchemy, Starlette, and framework unit test in the four consumer files that constructs a `memory://` Redis or Valkey config runs against it instead. `t02` owns the transport, encoder, teardown, handshake, key-value, TTL, `SCAN`, and `UNLINK` surface and the eight `test_redis_valkey_driver.py` tests that need only it; `t03` owns the pub/sub and scripting surface, the two remaining `test_redis_valkey_driver.py` tests, and the Advanced Alchemy, framework, and Starlette retargets.

`ch04-redis-valkey-memory-removal` owns R7b and R7c.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
def _reject_memory_dsn(dsn: str) -> None:
    """Reject in-process memory:// DSNs.

    Args:
        dsn: Configured PostgreSQL or MySQL DSN.

    Raises:
        ImproperConfigurationError: When ``dsn`` starts with ``memory://`` (case-insensitive).
    """
```

`postgres/driver.py` and `mysql/driver.py` each define a module-private `_reject_memory_dsn` with an engine-specific message. Each driver `__init__` calls the guard on the resolved config before `super().__init__`.

| Guard | Exact message |
| --- | --- |
| `postgres/driver.py::_reject_memory_dsn` | `PostgreSQL drivers do not support memory:// DSNs; pass a postgresql:// DSN or inject pool=` |
| `mysql/driver.py::_reject_memory_dsn` | `MySQL drivers do not support memory:// DSNs; pass a mysql:// DSN or inject connect_factory= or pool=` |
| `SyncPostgreSQLDriver._run_sync_query` pool fallthrough | `PostgreSQL pool must provide connection(), execute_with_description(), or execute()` |

Unchanged public signatures: `SyncPostgreSQLDriver(config=None, *, pool=None, dialer=None, broker=None, executor=None)`, `AsyncPostgreSQLDriver(config=None, *, pool=None, dialer=None, broker=None, executor=None, async_executor=None)`, `SyncMySQLDriver(config=None, *, dialer=None, broker=None, connect_factory=None)`, `AsyncMySQLDriver(config=None, *, dialer=None, broker=None, pool=None)`.

Test fixture contract (`src/py/tests/unit/conftest.py`):

```python
class Resp3FakeServer:
    """Threaded loopback RESP3 server holding keys, expiries, scripts, and subscriptions in process."""

    def __init__(self) -> None: ...
    @property
    def port(self) -> int: ...
    @property
    def redis_url(self) -> str: ...
    @property
    def valkey_url(self) -> str: ...
    @property
    def commands(self) -> list[list[bytes]]: ...
    def close(self) -> None: ...


@pytest.fixture
def resp3_server() -> Iterator[Resp3FakeServer]:
    """Yield a running loopback RESP3 server and stop it after the test."""
```

- `redis_url == f"redis://127.0.0.1:{port}/0"`; `valkey_url == f"valkey://127.0.0.1:{port}/0"`.
- Module-private helpers: `_Server(socketserver.ThreadingTCPServer)` (`allow_reuse_address = True`, `daemon_threads = True`, `owner: Resp3FakeServer`; served by `serve_forever(poll_interval=0.05)`; `_Handler` reaches it through `assert isinstance(self.server, _Server)`), `_Handler(socketserver.BaseRequestHandler)`, `_ErrorReply(str)`, `_encode(value: object) -> bytes` (`t02`); `_encode_push(items: Sequence[bytes | int | None]) -> bytes`, `_script_result(script: str, keys: list[bytes], args: list[bytes], values: dict[bytes, bytes]) -> object` (`t03`).
- `close()` shuts down and closes every accepted client socket from a `set[socket.socket]` snapshot, then calls `server.shutdown()`, `server.server_close()`, and joins the serve thread.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://"))` → `_reject_memory_dsn` → `ImproperConfigurationError`.
- `AsyncPostgreSQLDriver(cfg, pool=port)` → `_run_async_query` → `port.execute_tuples` / `port.execute_with_description` / `asyncio.to_thread(SyncPostgreSQLDriver._run_sync_query)`.
- `AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))` → `asyncio.open_connection("127.0.0.1", port)` → `_Handler.handle` → `Resp3Parser.parse_frame` → `Resp3FakeServer._dispatch` → `_encode` → `sendall` under the state lock.
- `SyncRedisDriver.publish(ch, payload)` → `_dispatch` `PUBLISH` case → `_encode_push` frames written to each subscribed connection → integer delivery count to the publisher.
- `SQLAlchemyCacheBackend.from_url("memory://")` → local L1 branch → no driver constructed.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Lowercase memory DSN | `SyncMySQLDriver(MySQLConfig(dsn="memory://"))` | `ImproperConfigurationError` | No connection attempt |
| Uppercase memory DSN | `AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="MEMORY://"))` | `ImproperConfigurationError` | Case-insensitive match |
| Unusable PostgreSQL pool | `SyncPostgreSQLDriver(PostgreSQLConfig(), pool=object()).execute_with_description("SELECT 1")` | `ImproperConfigurationError` naming the three methods | No empty-row result |
| Unknown wire command | A driver sends a command the fixture does not implement | `-ERR unknown command '<NAME>'` reaches the test as a driver error | No silent `+OK` |
| Pipelined or partial frames | Several frames in one `recv`, or a frame split across two | Replies in arrival order; partial frame waits | Parser offset drives framing |
| Concurrent handlers | `PUBLISH` from one connection while another connection receives a reply | Each `sendall` runs under the state lock | Frames never interleave on one socket |
| Shard pub/sub | `SSUBSCRIBE shard:1`, then `PUBLISH shard:1` and `SPUBLISH shard:1` | Only `SPUBLISH` delivers an `smessage` push | Redis 7 shard semantics |
| Bare unsubscribe | `UNSUBSCRIBE` with no names on a connection holding no `message` subscription | One push `>3 unsubscribe _ 0` (null channel, count `0`) | Matches real Redis, so the driver never waits on a missing reply |
| Teardown with blocked handler | Test ends while a handler thread waits in `recv` | `close()` shuts client sockets first, then the server, and returns | No leaked thread holds the port |
| Fixture port unreachable | Vacuity probe points both URLs at port `1` | Retargeted tests fail with `ConnectionRefusedError` / `OSError` | Tests depend on the wire |
| Alchemy local cache | `SQLAlchemyCacheBackend.from_url("memory://")` | Backend with `sync_driver is None` and `async_driver is None` | Local L1 cache preserved |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `PostgreSQLConfig(dsn="memory://")`, when `SyncPostgreSQLDriver` is constructed, then `ImproperConfigurationError` matching `memory://` is raised.
- Given a `resp3_server` fixture, when `AsyncRedisDriver(RedisConfig(url=resp3_server.redis_url))` sets `cache:a` with `px=250`, then `0 < await driver.pttl("cache:a") <= 250`.
- Given a `resp3_server` fixture, when `AsyncRedisDriver` runs `eval_script("return ARGV[1]", args=["hello"])`, then the result is `b"hello"` and `resp3_server.commands` records `EVALSHA` followed by `EVAL` (the `NOSCRIPT` fallback round trip).
- Given `SQLAlchemyCacheBackend.from_url("memory://")`, when built, then both driver attributes are `None`.

### 2.6 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| Where to reject `memory://` | Driver `__init__` guard per adapter module | Config `__post_init__`: rejects earlier but blocks building configs for non-driver uses and touches more files |
| Shared guard helper | Module-private guard in each adapter | One helper in `vertebra/db/driver/_common.py`: adds a lazy-export entry for a three-line check and couples adapters to a shared message |
| Redis/Valkey test double | Threaded loopback RESP3 server exercising the real wire path for sync and async drivers | Subclass overriding `execute_command`: tests a fake instead of encoding and parsing; separate asyncio and socketpair fakes: two implementations of one protocol |
| Fixture task split | `t02` builds the transport and key-value surface with the tests that need only it; `t03` adds pub/sub and scripting with the remaining tests | Split by test file: the first task would still need the full command surface, so the fixture module alone would exceed the task size bound |
| Direct-pubsub half of `test_key_value_backend_and_redis_pubsub` | Delete; wire pubsub is covered by `test_redis_valkey_resp3.py::test_multiplexed_resp3_pubsub_and_broker_forwarding` | Rewrite against the fake: the async subscription iterator blocks on a live stream and duplicates existing coverage |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch03]`
- No user-facing documentation changes: `docs/` and `README.md` describe `memory://` only for queues and object storage, which stay valid. The PostgreSQL and MySQL drivers document no `memory://` DSN in their public docstrings.
- `src/py/tests/unit/conftest.py` carries Google docstrings for `Resp3FakeServer`, `resp3_server`, and every helper (`t02`, `t03`).
- `driver-fail-loud:ch13:t02` (`ch13-agent-knowledge-and-embeddings-docs`) owns the `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` entry that names the `resp3_server` fixture as the unit-test seam for Redis and Valkey consumers.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6 | [t01](tasks/t01-postgres-mysql-memory-removal.md) | `behavior_tdd` | `src/py/tests/unit/db/test_native_db_drivers.py::test_postgres_and_mysql_drivers_reject_memory_dsn` |
| R7a (fixture core and key-value retargets) | [t02](tasks/t02-resp3-fixture-core-and-key-value-retarget.md) | `static_validation` | `src/py/tests/unit/db/test_redis_valkey_driver.py::test_redis_driver_scan_unlink_and_px` |
| R7a (pub/sub, scripting, and consumer retargets) | [t03](tasks/t03-resp3-pubsub-scripting-and-consumer-retarget.md) | `static_validation` | `src/py/tests/unit/db/test_redis_valkey_driver.py::test_async_redis_driver_store_operations`, `src/py/tests/unit/extensions/test_alchemy_cache_backend.py::test_alchemy_cache_memory_url_stays_local` |

## 5. Implementation Plan

- [x] [b04ded81] driver-fail-loud:ch03:t01 — PostgreSQL and MySQL drivers reject memory:// and lose in-memory rows ([tasks/t01-postgres-mysql-memory-removal.md](tasks/t01-postgres-mysql-memory-removal.md))
- [x] [b04ded81] driver-fail-loud:ch03:t02 — Loopback RESP3 fixture core and key-value driver tests over the wire ([tasks/t02-resp3-fixture-core-and-key-value-retarget.md](tasks/t02-resp3-fixture-core-and-key-value-retarget.md))
- [x] [b04ded81] driver-fail-loud:ch03:t03 — Loopback RESP3 pub/sub and scripting surface and consumer tests over the wire ([tasks/t03-resp3-pubsub-scripting-and-consumer-retarget.md](tasks/t03-resp3-pubsub-scripting-and-consumer-retarget.md))
- **Chapter Recovery Checkpoint**: `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/extensions/test_alchemy_cache_backend.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/framework/test_framework_subdomains.py -q && make lint` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ ea07cb21`; `state: completed`; depends on `ch01` and `ch02` for shared test files.
- **Remaining Tasks in Chapter**: None (`3/3` completed).
- **Downstream**: `ch04-redis-valkey-memory-removal` starts after `t03`, when no consumer unit test depends on the Redis memory emulation.
- **Documentation & OKF Sync**: `[0/0]` in this chapter; the `resp3_server` entry in `polyglot-test-matrix.md` is owned by `driver-fail-loud:ch13:t02`.
- **Active Invariants**: queue, object storage, Advanced Alchemy `from_url("memory://")` local cache, the SQLite dialect `memory://` StaticPool alias (`get_pool_class`), and SQLite `:memory:` paths unchanged; Redis production code unchanged; no inline `#` comments in touched Python.

[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — PostgreSQL sync and async drivers with _in_memory_rows branches.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL sync and async drivers with _in_memory_rows branches.
[^improper-config]: `src/py/vertebra/_typing.py::ImproperConfigurationError` — Configuration error subclassing VertebraError and ValueError.
[^redis-driver]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Redis sync and async RESP3 drivers whose wire commands the loopback fixture serves.
[^resp3-parser]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — RESP3 frame parser reused by the loopback test server.
[^kv-store]: `src/py/vertebra/framework/stores.py::KeyValueStore` — Framework key-value store with from_valkey and from_redis factories.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore` — Litestar store with from_valkey and from_redis factories.
[^starlette-store]: `src/py/vertebra/extensions/starlette/store.py::CacheStore` — Starlette cache store with from_valkey and from_redis factories.
[^alchemy-cache]: `src/py/vertebra/extensions/alchemy/cache.py::SQLAlchemyCacheBackend.from_url` — Advanced Alchemy cache whose from_url memory:// selects the local L1 cache.
[^sqlite-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect.get_pool_class` — SQLite dialects mapping memory:// and :memory: databases to StaticPool.
