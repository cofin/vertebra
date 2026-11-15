---
type: Spec
flow_id: native-redis-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: 'Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams'
description: Implement Redis URL/TLS/Google Cloud Memorystore IAM config and error mapping, RESP2/RESP3 codec + pipelines + Lua script cache, CLIENT TRACKING + Cluster + Sentinel topologies, and PyO3 Sync/AsyncRedisClient, Pub/Sub listeners, and Streams.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-wheel-scaffold-and-isolation
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:14:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:56:00Z'
    commit: 246d7d0e
tags:
  - redis
  - tls
  - gcp
  - resp3
  - cluster
  - streams
  - freethreaded
sources:
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: C-ABI Google Cloud IAM token cache capsule
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: RedisConfig topology, tracking, and Memorystore IAM fields
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: DbCompletionDrainer, spawn_rust_db_task_with_attr_and_cancel, and construct_py_arrow_cursor PyO3 async bridge
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: Legacy exception mapping and PyO3 DB bridge conventions
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded Python 3.14t/3.15t GIL_USED test
current_task: "native-redis-driver:ch02-rust-clients-topologies-and-streams:t01"
---

# Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams

<!-- truth: start -->
- `config.rs` normalizes `redis://`, `rediss://`, and `unix://` schemes for `redis 1.8.1`, enforces `ImproperConfigurationError` on `memory://` and `valkey://`/`valkeys://` (which belong to `vertebra._valkey`), and refreshes Google Cloud Memorystore for Redis IAM tokens dynamically via `GcpTokenCacheCapsule` [^gcp-capsule] [^redis-cfg].
- `errors.rs` maps `redis::RedisError` (`NOAUTH`, `WRONGPASS`, `NOPERM`, `READONLY`, `BUSY`, `LOADING`, `CLUSTERDOWN`, `CROSSSLOT`, `NOSCRIPT`, `WRONGTYPE`, I/O timeouts) to `vertebra._vertebra.DatabaseError` subclasses with `is_transient` [^vertebra-py-db].
- `value.rs` and `client_core.rs` negotiate `HELLO 3` (`SETNAME`, ACL/IAM `AUTH`) over `ConnectionManager` / `ClusterClient` for Redis `7.2+`/`8.0`, supporting `SCAN`+`UNLINK`, `PX` expiries, `MULTI`/`EXEC`/`WATCH` pipelines, and `EVALSHA`->`NOSCRIPT`->`EVAL` [^redis-cfg].
- `caching.rs` and `topology.rs` implement RESP3 push invalidations (`CLIENT TRACKING ON [PREFIX ...] BCAST`, bounded LRU), Sentinel `get-master-addr-by-name`, and Cluster CRC16 `MOVED` (slot-map update) / `ASK` (`ASKING`) routing [^redis-cfg].
- `py_client.rs`, `pubsub.rs`, and `streams.rs` expose `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`, `SyncRedisListener`, and `AsyncRedisListener` plus `XGROUP`/`XADD`/`XREADGROUP`/`XACK`/`XAUTOCLAIM` with `GIL_USED == False` and zero eager TCP dial [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Redis `7.2+`/`8.0` speaks RESP3 over TCP, `rustls` TLS (`rediss://`), and Unix sockets (`unix://`), while Google Cloud Memorystore for Redis uses short-lived OAuth2 IAM access tokens [^gcp-capsule] [^redis-cfg]. Constructing `SyncRedisDriver` or `AsyncRedisDriver` via `provide_driver` must validate config offline without dialing TCP until `connect()` or the first command executes, while running pipelined commands, `CLIENT TRACKING` invalidations, Sentinel/Cluster routing, Streams, and dedicated Pub/Sub sockets off the Python GIL (`py.detach`) [^py-async] [^ft-gil-test].

**Non-Goals:** Python driver/core cutover and `SyncNotificationSource`/`AsyncNotificationSource` protocol split (`ch03`); Streams queue backend cutover (`ch04`); Valkey `vertebra-valkey` wheel (`native-valkey-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Config, IAM & errors | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` [^gcp-capsule] [^redis-cfg] [^vertebra-py-db] | `redis://`/`rediss://`/`unix://`, `GcpTokenCacheCapsule`, typed `DatabaseError` |
| Codec & pipelines | `src/rs/vertebra-redis/src/{value,client_core}.rs` [^redis-cfg] | `HELLO 3`, KV ops, `MULTI`/`EXEC` pipelines, Lua `EVALSHA` fallback |
| Tracking & topology | `src/rs/vertebra-redis/src/{caching,topology}.rs` [^redis-cfg] | `CLIENT TRACKING` push invalidations, Cluster `MOVED`/`ASK`, Sentinel |
| PyO3, Pub/Sub & Streams | `src/rs/vertebra-redis/src/{py_client,pubsub,streams}.rs`, `_redis.pyi`, `test_freethreaded_gil.py` [^py-async] [^ft-gil-test] | `SyncRedis*` / `AsyncRedis*` clients, pipelines, listeners, and Streams |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5b**: Implement `RedisClientConfig` (`redis://`, `rediss://`, `unix://`; reject `memory://` and `valkey://`/`valkeys://`, `GcpTokenCacheCapsule` IAM hook) and `RedisError` -> `DatabaseError` mapping (`t01`).
- **R6**: Implement `value.rs` and `client_core.rs` (`HELLO 3`, `get`, `set(ex, px, nx, xx)`, `delete`, `unlink`, `pttl`, `scan`, `flushdb`, `publish`, `execute_command`, `execute_pipeline`, `eval_script`) (`t02`).
- **R7**: Implement `caching.rs` (`CLIENT TRACKING ON [PREFIX ...] BCAST` bounded LRU cache + push invalidation drain) and `topology.rs` (Sentinel master discovery + Cluster `MOVED`/`ASK` routing up to `max_redirections`) (`t03`).
- **R8 / R9**: Expose `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`, `SyncRedisListener`, and `AsyncRedisListener` (`subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `next_message`, `drain_messages`, `close`) plus Streams helpers (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`, `xrange`, `xdel`, `zadd`), synced in `_redis.pyi` and verified on `3.14t`/`3.15t` (`t04`).

```rust
pub enum RedisTopology {
    Standalone,
    Cluster { nodes: Vec<(String, u16)>, max_redirections: usize },
    Sentinel { master_name: String, nodes: Vec<(String, u16)> },
}
// URL normalization:
// redis://host:port/db    -> redis://host:port/db
// rediss://host:port/db   -> rediss://host:port/db (rustls + ring)
// unix:///path/to.sock    -> redis+unix:///path/to.sock
// valkey:// or memory://  -> ImproperConfigurationError
```

```python
class SyncRedisPipeline:
    def execute_command(self, args: Sequence[bytes | str | int]) -> Self: ...
    def results(self) -> list[Any]: ...

class AsyncRedisPipeline:
    def execute_command(self, args: Sequence[bytes | str | int]) -> Self: ...
    def submit_results(self) -> int: ...

class SyncRedisListener:
    def subscribe(self, channels: Sequence[str]) -> None: ...
    def psubscribe(self, patterns: Sequence[str]) -> None: ...
    def ssubscribe(self, shard_channels: Sequence[str]) -> None: ...
    def unsubscribe(self, channels: Sequence[str] = ()) -> None: ...
    def punsubscribe(self, patterns: Sequence[str] = ()) -> None: ...
    def sunsubscribe(self, shard_channels: Sequence[str] = ()) -> None: ...
    def next_message(self, timeout_ms: int | None = None) -> tuple[str, str, bytes, str | None] | None: ...
    def drain_messages(self) -> list[tuple[str, str, bytes, str | None]]: ...
    def close(self) -> None: ...

class AsyncRedisListener:
    def submit_subscribe(self, channels: Sequence[str]) -> int: ...
    def drain_messages(self) -> list[tuple[str, str, bytes, str | None]]: ...
    def cancel_operation(self, op_id: int) -> bool: ...
    def drain_completions(self) -> list[tuple[int, bool, Any]]: ...
    def notifier_fd(self) -> int: ...
    def close(self) -> None: ...

class SyncRedisClient: ...
class AsyncRedisClient: ...
```

**Integration Flow:**
- Lazy construction validates URL and topology offline without opening TCP; the first command or `connect()` dials and runs `HELLO 3`.
- `SyncRedisListener` and `AsyncRedisListener` own dedicated connections so subscribed sockets never block regular multiplexed command pipelines.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `memory://` or `valkey://` URL | `SyncRedisClient("memory://")` or `SyncRedisClient("valkey://...")` | Raises `ImproperConfigurationError` immediately | Fail-loud config validation |
| `CROSSSLOT` / `WRONGTYPE` | Server error prefix `CROSSSLOT` or `WRONGTYPE` | `ProgrammingError` (`is_transient=False`) | Typed command error |
| `BUSY` / `CLUSTERDOWN` / I/O | Transient server or network error | `OperationalError` (`is_transient=True`) | Retryable classification |
| Redirect loop | Cluster exceeds `max_redirections` (`5`) | `OperationalError` (`is_transient=True`) | Bounded `MOVED`/`ASK` hops |
| `ASK` vs `MOVED` | `ASK slot host:port` | Sends `ASKING` for single retry without mutating cached `slot_map()` | Accurate slot cache |
| `NOSCRIPT` | `EVALSHA` misses server script cache | Transparently retries once with `EVAL` | Zero cold-cache script failure |

## 3. Documentation & OKF Knowledge Plan

URL schemes, Memorystore IAM, tracking/Cluster/Sentinel usage, and `vertebra._redis` stub reference docs execute in `ch04:t03` and `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust URL/DSN Normalization, rustls mTLS, Google Cloud IAM Auth, and Error Mapping | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t02` | Implement RESP2/RESP3 Value Codec, Atomic/Non-Atomic Pipelines, and Lua Script Cache | `src/rs/vertebra-redis/src/{value,client_core,lib}.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t03` | Implement RESP3 CLIENT TRACKING L1 Cache, Cluster Routing, and Sentinel Discovery | `src/rs/vertebra-redis/src/{caching,topology,client_core}.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t04` | Expose PyO3 Sync/AsyncRedisClient, Pipelines, Pub/Sub Listeners, and Streams | `src/rs/vertebra-redis/src/{py_client,pubsub,streams,lib}.rs`, `src/py/vertebra/_redis.pyi`, `src/py/tests/unit/test_freethreaded_gil.py` | `make redis-test`, `mypy.stubtest vertebra._redis`, `src/py/tests/unit/test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-rust-url-tls-iam-and-error-mapping.md](tasks/t01-rust-url-tls-iam-and-error-mapping.md) — Build Redis URL normalization, `rustls` TLS/mTLS configuration, Google Cloud Memorystore IAM token provider, and structured error mapping in `vertebra-redis`.
- [ ] [t02-value-codec-pipeline-and-lua-scripts.md](tasks/t02-value-codec-pipeline-and-lua-scripts.md) — Implement `RedisValue` <-> `PyObject` conversion, atomic/non-atomic pipelines, and `EVALSHA` -> `EVAL` fallback in `vertebra-redis`.
- [ ] [t03-client-tracking-cluster-and-sentinel.md](tasks/t03-client-tracking-cluster-and-sentinel.md) — Implement RESP3 `CLIENT TRACKING` L1 invalidation, Cluster slot routing (`MOVED`/`ASK` + replica reads), and Sentinel master discovery.
- [ ] [t04-pyo3-clients-pubsub-and-streams.md](tasks/t04-pyo3-clients-pubsub-and-streams.md) — Bind `SyncRedisClient`, `AsyncRedisClient`, `Sync`/`AsyncRedisPipeline`, `Sync`/`AsyncRedisListener`, and Stream (`XADD`/`XREADGROUP`/`XACK`/`XAUTOCLAIM`) helpers in `vertebra._redis`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Client construction never opens network sockets eagerly; every blocking command releases the GIL via `py.detach`.
- **Fast Chapter Gate (`< 60s`)**: `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis --locked && uv run --no-sync python -m mypy.stubtest vertebra._redis --concise && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q`

[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI Google Cloud IAM token cache capsule.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy exception mapping and PyO3 DB bridge conventions.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED test.
