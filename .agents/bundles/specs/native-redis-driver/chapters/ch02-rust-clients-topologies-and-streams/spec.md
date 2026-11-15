---
type: Spec
flow_id: native-redis-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: 'Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams'
description: Implement URL/TLS/GCP Memorystore IAM config and error mapping, RESP2/RESP3 codec + pipelines + Lua script cache, CLIENT TRACKING + Cluster + Sentinel topologies, and PyO3 Sync/AsyncRedisClient, Pub/Sub listeners, and Streams.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- redis
- valkey
- tls
- gcp
- wheel
- resp3
- cluster
- streams
sources:
- id: pg-pyproj
  resource: src/rs/vertebra-postgres/pyproject.toml
  title: Maturin standalone wheel isolation precedent
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
- id: gcp-capsule
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
  title: C-ABI GCP IAM token cache capsule
- id: redis-cfg
  resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
  title: RedisConfig topology, tracking, and Memorystore IAM fields
- id: py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: AsyncCompletionQueue and Notifier PyO3 bridge
- id: ft-gil-test
  resource: src/py/tests/unit/test_freethreaded_gil.py
  title: Free-threaded Python 3.14t/3.15t GIL_USED test
---

# Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams

<!-- truth: start -->
- All sibling maturin wheels exclude `"**/py/vertebra/_redis.pyi"` (and reciprocal `_sqlite.pyi` if landed), verified by `test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests].
- `config.rs` normalizes `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` schemes for `redis 1.8.1` while rejecting `memory://` with `ImproperConfigurationError` [^redis-cfg].
- GCP Memorystore for Redis/Valkey IAM authentication refreshes tokens dynamically through `GcpTokenCacheCapsule` [^gcp-capsule] [^redis-cfg].
- `errors.rs` maps `redis::RedisError` (`NOAUTH`, `WRONGPASS`, `NOPERM`, `READONLY`, `BUSY`, `LOADING`, `CLUSTERDOWN`, `CROSSSLOT`, `NOSCRIPT`, `WRONGTYPE`, I/O timeouts) to `vertebra._vertebra.DatabaseError` subclasses with `is_transient`.
- `client_core.rs` negotiates `HELLO 3` (`SETNAME`, ACL/IAM `AUTH`) over `ConnectionManager` / `ClusterClient` for both Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0`, supporting `SCAN`+`UNLINK`, `PX` expiries, `MULTI`/`EXEC`/`WATCH`, and `EVALSHA`->`NOSCRIPT`->`EVAL` [^redis-cfg].
- `caching.rs` and `topology.rs` implement RESP3 push frames + `CLIENT TRACKING ON [PREFIX ...] BCAST` bounded LRU invalidation, Sentinel `get-master-addr-by-name`, and Cluster CRC16 `MOVED` (slot-map update) / `ASK` (`ASKING`) routing [^redis-cfg].
- `py_client.rs`, `pubsub.rs`, and `streams.rs` expose `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`, `SyncRedisListener`, and `AsyncRedisListener` plus `XREADGROUP`/`XACK`/`XAUTOCLAIM` with `GIL_USED == False` [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Both Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` speak RESP3 over TCP, `rustls` TLS (`rediss://` / `valkeys://`), and Unix domain sockets (`unix://`), while GCP Memorystore uses short-lived OAuth2 IAM access tokens [^gcp-capsule] [^redis-cfg]. This chapter enforces wheel isolation and builds the URL, TLS, IAM, and error foundations of `vertebra-redis`.

**Non-Goals:** Command execution, client tracking, Cluster/Sentinel, Streams, and Pub/Sub (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel payloads |
| Config, IAM & errors | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` [^gcp-capsule] [^redis-cfg] | `valkey://`/`valkeys://`/`unix://` + `GcpTokenCacheCapsule` + `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

Constructing `SyncRedisDriver` or `AsyncRedisDriver` via `provide_driver` must validate configuration in memory without dialing a live TCP port until `connect()` or the first command executes, while supporting multiplexed RESP3 pipelining, `CLIENT TRACKING` push invalidations, Sentinel/Cluster topologies, Streams, and dedicated Pub/Sub sockets off the Python GIL [^py-async] [^redis-cfg].

**Non-Goals:** Python adapter cutover (`ch04`); `SyncNotificationSource` / `AsyncNotificationSource` protocol split (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Core & pipelines | `src/rs/vertebra-redis/src/{value,client_core}.rs` | `HELLO 3`, KV ops, `MULTI`/`EXEC` pipelines, Lua `EVALSHA` fallback |
| Tracking & topology | `src/rs/vertebra-redis/src/{caching,topology}.rs` [^redis-cfg] | `CLIENT TRACKING` push invalidations, Cluster `MOVED`/`ASK`, Sentinel |
| PyO3, Pub/Sub & Streams | `src/rs/vertebra-redis/src/{py_client,pubsub,streams}.rs`, `_redis.pyi` [^py-async] [^ft-gil-test] | `SyncRedis*` / `AsyncRedis*` clients, pipelines, listeners, and Streams |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5a**: Exclude `_redis.pyi` across sibling PyO3 `pyproject.toml` files and assert `[redis]`/`[valkey]` wheel isolation in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `RedisClientConfig` (`redis://`, `rediss://`, `valkey://`, `valkeys://`, `unix://`, `memory://` rejection, `GcpTokenCacheCapsule` IAM hook) and `RedisError` -> `DatabaseError` mapping (`t04`).

```rust
pub enum RedisTopology {
    Standalone,
    Cluster { nodes: Vec<(String, u16)>, max_redirections: usize },
    Sentinel { master_name: String, nodes: Vec<(String, u16)> },
}
// URL scheme normalization:
// valkey://host:port/db  -> redis://host:port/db
// valkeys://host:port/db -> rediss://host:port/db (rustls + ring)
// unix:///path/to.sock   -> redis+unix:///path/to.sock
// memory://...           -> ImproperConfigurationError
```

**Integration Flow:**
- `make redis-wheel-check` verifies `vertebra-redis` wheel isolation.
- `client_core.rs` (`ch03`) consumes `RedisClientConfig`, `GcpTokenCacheCapsule`, and `map_redis_error`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `memory://` URL | `SyncRedisClient("memory://")` | Raises `ImproperConfigurationError` immediately | Fail-loud baseline preserved |
| `CROSSSLOT` / `WRONGTYPE` | Server error prefix `CROSSSLOT` or `WRONGTYPE` | `ProgrammingError` (`is_transient=False`) | Typed command error |
| `BUSY` / `CLUSTERDOWN` / I/O | Transient network or failover error | `OperationalError` (`is_transient=True`) | Retryable classification |

- **R6**: Implement `value.rs` and `client_core.rs` (`HELLO 3`, `get`, `set(ex, px, nx, xx)`, `delete`, `unlink`, `pttl`, `scan`, `flushdb`, `publish`, `execute_command`, `execute_pipeline`, `eval_script`) (`t01`).
- **R7**: Implement `caching.rs` (`CLIENT TRACKING ON [PREFIX ...] BCAST` bounded LRU cache + push invalidation drain) and `topology.rs` (Sentinel master discovery + Cluster `MOVED`/`ASK` slot routing up to `max_redirections`) (`t02`).
- **R8**: Expose `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, and `AsyncRedisPipeline` over `SyncCompletionQueue` and `AsyncCompletionQueue` + `Notifier` with lazy dial (`t03`).
- **R9**: Implement `SyncRedisListener` / `AsyncRedisListener` (`subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `next_message`, `drain_messages`, `close`) and Redis Streams (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`, `xrange`, `xdel`, `zadd`), verified on `3.14t`/`3.15t` (`t04`).

```python
# src/py/vertebra/_redis.pyi
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
    # submit_subscribe / submit_psubscribe / submit_ssubscribe / submit_unsubscribe / submit_next_message,
    # drain_messages(), cancel_operation(op_id), drain_completions(), notifier_fd(), close()
    ...

class SyncRedisClient: ...
class AsyncRedisClient: ...
```

**Integration Flow:**
- Lazy construction validates URL/options without dialing TCP; first command or `connect()` dials and runs `HELLO 3`.
- `SyncRedisListener` and `AsyncRedisListener` own dedicated connections so subscribed sockets never block regular multiplexed commands.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Redirect loop | Cluster exceeds `max_redirections` (`5`) | `OperationalError` (`is_transient=True`) | Bounded `MOVED`/`ASK` hops |
| `ASK` vs `MOVED` | `ASK slot host:port` | Sends `ASKING` for single retry without mutating cached `slot_map()` | Accurate slot cache |
| `NOSCRIPT` | `EVALSHA` misses server script cache | Transparently retries once with `EVAL` | Zero cold-cache script failure |

## 3. Documentation & OKF Knowledge Plan

URL schemes and Memorystore IAM are documented in `ch04:t03` and `ch04:t04`.

`vertebra._redis` stub docs execute in `ch04:t03`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust URL/DSN Normalization, rustls mTLS, GCP IAM Auth, and Error Mapping | `src/rs/vertebra-redis/src/config.rs`, `src/rs/vertebra-redis/src/errors.rs`, `src/rs/vertebra-redis/src/lib.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t02` | Implement RESP2/RESP3 Value Codec, Atomic/Non-Atomic Pipelines, and Lua Script Cache | `src/rs/vertebra-redis/src/value.rs`, `src/rs/vertebra-redis/src/client_core.rs`, `src/rs/vertebra-redis/src/lib.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t03` | Implement RESP3 CLIENT TRACKING L1 Cache, Cluster Routing, and Sentinel Discovery | `src/rs/vertebra-redis/src/caching.rs`, `src/rs/vertebra-redis/src/topology.rs`, `src/rs/vertebra-redis/src/client_core.rs` | `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t04` | Expose PyO3 Sync/AsyncRedisClient, Pipelines, Pub/Sub Listeners, and Streams | `src/rs/vertebra-redis/src/py_client.rs`, `src/rs/vertebra-redis/src/lib.rs`, `src/py/vertebra/_redis.pyi` (+2 more) | `src/rs/vertebra-redis/tests/redis_client.rs`, `src/py/tests/unit/test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-rust-url-tls-iam-and-error-mapping.md](tasks/t01-rust-url-tls-iam-and-error-mapping.md) — Build Redis/Valkey URL normalization, rustls TLS/mTLS configuration, GCP Memorystore IAM token provider, and structured error mapping in vertebra-redis.
- [ ] [t02-value-codec-pipeline-and-lua-scripts.md](tasks/t02-value-codec-pipeline-and-lua-scripts.md) — Implement RedisValue <-> PyObject conversion, atomic/non-atomic pipelines, and EVALSHA -> EVAL fallback in vertebra-redis.
- [ ] [t03-client-tracking-cluster-and-sentinel.md](tasks/t03-client-tracking-cluster-and-sentinel.md) — Implement RESP3 CLIENT TRACKING L1 invalidation, Cluster slot routing (MOVED/ASK + replica reads), and Sentinel master discovery.
- [ ] [t04-pyo3-clients-pubsub-and-streams.md](tasks/t04-pyo3-clients-pubsub-and-streams.md) — Bind SyncRedisClient, AsyncRedisClient, Sync/AsyncRedisPipeline, Sync/AsyncRedisPubSub, and Stream (XADD/XREADGROUP/XACK/XAUTOCLAIM) helpers in vertebra._redis.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Client construction never opens network sockets eagerly; every blocking command releases the GIL via `py.detach`.
- **Fast Chapter Gate (`< 60s`)**: `make redis-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py`

[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI GCP IAM token cache capsule.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED test.
