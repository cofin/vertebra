---
type: Spec
flow_id: native-redis-driver
chapter_id: ch03-rust-clients-topologies-pubsub-and-streams
title: "Chapter 03: Rust RESP3 Clients, CLIENT TRACKING, Sentinel/Cluster, Pipelines, Streams, and Listeners"
description: "Implement HELLO 3 RESP3 commands, SyncRedisPipeline/AsyncRedisPipeline, CLIENT TRACKING BCAST cache, Sentinel discovery, Cluster MOVED/ASK routing, Streams, and SyncRedisListener/AsyncRedisListener."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - redis
  - valkey
  - resp3
  - cluster
  - streams
  - pubsub
sources:
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: "RedisConfig topology, tracking, and Memorystore IAM fields"
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Free-threaded Python 3.14t/3.15t GIL_USED test"
---

# Chapter 03: Rust RESP3 Clients, CLIENT TRACKING, Sentinel/Cluster, Pipelines, Streams, and Listeners

<!-- truth: start -->
- `client_core.rs` negotiates `HELLO 3` (`SETNAME`, ACL/IAM `AUTH`) over `ConnectionManager` / `ClusterClient` for both Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0`, supporting `SCAN`+`UNLINK`, `PX` expiries, `MULTI`/`EXEC`/`WATCH`, and `EVALSHA`->`NOSCRIPT`->`EVAL` [^redis-cfg].
- `caching.rs` and `topology.rs` implement RESP3 push frames + `CLIENT TRACKING ON [PREFIX ...] BCAST` bounded LRU invalidation, Sentinel `get-master-addr-by-name`, and Cluster CRC16 `MOVED` (slot-map update) / `ASK` (`ASKING`) routing [^redis-cfg].
- `py_client.rs`, `pubsub.rs`, and `streams.rs` expose `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`, `SyncRedisListener`, and `AsyncRedisListener` plus `XREADGROUP`/`XACK`/`XAUTOCLAIM` with `GIL_USED == False` [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Constructing `SyncRedisDriver` or `AsyncRedisDriver` via `provide_driver` must validate configuration in memory without dialing a live TCP port until `connect()` or the first command executes, while supporting multiplexed RESP3 pipelining, `CLIENT TRACKING` push invalidations, Sentinel/Cluster topologies, Streams, and dedicated Pub/Sub sockets off the Python GIL [^py-async] [^redis-cfg].

**Non-Goals:** Python adapter cutover (`ch04`); `SyncNotificationSource` / `AsyncNotificationSource` protocol split (`ch05`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Core & pipelines | `src/rs/vertebra-redis/src/{value,client_core}.rs` | `HELLO 3`, KV ops, `MULTI`/`EXEC` pipelines, Lua `EVALSHA` fallback |
| Tracking & topology | `src/rs/vertebra-redis/src/{caching,topology}.rs` [^redis-cfg] | `CLIENT TRACKING` push invalidations, Cluster `MOVED`/`ASK`, Sentinel |
| PyO3, Pub/Sub & Streams | `src/rs/vertebra-redis/src/{py_client,pubsub,streams}.rs`, `_redis.pyi` [^py-async] [^ft-gil-test] | `SyncRedis*` / `AsyncRedis*` clients, pipelines, listeners, and Streams |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

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

`vertebra._redis` stub docs execute in `ch07:t03`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R6` | `t01` | `src/rs/vertebra-redis/src/{value,client_core,lib}.rs` | `cargo test -p vertebra-redis` |
| `R7` | `t02` | `src/rs/vertebra-redis/src/{caching,topology,client_core}.rs` | `cargo test -p vertebra-redis` |
| `R8` | `t03` | `src/rs/vertebra-redis/src/{py_client,lib}.rs`, `src/py/vertebra/_redis.pyi` | `mypy.stubtest vertebra._redis` |
| `R9` | `t04` | `src/rs/vertebra-redis/src/{pubsub,streams}.rs`, `src/py/vertebra/_redis.pyi` | `make redis-test && pytest test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-value-codec-pipeline-and-lua-scripts.md](tasks/t01-value-codec-pipeline-and-lua-scripts.md) — Implement value.rs and client_core.rs with HELLO 3, KV/TTL/SCAN+UNLINK commands, MULTI/EXEC pipelines, and Lua scripts.
- [ ] [t02-client-tracking-cluster-and-sentinel.md](tasks/t02-client-tracking-cluster-and-sentinel.md) — Implement caching.rs and topology.rs for CLIENT TRACKING push invalidations, Sentinel failover, and Cluster MOVED/ASK routing.
- [ ] [t03-pyo3-clients-and-pipelines.md](tasks/t03-pyo3-clients-and-pipelines.md) — Expose lazy-dial SyncRedisClient/AsyncRedisClient and SyncRedisPipeline/AsyncRedisPipeline in vertebra._redis and _redis.pyi.
- [ ] [t04-pubsub-listeners-and-streams.md](tasks/t04-pubsub-listeners-and-streams.md) — Implement pubsub.rs (SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE) and streams.rs (XREADGROUP/XACK/XAUTOCLAIM) and verify 3.14t/3.15t GIL safety.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Client construction never opens network sockets eagerly; every blocking command releases the GIL via `py.detach`.
- **Fast Chapter Gate (`< 60s`)**: `make redis-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py`

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED test.
