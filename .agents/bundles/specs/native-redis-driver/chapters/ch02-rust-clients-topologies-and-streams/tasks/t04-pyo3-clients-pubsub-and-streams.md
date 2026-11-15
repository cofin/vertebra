---
type: Task
id: native-redis-driver:ch02-rust-clients-topologies-and-streams:t04
flow_id: native-redis-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Expose PyO3 Sync/AsyncRedisClient, Pipelines, Pub/Sub Listeners, and Streams
description: Bind SyncRedisClient, AsyncRedisClient, Sync/AsyncRedisPipeline, Sync/AsyncRedisListener, and Stream (XADD/XREADGROUP/XACK/XAUTOCLAIM) helpers in vertebra._redis.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch02-rust-clients-topologies-and-streams:t03
files:
- src/rs/vertebra-redis/src/py_client.rs
- src/rs/vertebra-redis/src/lib.rs
- src/py/vertebra/_redis.pyi
- src/rs/vertebra-redis/src/pubsub.rs
- src/rs/vertebra-redis/src/streams.rs
- src/py/tests/unit/test_freethreaded_gil.py
tests:
- src/rs/vertebra-redis/tests/redis_client.rs
- src/py/tests/unit/test_freethreaded_gil.py
read_set:
- src/rs/vertebra-db/src/py_async.rs
- src/rs/vertebra-redis/src/client_core.rs
- src/py/vertebra/_redis.pyi
- src/rs/vertebra-redis/src/lib.rs
- src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 245
tags:
- redis
- pyo3
- async
- pipeline
- valkey
- pubsub
- streams
- freethreaded
sources:
- id: py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: AsyncCompletionQueue and Notifier PyO3 bridge
- id: mysql-pyi
  resource: src/py/vertebra/_mysql.pyi
  title: Standalone extension stub precedent
- id: ft-gil-test
  resource: src/py/tests/unit/test_freethreaded_gil.py
  title: Free-threaded Python 3.14t/3.15t GIL_USED test
---

# Task `t04`: Expose PyO3 Sync/AsyncRedisClient, Pipelines, Pub/Sub Listeners, and Streams

## Objective & Scope Boundary

Implement `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, and `AsyncRedisPipeline` (`py_client.rs`), implement `SyncRedisListener` and `AsyncRedisListener` (`pubsub.rs`) plus Streams helpers (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`, `xrange`, `xdel`, `zadd` in `streams.rs`), synchronize `_redis.pyi`, and register `vertebra._redis` in `test_freethreaded_gil.py` per [`../spec.md` §2](../spec.md) [^py-async] [^mysql-pyi] [^ft-gil-test].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier`.
- `src/rs/vertebra-redis/src/client_core.rs` — Rust client core and pipeline execution from `t02`–`t03`.
- `src/py/vertebra/_redis.pyi` — Type stubs for `vertebra._redis`.
- `src/rs/vertebra-redis/src/lib.rs` — Module exports and `#[pymodule(gil_used = false)]` registrations.
- `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded `GIL_USED == False` checks (`TEST-10`).

## Contracts & Edge Cases

- Construct `SyncRedisClient` and `AsyncRedisClient` in lazy-dial mode so `provide_driver` + `close()` succeeds offline without dialing `127.0.0.1:6379`.
- Drive `AsyncRedisClient`, `AsyncRedisPipeline`, and `AsyncRedisListener` completions via `AsyncCompletionQueue` + `Notifier` + `cancel_operation(op_id)`.
- Support channel (`subscribe`), pattern (`psubscribe`), and Cluster sharded (`ssubscribe`) subscriptions with bounded message ring buffering and timeout-aware `next_message`, plus idempotent `BUSYGROUP` handling in `xgroup_create(..., mkstream=True)`.
- **Edge Case**: `reconnect(password=...)` refreshes dynamic IAM credentials cleanly without leaking tasks.
- **Edge Case**: `next_message(timeout_ms=...)` returns `None` cleanly on idle timeout without tearing down the subscription.

## Test Matrix

- Loopback tests cover lazy construction + `close()` offline, async completion draining, `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE` delivery, and `XADD` -> `XREADGROUP` -> `XACK` -> `XAUTOCLAIM`.
- `mypy.stubtest vertebra._redis` and `test_freethreaded_gil.py` (`GIL_USED == False`) pass cleanly.

## Verification

```bash
make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis --locked && uv run --no-sync python -m mypy.stubtest vertebra._redis --concise && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q
```

## Acceptance Criteria

- [ ] `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`, `SyncRedisListener`, `AsyncRedisListener`, and Streams helpers pass `stubtest`.
- [ ] `cargo test -p vertebra-redis` and `test_freethreaded_gil.py` (`GIL_USED == False`) pass.
- [ ] Zero regressions in the fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^mysql-pyi]: `src/py/vertebra/_mysql.pyi` — Standalone extension stub precedent.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED test.
