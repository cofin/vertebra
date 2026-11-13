---
type: Task
id: "native-redis-driver:ch03:t04"
flow_id: native-redis-driver
chapter_id: ch03-rust-clients-topologies-pubsub-and-streams
title: "Dedicated SyncRedisListener/AsyncRedisListener Pub/Sub and Redis Streams Primitives"
description: "Implement pubsub.rs (SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE) and streams.rs (XREADGROUP/XACK/XAUTOCLAIM) and verify 3.14t/3.15t GIL safety."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch03:t03"
files:
  - src/rs/vertebra-redis/src/pubsub.rs
  - src/rs/vertebra-redis/src/streams.rs
  - src/py/vertebra/_redis.pyi
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
  - src/py/tests/unit/test_freethreaded_gil.py
read_set:
  - src/rs/vertebra-redis/src/py_client.rs
  - src/py/vertebra/_redis.pyi
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 245
tags:
  - redis
  - valkey
  - pubsub
  - streams
  - freethreaded
sources:
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Free-threaded Python 3.14t/3.15t GIL_USED test"
---

# Task `t04`: Dedicated SyncRedisListener/AsyncRedisListener Pub/Sub and Redis Streams Primitives

## Objective & Scope Boundary

Implement `SyncRedisListener` and `AsyncRedisListener` (`pubsub.rs`) plus Streams primitives (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`, `xrange`, `xdel`, `zadd` in `streams.rs`) and register `vertebra._redis` in `test_freethreaded_gil.py` per [`../spec.md` §2](../spec.md) [^py-async] [^ft-gil-test].

## Bounded Read Manifest

- `src/rs/vertebra-redis/src/py_client.rs` — Client handle and completion queue wiring.
- `src/py/vertebra/_redis.pyi` — Listener and stream stub declarations.
- `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded `GIL_USED == False` checks (`TEST-10`).

## Contracts & Edge Cases

- Support channel (`subscribe`), pattern (`psubscribe`), and Redis 7+/Valkey sharded (`ssubscribe`) subscriptions with bounded message ring buffering and timeout-aware `next_message`.
- Handle `BUSYGROUP` idempotently in `xgroup_create(..., mkstream=True)` when requested by callers.
- **Edge Case**: `next_message(timeout_ms=...)` returns `None` cleanly on idle timeout without tearing down the subscription.

## Test Matrix

- Loopback tests cover `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE` delivery, `XADD` -> `XREADGROUP` -> `XACK` -> `XAUTOCLAIM`, `mypy.stubtest vertebra._redis`, and `test_freethreaded_gil.py`.

## Verification

```bash
make redis-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q
```

## Acceptance Criteria

- [ ] `make redis-test` and `test_freethreaded_gil.py` pass with full Pub/Sub and Streams coverage.
- [ ] Zero regressions in the chapter fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED test.
