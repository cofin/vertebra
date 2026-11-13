---
type: Task
id: "native-redis-driver:ch03:t03"
flow_id: native-redis-driver
chapter_id: ch03-rust-clients-topologies-pubsub-and-streams
title: "PyO3 SyncRedisClient, AsyncRedisClient, SyncRedisPipeline, and AsyncRedisPipeline"
description: "Expose lazy-dial SyncRedisClient/AsyncRedisClient and SyncRedisPipeline/AsyncRedisPipeline in vertebra._redis and _redis.pyi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch03:t02"
files:
  - src/rs/vertebra-redis/src/py_client.rs
  - src/rs/vertebra-redis/src/lib.rs
  - src/py/vertebra/_redis.pyi
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
read_set:
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-redis/src/client_core.rs
  - src/py/vertebra/_redis.pyi
estimated_loc: 245
tags:
  - redis
  - pyo3
  - async
  - pipeline
sources:
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: mysql-pyi
    resource: src/py/vertebra/_mysql.pyi
    title: "Standalone extension stub precedent"
---

# Task `t03`: PyO3 SyncRedisClient, AsyncRedisClient, SyncRedisPipeline, and AsyncRedisPipeline

## Objective & Scope Boundary

Implement `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, and `AsyncRedisPipeline` in `py_client.rs` and synchronize `_redis.pyi` per [`../spec.md` §2](../spec.md) [^py-async] [^mysql-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier`.
- `src/rs/vertebra-redis/src/client_core.rs` — Rust client core and pipeline execution.
- `src/py/vertebra/_redis.pyi` — Type stubs for `vertebra._redis`.

## Contracts & Edge Cases

- Construct `SyncRedisClient` and `AsyncRedisClient` in lazy-dial mode so `provide_driver` + `close()` succeeds without a live server on `127.0.0.1:6379`.
- Drive `AsyncRedisClient` and `AsyncRedisPipeline` via `AsyncCompletionQueue` + `Notifier` + `cancel_operation(op_id)`.
- **Edge Case**: `reconnect(password=...)` refreshes dynamic IAM credentials cleanly without leaking tasks.

## Test Matrix

- Verify lazy construction + `close()` offline, async completion draining, and `mypy.stubtest vertebra._redis`.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._redis --concise && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, and `AsyncRedisPipeline` pass `stubtest`.
- [ ] Zero regressions in the chapter fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^mysql-pyi]: `src/py/vertebra/_mysql.pyi` — Standalone extension stub precedent.
