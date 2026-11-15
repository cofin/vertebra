---
type: Task
id: native-redis-driver:ch02-rust-clients-topologies-and-streams:t02
flow_id: native-redis-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Implement RESP2/RESP3 Value Codec, Atomic/Non-Atomic Pipelines, and Lua Script Cache
description: Implement RedisValue <-> PyObject conversion, atomic/non-atomic pipelines, and EVALSHA -> EVAL fallback in vertebra-redis.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch02-rust-clients-topologies-and-streams:t01
files:
  - src/rs/vertebra-redis/src/value.rs
  - src/rs/vertebra-redis/src/client_core.rs
  - src/rs/vertebra-redis/src/lib.rs
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
read_set:
  - src/rs/vertebra-redis/src/config.rs
  - src/rs/vertebra-redis/src/errors.rs
estimated_loc: 245
tags:
  - redis
  - resp3
  - pipeline
sources:
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: RedisConfig topology, tracking, and Memorystore IAM fields
verification_strategy: regression_tdd
---

# Task `t02`: Implement RESP2/RESP3 Value Codec, Atomic/Non-Atomic Pipelines, and Lua Script Cache

## Objective & Scope Boundary

Implement `value.rs` and `client_core.rs` for `HELLO 3`, core KV commands, `redis` crate `Pipeline` (`MULTI`/`EXEC` + non-atomic batching), and `EVALSHA`->`NOSCRIPT`->`EVAL` per [`../spec.md` §2](../spec.md) [^redis-cfg].

## Bounded Read Manifest

- `src/rs/vertebra-redis/src/config.rs` — `RedisClientConfig` and IAM token hook from `t01`.
- `src/rs/vertebra-redis/src/errors.rs` — `RedisError` mapping.

## Contracts & Edge Cases

- Support `get`, `set(ex, px, nx, xx)`, `delete`, `unlink`, `pttl`, `scan(cursor, match, count)`, `flushdb`, `publish`, `execute_command`, `execute_pipeline`, and `eval_script`.
- Convert RESP3 maps, sets, doubles, booleans, verbatim strings, big numbers, and bulk bytes faithfully between Rust and Python.
- **Edge Case**: Mutually exclusive `ex` and `px` or `nx` and `xx` flags validate cleanly.
- **Edge Case**: `NOSCRIPT` triggers exactly one `EVAL` upload-and-execute retry.

## Test Matrix

- Loopback RESP3 mock server tests verify `HELLO 3` handshake, pipelined round-trips, `SCAN`+`UNLINK`, and `EVALSHA`->`EVAL` fallback.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] RESP3 value conversions, pipelines, and Lua script caching pass Rust loopback tests.
- [ ] Zero regressions in the fast verification gate.

[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
