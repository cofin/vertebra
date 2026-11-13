---
type: Task
id: "native-redis-driver:ch03:t02"
flow_id: native-redis-driver
chapter_id: ch03-rust-clients-topologies-pubsub-and-streams
title: "RESP3 CLIENT TRACKING BCAST Cache, Sentinel Discovery, and Cluster Slot Routing"
description: "Implement caching.rs and topology.rs for CLIENT TRACKING push invalidations, Sentinel failover, and Cluster MOVED/ASK routing."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch03:t01"
files:
  - src/rs/vertebra-redis/src/caching.rs
  - src/rs/vertebra-redis/src/topology.rs
  - src/rs/vertebra-redis/src/client_core.rs
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
read_set:
  - src/rs/vertebra-redis/src/client_core.rs
  - src/py/vertebra/db/adapters/redis/config.py
estimated_loc: 245
tags:
  - redis
  - valkey
  - cluster
  - sentinel
  - caching
sources:
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: "RedisConfig topology, tracking, and Memorystore IAM fields"
---

# Task `t02`: RESP3 CLIENT TRACKING BCAST Cache, Sentinel Discovery, and Cluster Slot Routing

## Objective & Scope Boundary

Implement bounded LRU `CLIENT TRACKING ON [PREFIX ...] BCAST` push invalidation (`caching.rs`), Sentinel `get-master-addr-by-name`, and Cluster `MOVED`/`ASK` slot routing (`topology.rs`) per [`../spec.md` §2](../spec.md) [^redis-cfg].

## Bounded Read Manifest

- `src/rs/vertebra-redis/src/client_core.rs` — Core RESP3 connection manager from `t01`.
- `src/py/vertebra/db/adapters/redis/config.py` — Tracking, Cluster, and Sentinel config contracts.

## Contracts & Edge Cases

- Evict invalidated keys immediately on RESP3 `invalidate` push frames (including `FLUSHDB` `null` invalidation) and queue events for `drain_invalidations()`.
- Update `slot_map()` on `MOVED`, send `ASKING` without mutating `slot_map()` on `ASK`, and cap hops at `max_redirections`.
- **Edge Case**: `client_cache_capacity` enforces strict LRU upper bound with zero unbounded growth.

## Test Matrix

- Loopback tests cover `BCAST` prefix push invalidation, `null` flush invalidation, `MOVED` vs `ASK` slot-map behavior, redir-cap overflow, and Sentinel master resolution.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] Client-side tracking, Cluster `MOVED`/`ASK`, and Sentinel discovery pass Rust loopback tests.
- [ ] Zero regressions in the chapter fast verification gate.

[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
