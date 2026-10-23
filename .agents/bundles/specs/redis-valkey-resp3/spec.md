---
type: Spec
flow_id: redis-valkey-resp3
title: "Redis & Valkey RESP3 Topology Routing, Client Tracking & Pub/Sub Multiplexing"
state: planned
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-23T19:33:00Z
updated_at: 2026-10-23T19:33:00Z
description: "Rust-native and SHM-backed Redis 7.2+/8.0+ and Valkey 8.0+/9.0+ RESP3 drivers (src/rs/vertebra-db + vertebra.db.{redis,valkey}): Cluster CRC16 slot routing and Sentinel discovery, live socket wiring for Valkey CLIENT TRACKING BCAST push invalidation and GCP Memorystore IAM token refresh, and multiplexed Pub/Sub subscriber streams."
tags: [feature, rust, database, redis, valkey, resp3, shm]
parent_prd: shm-native-drivers-and-broker-proxy
research: []
---

# Flow: Redis & Valkey RESP3 Topology Routing, Client Tracking & Pub/Sub Multiplexing

**Flow ID:** `redis-valkey-resp3`

## Specification

### Code Analysis Summary

- [`src/py/vertebra/db/redis.py`](../../../../src/py/vertebra/db/redis.py): Implements `Resp3Parser`, `RedisConfig`, `SyncRedisDriver`, and `AsyncRedisDriver` with `HELLO 3` handshakes, command pipelining, Lua `EVALSHA`/`EVAL` fallback, and basic single-socket `SUBSCRIBE`. `RedisConfig` declares `topology: Literal["standalone", "sentinel", "cluster"]`, `sentinel_service_name`, `cluster_nodes`, and `enable_gcp_iam_auth`, but `_connect_socket` and `_create_connection` currently connect only to the single `url` host/port without Sentinel master discovery or Cluster CRC16 slot routing (`MOVED`/`ASK` redirection).
- [`src/py/vertebra/db/valkey.py`](../../../../src/py/vertebra/db/valkey.py): Implements `ValkeyConfig`, `SyncValkeyDriver`, and `AsyncValkeyDriver`. `ValkeyConfig.build_handshake_commands` builds `HELLO 3` and `CLIENT TRACKING ON [PREFIX ...] BCAST` wire frames, and `SyncValkeyDriver`/`AsyncValkeyDriver` register `_on_push_frame` for `invalidate` push frames, but `_connect_socket` and `_create_connection` inherited from `SyncRedisDriver`/`AsyncRedisDriver` send only `encode_hello3_handshake` rather than executing the full `build_handshake_commands` sequence on newly opened sockets.
- [`src/py/vertebra/extensions/litestar/channels.py`](../../../../src/py/vertebra/extensions/litestar/channels.py) and [`src/py/vertebra/extensions/litestar/store.py`](../../../../src/py/vertebra/extensions/litestar/store.py): Litestar Channels and Store integrations that can optionally delegate to `AsyncRedisDriver` / `AsyncValkeyDriver` when configured for multi-worker deployments.

### Relevant Patterns

- [Python Module Structure, Class Layout & PEP 695 Typing](../../knowledge/patterns/python/module-and-class-layout.md)
- [Hot-Path Caching & Single-Wake Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md)
- [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md)

### Requirements

1. **Valkey Socket Handshake & GCP Memorystore IAM Refresh**:
   - Wire `ValkeyConfig.build_handshake_commands` into `SyncValkeyDriver._connect_socket` and `AsyncValkeyDriver._create_connection` so live TCP/TLS sockets execute both `HELLO 3` (with optional IAM token auth) and `CLIENT TRACKING ON ... BCAST` before entering the pool.
   - Add background/proactive RESP3 push frame draining on idle pooled connections or dedicated tracking sockets so `invalidate` push notifications evict `_client_cache` entries without waiting for the next outbound command on that socket.
2. **Sentinel Master Discovery & Cluster CRC16 Slot Routing**:
   - Implement `SENTINEL get-master-addr-by-name <service>` resolution when `topology == "sentinel"`.
   - Implement CRC16-XMODEM hash-slot calculation (`key` or `{hash_tag}` -> `0..16383`) and `MOVED` / `ASK` redirection handling across `cluster_nodes` when `topology == "cluster"`.
3. **Multiplexed Pub/Sub & Bounded Client Cache**:
   - Bound `_client_cache` in `SyncValkeyDriver` and `AsyncValkeyDriver` with a configurable `max_cached_keys: int = 4096` LRU eviction cap so client-side caching cannot grow unbounded.
   - Support multiplexed channel subscriptions (`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`) over a shared subscriber connection.

## Implementation Plan

### Phase 1: Valkey Handshake Wiring & Bounded Client Tracking Cache

- [ ] 1.1 Wire `ValkeyConfig.build_handshake_commands`, GCP Memorystore IAM token provider, and bounded LRU `_client_cache` into `SyncValkeyDriver` and `AsyncValkeyDriver`
- [ ] 1.2 Implement Sentinel master resolution and Cluster CRC16 hash-slot routing (`MOVED`/`ASK`) in `SyncRedisDriver` and `AsyncRedisDriver`

### Phase 2: Multiplexed RESP3 Pub/Sub & Verification

- [ ] 2.1 Add multiplexed RESP3 `SUBSCRIBE`/`UNSUBSCRIBE` channel fan-out and end-to-end loopback socket tests for Redis and Valkey

## Continuity Snapshot

- **Active Flow**: `redis-valkey-resp3` (`planned`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `null`
- **Decisions**:
  - Deferred Redis and Valkey RESP3 topology routing, live `CLIENT TRACKING` socket handshake wiring, and pub/sub multiplexing into a standalone `planned` Flow spec so core SQLSpec/Oracle consolidation lands cleanly first.
- **Recent Discoveries**:
  - `ValkeyConfig.build_handshake_commands` constructs both `HELLO 3` and `CLIENT TRACKING ON ... BCAST`, but `SyncRedisDriver._connect_socket` and `AsyncRedisDriver._create_connection` only send `encode_hello3_handshake`.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Activate flow and claim Task `1.1` when ready to implement Redis/Valkey enhancements.
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
  - `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`
