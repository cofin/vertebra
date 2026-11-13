---
type: Spec
flow_id: native-redis-driver
chapter_id: ch06-redis-notifications-and-resp3-cleanup
title: "Chapter 06: Legacy vertebra-db::resp3 Deletion and Redis/Valkey Pub/Sub Notification Sources"
description: "Delete legacy RESP3 bindings from vertebra-py/_vertebra.pyi and src/rs/vertebra-db/src/resp3/mod.rs, and implement SyncRedisNotificationSource, AsyncRedisNotificationSource, SyncValkeyNotificationSource, and AsyncValkeyNotificationSource."
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
  - pubsub
  - notifications
  - debloat
sources:
  - id: resp3-bindings
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: "Legacy RESP3 helper bindings on vertebra._vertebra"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Core vertebra._vertebra stub declarations"
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: "Legacy RESP3 codec module in vertebra-db"
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: "vertebra-db crate root re-exporting resp3"
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "NotificationSource capability protocol in capabilities.py"
---

# Chapter 06: Legacy vertebra-db::resp3 Deletion and Redis/Valkey Pub/Sub Notification Sources

<!-- truth: start -->
- `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` are removed from `vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi` [^resp3-bindings] [^vertebra-pyi].
- `src/rs/vertebra-db/src/resp3/mod.rs` and its `vertebra-db/src/lib.rs` exports are deleted completely [^resp3-rs] [^db-lib].
- `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` (`redis/notify.py`) and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` (`valkey/notify.py`) implement `SyncNotificationSource` and `AsyncNotificationSource` over `SyncRedisListener` and `AsyncRedisListener` (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) [^driver-caps].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Once `ch04` cuts Redis and Valkey adapters onto `vertebra._redis` and `ch05` lands `SyncNotificationSource` / `AsyncNotificationSource`, no caller remains for `vertebra_db::resp3` or its `_vertebra` bindings [^resp3-rs] [^resp3-bindings]. Deleting them and adding dedicated Redis and Valkey `Sync*`/`Async*` notification sources completes the pub/sub transport plane.

**Non-Goals:** Distributed queue stream backends (`ch07:t01`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| `_vertebra` cleanup | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi` [^resp3-bindings] [^vertebra-pyi] | RESP3 helper bindings removed |
| `vertebra-db` cleanup | `src/rs/vertebra-db/src/{resp3/mod.rs,lib.rs}` [^resp3-rs] [^db-lib] | 475-line `resp3` codec module deleted |
| Redis & Valkey notify | `adapters/redis/{notify,__init__}.py`, `adapters/valkey/{notify,__init__}.py` [^driver-caps] | `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, and Valkey subclasses |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R14**: Remove `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` from `vertebra-py/src/db.rs` and `_vertebra.pyi` (`t01`).
- **R15**: Delete `src/rs/vertebra-db/src/resp3/mod.rs` and remove `pub mod resp3;` from `src/rs/vertebra-db/src/lib.rs` (`t02`).
- **R16a**: Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` in `adapters/redis/notify.py` supporting `subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `publish`, `listen()`, and `ChangeFeed` hydration (`t03`).
- **R16b**: Implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `adapters/valkey/notify.py` defaulting `ValkeyConfig` and probing `vertebra-core[valkey]` (`t04`).

```python
class SyncRedisNotificationSource(SyncNotificationSource):
    def __init__(self, driver_or_config: SyncRedisDriver | RedisConfig | None = None, *, change_feed: ChangeFeed | None = None) -> None: ...
    def publish(self, channel: str, payload: bytes | str) -> None: ...
    def subscribe(self, channels: Sequence[str]) -> None: ...
    def psubscribe(self, patterns: Sequence[str]) -> None: ...
    def ssubscribe(self, shard_channels: Sequence[str]) -> None: ...
    def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> Iterator[NotificationEvent]: ...
    def close(self) -> None: ...

class AsyncRedisNotificationSource(AsyncNotificationSource):
    # Async mirror over AsyncRedisListener + AsyncRedisDriver, with aclose() and async context manager
    ...

class SyncValkeyNotificationSource(SyncRedisNotificationSource): ...
class AsyncValkeyNotificationSource(AsyncRedisNotificationSource): ...
```

**Integration Flow:**
- `ChannelBus(notification_source=AsyncRedisNotificationSource(driver))` streams live Redis/Valkey pub/sub frames into SSE/WebSocket channels.
- Synchronous CLI/Flask/Django workers consume `SyncRedisNotificationSource.listen()` directly.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Core `_vertebra` probe | `hasattr(vertebra._vertebra, "encode_resp_command")` | `False`; `mypy.stubtest vertebra._vertebra` passes | Clean core binary |
| Broken `async for` loop | Consumer exits `async for event in source.listen():` via `break` | Dedicated `AsyncRedisListener` closes deterministically | Zero leaked pub/sub sockets |

## 3. Documentation & OKF Knowledge Plan

Pub/sub notification sources and `vertebra-db` table updates are documented in `ch07:t03` and `ch08:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R14` | `t01` | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi` | `mypy.stubtest vertebra._vertebra` |
| `R15` | `t02` | `src/rs/vertebra-db/src/{resp3/mod.rs,lib.rs}` | `cargo test -p vertebra-db` |
| `R16a` | `t03` | `src/py/vertebra/db/adapters/redis/{notify,__init__}.py` | `pytest src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `R16b` | `t04` | `src/py/vertebra/db/adapters/valkey/{notify,__init__}.py` | `pytest src/py/tests/unit/db/test_redis_valkey_resp3.py` |

## 5. Implementation Plan

- [ ] [t01-remove-vertebra-py-resp3-bindings.md](tasks/t01-remove-vertebra-py-resp3-bindings.md) — Delete encode_resp_command, encode_hello3_handshake, crc16_xmodem, redis_hash_slot, and parse_cluster_redirect from _vertebra.
- [ ] [t02-delete-vertebra-db-resp3-module.md](tasks/t02-delete-vertebra-db-resp3-module.md) — Delete legacy resp3 codec module and remove pub mod resp3 re-exports from vertebra-db/src/lib.rs.
- [ ] [t03-redis-pubsub-notification-sources.md](tasks/t03-redis-pubsub-notification-sources.md) — Create redis/notify.py implementing SyncNotificationSource and AsyncNotificationSource over SyncRedisListener/AsyncRedisListener.
- [ ] [t04-valkey-pubsub-notification-sources.md](tasks/t04-valkey-pubsub-notification-sources.md) — Create valkey/notify.py defaulting ValkeyConfig and probing vertebra-core[valkey], lazy-exported in valkey/__init__.py.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `vertebra._vertebra` and `vertebra-db` carry zero Redis/RESP3 codec code after `t02`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py`

[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate root re-exporting resp3.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
