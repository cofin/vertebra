---
type: Spec
flow_id: native-valkey-driver
chapter_id: ch03-python-cutover-and-notifications
title: 'Chapter 03: Python Valkey Cutover, Notifications, Legacy RESP3 Cleanup, and Queue Backend'
description: Cut SyncValkeyDriver, AsyncValkeyDriver, ValkeyCore, SyncValkeyNotificationSource, AsyncValkeyNotificationSource, and ValkeyQueueBackend over to vertebra._valkey and remove legacy RESP3 bindings from vertebra-py and vertebra-db.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-rust-clients-topologies-and-streams
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
tags:
  - python
  - valkey
  - pubsub
  - queues
  - debloat
  - pyo3
sources:
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore RESP3 command builder and hash slot helpers
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource
    title: SyncNotificationSource and AsyncNotificationSource capability protocols
  - id: resp3-bindings
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: Legacy RESP3 helper bindings on vertebra._vertebra
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: Core vertebra._vertebra stub declarations
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: Legacy RESP3 codec module in vertebra-db
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db crate root re-exporting resp3
  - id: queue-valkey
    resource: src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend
    title: Thin ValkeyQueueBackend(RedisQueueBackend) subclass moved to queues/valkey.py by native-redis-driver
current_task: "native-valkey-driver:ch03-python-cutover-and-notifications:t01"
---

# Chapter 03: Python Valkey Cutover, Notifications, Legacy RESP3 Cleanup, and Queue Backend

<!-- truth: start -->
- `ValkeyConfig`, `ValkeyCore`, `SyncValkeyDriver`, and `AsyncValkeyDriver` (`src/py/vertebra/db/adapters/valkey/{config,core,driver,__init__}.py`) cut over to `vertebra._valkey` (`ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")`), exposing Valkey 9.0 `hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, and `hpersist` with zero Python socket/asyncio loops and zero imports from `adapters/redis/` [^valkey-drv] [^valkey-cfg] [^valkey-core].
- `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` (`src/py/vertebra/db/adapters/valkey/notify.py`) implement `SyncNotificationSource` and `AsyncNotificationSource` over `vertebra._valkey.SyncValkeyListener` and `AsyncValkeyListener` [^driver-caps].
- Legacy RESP3 bindings (`encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, `parse_cluster_redirect`) are removed from `src/rs/vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi`, and `src/rs/vertebra-db/src/resp3/mod.rs` is deleted [^resp3-bindings] [^vertebra-pyi] [^resp3-rs] [^db-lib].
- `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py`) replaces `redis.asyncio` with `_ValkeyStreamAdapter` over `vertebra._valkey.AsyncValkeyClient` (`valkey://`, `valkeys://`, `unix://`) while keeping `client: ValkeyStreamClient | None = None` injection [^queue-valkey].
<!-- truth: end -->

## 1. Specification, Context & Provenance

With `src/rs/vertebra-valkey` (`vertebra._valkey`) in place from Chapter 02 and `adapters/valkey/` + `framework/queues/valkey.py` decoupled from Redis in `native-redis-driver:ch03`, this chapter cuts all Python Valkey drivers, notification sources, and stream queues over to `vertebra._valkey`. It also deletes the last remaining legacy RESP3 bindings from `vertebra-py` and `vertebra-db` [^valkey-drv] [^resp3-rs] [^queue-valkey].

**Non-Goals:** Redis adapter/queue code (`native-redis-driver`) and distributed `ChannelBus` / `ValkeyStore` consolidation (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Valkey driver cutover | `src/py/vertebra/db/adapters/valkey/{config,core,driver,__init__}.py` [^valkey-drv] [^valkey-cfg] [^valkey-core] | `SyncValkeyDriver`, `AsyncValkeyDriver` (`hexpire`, `hpexpire`, `httl`, `hpttl`, `hpersist`) over `vertebra._valkey` |
| Valkey notifications | `src/py/vertebra/db/adapters/valkey/{notify,__init__}.py` [^driver-caps] | `SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource` |
| Legacy RESP3 removal | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi`, `src/rs/vertebra-db/src/{lib.rs,resp3/mod.rs}` [^resp3-bindings] [^vertebra-pyi] [^resp3-rs] [^db-lib] | Delete `vertebra_db::resp3` and `_vertebra` RESP3 wrappers |
| Valkey queue cutover | `src/py/vertebra/framework/queues/valkey.py` [^queue-valkey] | `_ValkeyStreamAdapter` over `vertebra._valkey.AsyncValkeyClient` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../../../knowledge/domains/framework/distributed-queues-and-email.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R12**: Cut `ValkeyConfig`, `ValkeyCore`, `SyncValkeyDriver`, and `AsyncValkeyDriver` over to `vertebra._valkey`, adding `hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, and `hpersist` on both sync and async drivers (`t01`).
- **R13**: Implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `src/py/vertebra/db/adapters/valkey/notify.py` and lazy-export from `valkey/__init__.py` (`t02`).
- **R14**: Delete `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` from `src/rs/vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi`, and delete `src/rs/vertebra-db/src/resp3/mod.rs` + `pub mod resp3;` from `src/rs/vertebra-db/src/lib.rs` (`t03`).
- **R15**: Cut `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py`) over to `_ValkeyStreamAdapter` wrapping `vertebra._valkey.AsyncValkeyClient`, removing the lazy `ensure_module("redis.asyncio")` load from `src/py/vertebra` (`t04`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Missing `vertebra._valkey` | `SyncValkeyDriver()` / `ValkeyQueueBackend()` without wheel | Raises `MissingDependencyError` naming `vertebra-core[valkey]` | Fail-loud wheel guard |
| Legacy RESP3 probe | `hasattr(vertebra._vertebra, "encode_resp_command")` | `False`; `src/rs/vertebra-db/src/resp3/mod.rs` deleted | Zero legacy RESP3 duplication |
| `redis.asyncio` probe | `git grep 'redis.asyncio' src/py/vertebra` | Zero matches under `src/py/vertebra`; the `test_distributed_queues.py` missing-dependency tests are rewritten for `vertebra-core[valkey]` | Third-party `redis-py` removed from production code |

## 3. Documentation & OKF Knowledge Plan

Public docs, `native-stubs.md`, `product.md`, and `valkey-limitations.md` are updated in Chapter 04.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut Python SyncValkeyDriver and AsyncValkeyDriver Over to vertebra._valkey | `src/py/vertebra/db/adapters/valkey/{config,core,driver,__init__}.py` | `src/py/tests/unit/db/test_redis_valkey_driver.py` |
| `t02` | Implement Sync/Async Valkey Pub/Sub Notification Sources | `src/py/vertebra/db/adapters/valkey/{notify,__init__}.py` | `src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `t03` | Remove Legacy RESP3 Bindings from vertebra-py and Delete vertebra-db/src/resp3/mod.rs | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi`, `src/rs/vertebra-db/src/{lib.rs,resp3/mod.rs}` | `mypy.stubtest vertebra._vertebra` |
| `t04` | Cut ValkeyQueueBackend Over to vertebra._valkey Streams and ZSETs | `src/py/vertebra/framework/queues/valkey.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |

## 5. Implementation Plan

- [ ] [t01-python-valkey-driver-cutover.md](tasks/t01-python-valkey-driver-cutover.md) — Delegate `ValkeyConfig`, `ValkeyCore`, `SyncValkeyDriver`, and `AsyncValkeyDriver` (including Valkey 9.0 hash-field expiration methods) to `vertebra._valkey`.
- [ ] [t02-valkey-pubsub-notifications.md](tasks/t02-valkey-pubsub-notifications.md) — Implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `src/py/vertebra/db/adapters/valkey/notify.py`.
- [ ] [t03-remove-legacy-resp3-bindings.md](tasks/t03-remove-legacy-resp3-bindings.md) — Delete legacy RESP3 functions from `vertebra-py`/`_vertebra.pyi` and remove `src/rs/vertebra-db/src/resp3/mod.rs`.
- [ ] [t04-native-valkey-queue-backend.md](tasks/t04-native-valkey-queue-backend.md) — Wire `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py`) onto `vertebra._valkey.AsyncValkeyClient`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero imports between `valkey` and `redis` Python modules; `mypy.stubtest vertebra._vertebra` and `mypy.stubtest vertebra._valkey` pass.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-valkey --features vertebra-db/gcp && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._valkey --concise && uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/framework/test_distributed_queues.py -q`

[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore RESP3 command builder and hash slot helpers.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource` — `SyncNotificationSource` and `AsyncNotificationSource` capability protocols.
[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate root re-exporting resp3.
[^queue-valkey]: `src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend` — Standalone `ValkeyQueueBackend` extracted to `queues/valkey.py` by `native-redis-driver`.
