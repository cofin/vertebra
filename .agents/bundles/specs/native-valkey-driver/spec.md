---
type: Spec
flow_id: native-valkey-driver
parent_prd: native-data-platform
title: Native Valkey Driver (`vertebra-valkey` / `vertebra._valkey`)
description: Build the standalone Rust PyO3 native Valkey driver wheel (`vertebra-valkey` / `vertebra._valkey`), cut over SyncValkeyDriver, AsyncValkeyDriver, ValkeyNotificationSource, and ValkeyQueueBackend with Valkey 9.0 hash-field expiration (`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`), `CLIENT CAPA redirect`, `CLUSTER SHARDS`, and Google Cloud Memorystore for Valkey IAM, and delete legacy RESP3 helpers from `vertebra-py`/`vertebra-db`.
status: stable
state: planned
has_chapters: true
chapters:
  - ch01-wheel-scaffold-and-isolation
  - ch02-rust-clients-topologies-and-streams
  - ch03-python-cutover-and-notifications
  - ch04-conformance-and-docs
current_chapter: ch01-wheel-scaffold-and-isolation
current_task: native-valkey-driver:ch01:t01
depends_on_flows: []
plan_revision: 2
plan_commit: 473e1f2e
created_at: '2026-11-15T20:55:00Z'
updated_at: '2026-11-15T23:30:00Z'
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
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
  - rust
  - pyo3
  - valkey
  - resp3
  - cluster
  - sentinel
  - pubsub
  - streams
  - gcp
sources:
  - id: prd-spec
    resource: .agents/bundles/specs/native-data-platform/spec.md
    title: Native Data Platform & First-Party Driver Parity Master PRD
  - id: redis-spec
    resource: .agents/bundles/knowledge/domains/data/redis-limitations.md
    title: Archived Native Redis Driver operational boundaries and wheel baseline
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig with Google Cloud Memorystore IAM and tracking fields
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore RESP3 command builder and cluster hash slot routing
  - id: queue-valkey
    resource: src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend
    title: Thin ValkeyQueueBackend(RedisQueueBackend) subclass moved to queues/valkey.py by native-redis-driver
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame
    title: Legacy RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation
  - id: resp3-bindings
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: Legacy RESP3 PyO3 helper functions on vertebra._vertebra
  - id: gcp-token
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI
  - id: root-pyproject
    resource: pyproject.toml
    title: Root pyproject.toml extras and wheel exclusions
  - id: areas-toml
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area capability manifest
current_chapter: ch01-wheel-scaffold-and-isolation
---

# Native Valkey Driver (`vertebra-valkey` / `vertebra._valkey`)

<!-- truth: start -->
- `src/rs/vertebra-valkey` compiles to standalone wheel `vertebra-valkey` (`vertebra._valkey` + `src/py/vertebra/_valkey.pyi`, `#[pymodule(gil_used = false)]`) behind `vertebra-core[valkey]`, providing Rust RESP3 `SyncValkeyClient`, `AsyncValkeyClient` (`_vertebra_valkey_completion_drainer`), `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `SyncValkeyListener`, and `AsyncValkeyListener` [^prd-spec] [^root-pyproject].
- `vertebra._valkey` implements Valkey 8.0+/9.0 native optimizations in Rust: hash-field expiration (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`), `HELLO 3` + `CLIENT CAPA redirect`, `CLUSTER SHARDS` (with `CLUSTER SLOTS` fallback) + replica read routing (`READONLY`), `CLIENT TRACKING ON ... BCAST` L1 invalidation, Sentinel primary discovery, and Google Cloud Memorystore for Valkey IAM via `GcpTokenCacheCapsule` [^valkey-cfg] [^gcp-token].
- `SyncValkeyDriver`, `AsyncValkeyDriver`, `ValkeyCore`, `SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource` (`src/py/vertebra/db/adapters/valkey/`), and `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py`) delegate all wire I/O to `vertebra._valkey` with zero imports from `adapters/redis/` or `queues/redis.py` [^valkey-drv] [^valkey-core] [^queue-valkey].
- Once `ValkeyCore` cuts over to `vertebra._valkey`, legacy `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` bindings are removed from `vertebra._vertebra` (`src/rs/vertebra-py/src/db.rs` and `_vertebra.pyi`) and `src/rs/vertebra-db/src/resp3/mod.rs` is deleted [^resp3-rs] [^resp3-bindings].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Today `ValkeyConfig(RedisConfig)`, `ValkeyCore(RedisCore)`, `SyncValkeyDriver(SyncRedisDriver)`, `AsyncValkeyDriver(AsyncRedisDriver)`, and `ValkeyQueueBackend(RedisQueueBackend)` (in `queues/redis.py`) are thin subclasses of the Redis classes. After `native-redis-driver` decouples `src/py/vertebra/db/adapters/valkey/` and `src/py/vertebra/framework/queues/valkey.py` from Redis modules [^redis-spec], Valkey still runs over Python `socket`/`asyncio` I/O and legacy `vertebra._vertebra` RESP3 helpers [^valkey-drv] [^resp3-bindings]. This child flow of [`native-data-platform`](../native-data-platform/spec.md) (`t15`) builds the dedicated `src/rs/vertebra-valkey` crate and `vertebra-valkey` wheel (`vertebra._valkey` + `src/py/vertebra/_valkey.pyi`, installed via `vertebra-core[valkey]`) with Valkey-specific wire optimizations, cuts all Python Valkey adapters, notification sources, and stream queues over to `vertebra._valkey`, and deletes `src/rs/vertebra-db/src/resp3/mod.rs` and its `vertebra._vertebra` wrappers [^prd-spec] [^resp3-rs].

**Non-Goals:** Redis native wheel (`native-redis-driver`) and distributed `ChannelBus` / `ValkeyStore` consolidation (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel & CI | `src/rs/vertebra-valkey/`, `src/py/vertebra/_valkey.pyi`, `Makefile`, `.github/workflows/ci.yml`, `.github/workflows/release.yaml`, `areas.toml` [^root-pyproject] [^areas-toml] | `vertebra-valkey==0.3.0` (`[valkey]`), `make valkey-test`, `make valkey-wheel-check`, `FFI-09` |
| Rust clients & Valkey 9.0 | `src/rs/vertebra-valkey/src/{lib,config,codec,client,cluster,sentinel,tracking,pubsub,streams}.rs` [^valkey-cfg] [^gcp-token] | `SyncValkeyClient`, `AsyncValkeyClient`, `HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`, `CLIENT CAPA redirect`, `CLUSTER SHARDS` |
| Python Valkey cutover | `src/py/vertebra/db/adapters/valkey/{config,core,driver,notify,__init__}.py`, `src/py/vertebra/framework/queues/valkey.py` [^valkey-drv] [^queue-valkey] | `SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource`, `ValkeyQueueBackend` |
| Legacy RESP3 cleanup | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi`, `src/rs/vertebra-db/src/{lib.rs,resp3/mod.rs}` [^resp3-rs] [^resp3-bindings] | Zero RESP3 helpers left on `vertebra._vertebra`; `vertebra_db::resp3` deleted |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../knowledge/domains/framework/distributed-queues-and-email.md), [PyO3 0.30.1 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [Project Workflow](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R3 (Wheel, Stubs, Isolation & Review Manifests)**: Scaffold `src/rs/vertebra-valkey` (`vertebra._valkey`, `#[pymodule(gil_used = false)]`), add `[valkey] = ["vertebra-valkey==0.3.0"]` to `pyproject.toml`, author `src/py/vertebra/_valkey.pyi`, add `valkey-test` and `valkey-wheel-check` to `Makefile` and `.github/workflows/release.yaml`, exclude `_valkey.pyi` from the root wheel (`{ path = "src/py/vertebra/_valkey.pyi", format = "wheel" }`) and from sibling wheels (`"**/py/vertebra/_valkey.pyi"`), and register `vertebra-valkey` in `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` (`ch01`).
- **R4–R11 (Rust Clients, Valkey 9.0 Hash-Field TTL, Cluster, Sentinel, Tracking, Pub/Sub & Streams)**: Implement `ValkeyClientConfig` (`valkey://`, `valkeys://`, `unix://`; rejects `memory://` and `redis://`/`rediss://`), Google Cloud Memorystore for Valkey IAM (`GcpTokenCacheCapsule`), RESP3 codec + `HELLO 3` + `CLIENT CAPA redirect`, Valkey 9.0 hash-field expiration (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`), `CLIENT TRACKING ON ... BCAST` L1 cache, Cluster (`CLUSTER SHARDS` with `CLUSTER SLOTS` fallback, `MOVED`/`ASK`, `READONLY` replica reads), Sentinel discovery, dedicated `SyncValkeyListener`/`AsyncValkeyListener`, and Streams (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`) (`ch02`).
- **R12–R15 (Python Valkey Cutover, Notifications, Legacy RESP3 Cleanup & Queue Cutover)**: Cut `ValkeyConfig`, `ValkeyCore`, `SyncValkeyDriver`, and `AsyncValkeyDriver` over to `vertebra._valkey` (`ch03:t01`), implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` (`ch03:t02`), delete legacy RESP3 bindings from `vertebra-py`/`_vertebra.pyi` and delete `src/rs/vertebra-db/src/resp3/mod.rs` (`ch03:t03`), and cut `ValkeyQueueBackend` (`src/py/vertebra/framework/queues/valkey.py`) over to `vertebra._valkey.AsyncValkeyClient` (`ch03:t04`).
- **R16–R19 (Conformance, CI, Docs & OKF)**: Add Valkey 9.0 hash-field expiration and `CLIENT CAPA redirect` loopback tests (`ch04:t01`), and wire `vertebra._valkey` fail-loud checks on `CACHE_ENGINE_SPECS` (`valkey`) plus a `valkey` entry in the `.github/workflows/ci.yml` `test-db-conformance` matrix (existing `pytest-databases` `valkey_service` fixture, `ConformanceGate.SUPPORTED_ENGINES`) (`ch04:t02`). Update public docs, `native-stubs.md`, changelog, `AGENTS.md`, and `product.md` (`ch04:t03`), and author `valkey-limitations.md` (`<= 200` lines) + sync OKF chapters and review skills (`ch04:t04`).

```python
# src/py/vertebra/_valkey.pyi (excerpt)
class SyncValkeyClient:
    def hexpire(self, key: str, ttl_seconds: int, fields: Sequence[str], condition: str | None = None) -> list[int]: ...
    def hpexpire(self, key: str, ttl_ms: int, fields: Sequence[str], condition: str | None = None) -> list[int]: ...
    def httl(self, key: str, fields: Sequence[str]) -> list[int]: ...
    def hpttl(self, key: str, fields: Sequence[str]) -> list[int]: ...
    def hpersist(self, key: str, fields: Sequence[str]) -> list[int]: ...
```

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Missing `vertebra._valkey` | `SyncValkeyDriver()` / `ValkeyQueueBackend()` without wheel | Raises `MissingDependencyError` naming `vertebra-core[valkey]` | Fail-loud wheel guard |
| Scheme mismatch | `ValkeyClientConfig("redis://...")` or `"memory://"` | Raises `ImproperConfigurationError` | Strict Redis vs Valkey scheme boundary |
| Legacy RESP3 removal | `hasattr(vertebra._vertebra, "encode_resp_command")` | `False`; `src/rs/vertebra-db/src/resp3/mod.rs` deleted | Zero duplicate RESP3 code in core |

## 3. Documentation & OKF Knowledge Plan

Chapter 04 (`t03` and `t04`) updates `docs/usage/databases.md`, `docs/usage/queues-and-realtime.md`, `docs/reference/native-stubs.md` (`heading_level: 4`), `docs/changelog/index.md`, `AGENTS.md`, `.agents/bundles/product/product.md` (adding `vertebra-valkey` and removing `parse_resp3_frame`/`crc16_xmodem`/`redis_hash_slot` from `vertebra-db`), `.agents/bundles/knowledge/domains/data/valkey-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `review-pyo3-bridge/SKILL.md`, `domains/index.md`, and `workflow.md`.

## 4. Requirement-to-Chapter Traceability Matrix

| Chapter | Title | Requirements | Primary Outputs |
| --- | --- | --- | --- |
| `ch01` | Crate, Wheel, Stubs, Isolation Gates, and Review Manifests | `R1`–`R3` | `src/rs/vertebra-valkey/`, `src/py/vertebra/_valkey.pyi`, `Makefile`, `areas.toml` |
| `ch02` | Rust Clients, Valkey 9.0 Hash-Field TTL, Cluster, Sentinel, Tracking, Pub/Sub, and Streams | `R4`–`R11` | `SyncValkeyClient`, `AsyncValkeyClient`, `HEXPIRE`/`HPEXPIRE`, `CLUSTER SHARDS`, Streams |
| `ch03` | Python Valkey Cutover, Notifications, Legacy RESP3 Cleanup, and Queue Backend | `R12`–`R15` | `adapters/valkey/{config,core,driver,notify}.py`, `queues/valkey.py`, delete `vertebra_db::resp3` |
| `ch04` | Valkey 9.0 Conformance, CI Services, Public Docs, and OKF Synchronization | `R16`–`R19` | `test_driver_conformance.py`, `.github/workflows/ci.yml`, `valkey-limitations.md`, docs |

## 5. Implementation Plan

- [ ] [ch01-wheel-scaffold-and-isolation](chapters/ch01-wheel-scaffold-and-isolation/spec.md) — Scaffold `src/rs/vertebra-valkey`, `src/py/vertebra/_valkey.pyi`, `Makefile` targets, sibling `.pyi` wheel exclusions, and review manifests (`4` tasks).
- [ ] [ch02-rust-clients-topologies-and-streams](chapters/ch02-rust-clients-topologies-and-streams/spec.md) — Implement Rust `ValkeyClientConfig`, Google Cloud IAM, RESP3 + `CLIENT CAPA redirect`, Valkey 9.0 hash-field TTL, Cluster (`CLUSTER SHARDS`), Sentinel, tracking, Pub/Sub, and Streams (`4` tasks).
- [ ] [ch03-python-cutover-and-notifications](chapters/ch03-python-cutover-and-notifications/spec.md) — Cut Python `SyncValkeyDriver`, `AsyncValkeyDriver`, `ValkeyNotificationSource`, and `ValkeyQueueBackend` over to `vertebra._valkey` and delete `vertebra_db::resp3` (`4` tasks).
- [ ] [ch04-conformance-and-docs](chapters/ch04-conformance-and-docs/spec.md) — Add Valkey 9.0 conformance tests, add the `valkey` `test-db-conformance` matrix entry in CI, and synchronize public docs, `product.md`, and `valkey-limitations.md` (`4` tasks).

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` chapters (`0/16` tasks) completed; depends on `native-redis-driver`.
- **Active Invariants**: Zero imports between `redis` and `valkey` Python packages; `vertebra._valkey` passes `mypy.stubtest`; `src/rs/vertebra-db/src/resp3/mod.rs` is deleted in `ch03:t03`.
- **Fast Verification Gate (`< 60s`)**: `make valkey-test && make valkey-wheel-check && make review-check`

[^prd-spec]: `.agents/bundles/specs/native-data-platform/spec.md` — Native Data Platform & First-Party Driver Parity Master PRD.
[^redis-spec]: `.agents/bundles/knowledge/domains/data/redis-limitations.md` — Archived Native Redis Driver operational boundaries and wheel baseline.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig with Google Cloud Memorystore IAM and tracking fields.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore RESP3 command builder and cluster hash slot routing.
[^queue-valkey]: `src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend` — Standalone `ValkeyQueueBackend` extracted to `queues/valkey.py` by `native-redis-driver`.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame` — Legacy RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation.
[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 PyO3 helper functions on vertebra._vertebra.
[^gcp-token]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI.
[^root-pyproject]: `pyproject.toml` — Root pyproject.toml extras and wheel exclusions.
[^areas-toml]: `.agents/skills/review-core/references/areas.toml` — Review area capability manifest.
