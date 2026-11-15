---
type: Spec
flow_id: native-valkey-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: 'Chapter 02: Rust Clients, Valkey 9.0 Hash-Field TTL, Cluster, Sentinel, Tracking, Pub/Sub, and Streams'
description: Implement ValkeyClientConfig, Google Cloud Memorystore for Valkey IAM, RESP3 + CLIENT CAPA redirect, Valkey 9.0 hash-field expiration (HEXPIRE/HPEXPIRE/HTTL/HPTTL/HPERSIST), CLIENT TRACKING L1 cache, Cluster (CLUSTER SHARDS), Sentinel, Pub/Sub, and Streams in src/rs/vertebra-valkey.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-wheel-scaffold-and-isolation
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
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig with Google Cloud Memorystore IAM and tracking fields
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame
    title: RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation
  - id: gcp-token
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI
  - id: completion-ring
    resource: src/rs/vertebra-db/src/py_async.rs::DbCompletionDrainer
    title: DbCompletionDrainer and spawn_rust_db_task_with_attr_and_cancel async bridge
current_task: "native-valkey-driver:ch02-rust-clients-topologies-and-streams:t01"
---

# Chapter 02: Rust Clients, Valkey 9.0 Hash-Field TTL, Cluster, Sentinel, Tracking, Pub/Sub, and Streams

<!-- truth: start -->
- `ValkeyClientConfig` parses `valkey://`, `valkeys://` (Rustls TLS + mTLS), and `unix://` (rejecting `memory://` and `redis://`/`rediss://` with `ImproperConfigurationError`), integrates Google Cloud Memorystore for Valkey IAM via `GcpTokenCacheCapsule`, and maps server errors to the `vertebra._vertebra` `DatabaseError` hierarchy [^valkey-cfg] [^gcp-token].
- `src/rs/vertebra-valkey` implements zero-copy RESP3 parsing, `HELLO 3` + `CLIENT CAPA redirect`, Valkey 9.0 hash-field expiration (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`), and `CLIENT TRACKING ON ... BCAST` L1 cache invalidation [^resp3-rs].
- Cluster routing discovers slots via `CLUSTER SHARDS` (falling back to `CLUSTER SLOTS`), routes replica reads via `READONLY`, handles `MOVED`/`ASK` redirects, and discovers primaries via Sentinel (`SENTINEL get-master-addr-by-name` + `+switch-master`) [^resp3-rs].
- `SyncValkeyClient` (`py.detach`) and `AsyncValkeyClient` (`_vertebra_valkey_completion_drainer` `DbCompletionDrainer`), `SyncValkeyPipeline`/`AsyncValkeyPipeline`, `SyncValkeyListener`/`AsyncValkeyListener`, and Streams (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`) expose full Valkey capabilities to Python [^completion-ring].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Valkey 8.0+ and 9.0 introduce protocol capabilities beyond baseline RESP3, including native hash-field expiration (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`), `CLIENT CAPA redirect`, and `CLUSTER SHARDS` replica routing [^valkey-cfg]. This chapter implements the complete Rust engine inside `src/rs/vertebra-valkey`, reusing `vertebra_core::gcp_auth::GcpTokenCache` and `DbCompletionDrainer` [^gcp-token] [^completion-ring].

**Non-Goals:** Python `adapters/valkey/` and `queues/valkey.py` cutover (Chapter 03).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| URL, TLS, IAM & Errors | `src/rs/vertebra-valkey/src/{config,error}.rs` [^valkey-cfg] [^gcp-token] | `ValkeyClientConfig`, `GcpTokenCacheCapsule`, `DatabaseError` mapping |
| Codec, Valkey 9.0 & Tracking | `src/rs/vertebra-valkey/src/{codec,tracking}.rs` [^resp3-rs] | `HELLO 3`, `CLIENT CAPA redirect`, `HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`, L1 tracking |
| Cluster & Sentinel | `src/rs/vertebra-valkey/src/{cluster,sentinel}.rs` [^resp3-rs] | `CLUSTER SHARDS` + `CLUSTER SLOTS`, `MOVED`/`ASK`, `READONLY` replica reads, Sentinel |
| Clients, Pub/Sub & Streams | `src/rs/vertebra-valkey/src/{client,pubsub,streams,lib}.rs` [^completion-ring] | `SyncValkeyClient`, `AsyncValkeyClient`, `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `SyncValkeyListener`, `AsyncValkeyListener` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PyO3 0.30.1 FFI & GIL Discipline](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [PyO3 FFI & Vectorized Batch Dispatch](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R4–R5 (Config, TLS, Google Cloud Memorystore for Valkey IAM & Errors)**: Implement `ValkeyClientConfig` (`valkey://`, `valkeys://`, `unix://`; rejects `memory://` and `redis://`/`rediss://`), Rustls TLS + mTLS, `GcpTokenCacheCapsule` IAM refresh, and server error mapping (`WRONGPASS`/`NOAUTH` -> `OperationalError` with `is_transient=False`, `READONLY`/`MASTERDOWN`/`CLUSTERDOWN` -> `OperationalError`, `BUSYKEY`/`CROSSSLOT` -> `ProgrammingError`) (`t01`).
- **R6–R7 (RESP3 Codec, Valkey 9.0 Hash-Field Expiration & L1 Tracking)**: Implement RESP3 encoder/decoder, `HELLO 3` (`PROTOCOL 2` fallback) + `CLIENT CAPA redirect`, Valkey 9.0 hash-field TTL commands (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST` with optional `NX`/`XX`/`GT`/`LT` conditions), and `CLIENT TRACKING ON ... BCAST` L1 invalidation (`t02`).
- **R8–R9 (Cluster `CLUSTER SHARDS` & Sentinel Topology)**: Implement `16384`-slot table populated from `CLUSTER SHARDS` (with `CLUSTER SLOTS` fallback), `{hash_tag}` CRC16-XMODEM routing, `MOVED`/`ASK` redirect handling (`<= max_redirections`), `READONLY` replica read routing, and Sentinel primary discovery (`t03`).
- **R10–R11 (PyO3 Sync/Async Clients, Pipelines, Pub/Sub & Streams)**: Expose `SyncValkeyClient` (`py.detach`), `AsyncValkeyClient` (`_vertebra_valkey_completion_drainer`), `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `SyncValkeyListener`, `AsyncValkeyListener` (`SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`), and Streams (`XGROUP CREATE`, `XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`) (`t04`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid URL scheme | `ValkeyClientConfig("redis://...")` or `"memory://"` | Raises `ImproperConfigurationError` | Strict Valkey scheme boundary |
| Hash-field TTL condition | `hexpire(key, 60, ["f1"], condition="INVALID")` | Raises `ProgrammingError` before socket write | Input validation |
| Cluster redirect loop | `MOVED` count `> max_redirections` | Raises `OperationalError` | Bounded redirect retries |

## 3. Documentation & OKF Knowledge Plan

Public stub reference (`docs/reference/native-stubs.md`) and OKF chapters (`valkey-limitations.md`, `database-drivers-and-dialers.md`) are updated in Chapter 04.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust Valkey URL/TLS Config, Google Cloud Memorystore IAM, and Error Mapping | `src/rs/vertebra-valkey/src/{config,error,lib}.rs` | `cargo test -p vertebra-valkey --test valkey_client` |
| `t02` | Implement RESP3 Codec, Valkey 9.0 Hash-Field TTL, and Client Tracking L1 Cache | `src/rs/vertebra-valkey/src/{codec,tracking,lib}.rs` | `cargo test -p vertebra-valkey --test valkey_client` |
| `t03` | Implement Valkey Cluster (CLUSTER SHARDS) and Sentinel Discovery | `src/rs/vertebra-valkey/src/{cluster,sentinel,lib}.rs` | `cargo test -p vertebra-valkey --test valkey_cluster_sentinel` |
| `t04` | Expose Sync/Async Valkey Clients, Pipelines, Pub/Sub Listeners, and Streams | `src/rs/vertebra-valkey/src/{client,pubsub,streams,lib}.rs`, `src/py/vertebra/_valkey.pyi` | `cargo test -p vertebra-valkey && mypy.stubtest vertebra._valkey` |

## 5. Implementation Plan

- [ ] [t01-rust-url-tls-iam-and-error-mapping.md](tasks/t01-rust-url-tls-iam-and-error-mapping.md) — Implement `ValkeyClientConfig`, Rustls TLS/mTLS, Google Cloud Memorystore for Valkey IAM (`GcpTokenCacheCapsule`), and `DatabaseError` mapping.
- [ ] [t02-resp3-codec-hash-ttl-and-tracking.md](tasks/t02-resp3-codec-hash-ttl-and-tracking.md) — Implement RESP3 frame codec, `HELLO 3` + `CLIENT CAPA redirect`, Valkey 9.0 hash-field expiration (`HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST`), and `CLIENT TRACKING` L1 invalidation.
- [ ] [t03-cluster-shards-and-sentinel-routing.md](tasks/t03-cluster-shards-and-sentinel-routing.md) — Implement `CLUSTER SHARDS` / `CLUSTER SLOTS` routing, `MOVED`/`ASK` redirects, `READONLY` replica reads, and Sentinel primary failover.
- [ ] [t04-pyo3-clients-pipelines-pubsub-and-streams.md](tasks/t04-pyo3-clients-pipelines-pubsub-and-streams.md) — Expose `SyncValkeyClient`, `AsyncValkeyClient` (`_vertebra_valkey_completion_drainer`), `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `SyncValkeyListener`, `AsyncValkeyListener`, and Streams.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero GIL retention across blocking I/O (`py.detach`); `_vertebra_valkey_completion_drainer` registered once per event loop; `mypy.stubtest vertebra._valkey` green.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey && make dev && uv run --no-sync python -m mypy.stubtest vertebra._valkey --concise`

[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig with Google Cloud Memorystore IAM and tracking fields.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame` — RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation.
[^gcp-token]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI.
[^completion-ring]: `src/rs/vertebra-db/src/py_async.rs::DbCompletionDrainer` — DbCompletionDrainer and spawn_rust_db_task_with_attr_and_cancel async bridge.
