---
type: Spec
flow_id: native-redis-driver
chapter_id: ch02-wheel-isolation-and-transport-config
title: "Chapter 02: Sibling Wheel Isolation, Redis/Valkey URL Normalization, TLS, Memorystore IAM, and Error Mapping"
description: "Exclude _redis.pyi across sibling wheels, update test_optional_driver_wheels.py, and implement Rust URL/TLS normalization, GcpTokenCacheCapsule IAM refresh, and DatabaseError mapping."
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
  - tls
  - gcp
  - wheel
sources:
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "Maturin standalone wheel isolation precedent"
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: "Sibling PyO3 wheel isolation test gate"
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: "C-ABI GCP IAM token cache capsule"
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: "RedisConfig topology, tracking, and Memorystore IAM fields"
---

# Chapter 02: Sibling Wheel Isolation, Redis/Valkey URL Normalization, TLS, Memorystore IAM, and Error Mapping

<!-- truth: start -->
- All sibling maturin wheels exclude `"**/py/vertebra/_redis.pyi"` (and reciprocal `_sqlite.pyi` if landed), verified by `test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests].
- `config.rs` normalizes `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` schemes for `redis 1.8.1` while rejecting `memory://` with `ImproperConfigurationError` [^redis-cfg].
- GCP Memorystore for Redis/Valkey IAM authentication refreshes tokens dynamically through `GcpTokenCacheCapsule` [^gcp-capsule] [^redis-cfg].
- `errors.rs` maps `redis::RedisError` (`NOAUTH`, `WRONGPASS`, `NOPERM`, `READONLY`, `BUSY`, `LOADING`, `CLUSTERDOWN`, `CROSSSLOT`, `NOSCRIPT`, `WRONGTYPE`, I/O timeouts) to `vertebra._vertebra.DatabaseError` subclasses with `is_transient`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Both Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` speak RESP3 over TCP, `rustls` TLS (`rediss://` / `valkeys://`), and Unix domain sockets (`unix://`), while GCP Memorystore uses short-lived OAuth2 IAM access tokens [^gcp-capsule] [^redis-cfg]. This chapter enforces wheel isolation and builds the URL, TLS, IAM, and error foundations of `vertebra-redis`.

**Non-Goals:** Command execution, client tracking, Cluster/Sentinel, Streams, and Pub/Sub (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel payloads |
| Config, IAM & errors | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` [^gcp-capsule] [^redis-cfg] | `valkey://`/`valkeys://`/`unix://` + `GcpTokenCacheCapsule` + `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5a**: Exclude `_redis.pyi` across sibling PyO3 `pyproject.toml` files and assert `[redis]`/`[valkey]` wheel isolation in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `RedisClientConfig` (`redis://`, `rediss://`, `valkey://`, `valkeys://`, `unix://`, `memory://` rejection, `GcpTokenCacheCapsule` IAM hook) and `RedisError` -> `DatabaseError` mapping (`t04`).

```rust
pub enum RedisTopology {
    Standalone,
    Cluster { nodes: Vec<(String, u16)>, max_redirections: usize },
    Sentinel { master_name: String, nodes: Vec<(String, u16)> },
}
// URL scheme normalization:
// valkey://host:port/db  -> redis://host:port/db
// valkeys://host:port/db -> rediss://host:port/db (rustls + ring)
// unix:///path/to.sock   -> redis+unix:///path/to.sock
// memory://...           -> ImproperConfigurationError
```

**Integration Flow:**
- `make redis-wheel-check` verifies `vertebra-redis` wheel isolation.
- `client_core.rs` (`ch03`) consumes `RedisClientConfig`, `GcpTokenCacheCapsule`, and `map_redis_error`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `memory://` URL | `SyncRedisClient("memory://")` | Raises `ImproperConfigurationError` immediately | Fail-loud baseline preserved |
| `CROSSSLOT` / `WRONGTYPE` | Server error prefix `CROSSSLOT` or `WRONGTYPE` | `ProgrammingError` (`is_transient=False`) | Typed command error |
| `BUSY` / `CLUSTERDOWN` / I/O | Transient network or failover error | `OperationalError` (`is_transient=True`) | Retryable classification |

## 3. Documentation & OKF Knowledge Plan

URL schemes and Memorystore IAM are documented in `ch07:t03` and `ch08:t02`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5a` | `t01`–`t03` | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` | `make redis-wheel-check && pytest test_optional_driver_wheels.py` |
| `R5b` | `t04` | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` | `cargo test -p vertebra-redis` |

## 5. Implementation Plan

- [ ] [t01-sibling-exclusions-part1.md](tasks/t01-sibling-exclusions-part1.md) — Add **/py/vertebra/_redis.pyi to [tool.maturin].exclude in bigquery, bundler, and duckdb pyproject.toml files.
- [ ] [t02-sibling-exclusions-part2.md](tasks/t02-sibling-exclusions-part2.md) — Add **/py/vertebra/_redis.pyi to [tool.maturin].exclude in mssql, mysql, and oracle pyproject.toml files.
- [ ] [t03-sibling-exclusions-part3-and-wheel-gate.md](tasks/t03-sibling-exclusions-part3-and-wheel-gate.md) — Exclude _redis.pyi in postgres, spanner, and storage and enforce vertebra-redis + [redis]/[valkey] in test_optional_driver_wheels.py.
- [ ] [t04-rust-url-tls-iam-and-error-mapping.md](tasks/t04-rust-url-tls-iam-and-error-mapping.md) — Implement config.rs and errors.rs supporting redis/rediss/valkey/valkeys/unix URLs, memory:// rejection, GcpTokenCacheCapsule, and DatabaseError mapping.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels include `_redis.pyi`; `memory://` is rejected without opening sockets.
- **Fast Chapter Gate (`< 60s`)**: `make redis-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis`

[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI GCP IAM token cache capsule.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
