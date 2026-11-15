---
type: Task
id: native-redis-driver:ch02-rust-clients-topologies-and-streams:t01
flow_id: native-redis-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Implement Rust URL/DSN Normalization, rustls mTLS, Google Cloud IAM Auth, and Error Mapping
description: Build Redis URL normalization, rustls TLS/mTLS configuration, Google Cloud Memorystore IAM token provider, and structured error mapping in vertebra-redis.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch01-wheel-scaffold-and-isolation:t04
files:
  - src/rs/vertebra-redis/src/config.rs
  - src/rs/vertebra-redis/src/errors.rs
  - src/rs/vertebra-redis/src/lib.rs
tests:
  - src/rs/vertebra-redis/tests/redis_client.rs
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/py/vertebra/db/adapters/redis/config.py
  - src/rs/vertebra-py/src/db.rs
estimated_loc: 235
tags:
  - redis
  - tls
  - gcp
  - errors
sources:
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: C-ABI Google Cloud IAM token cache capsule
  - id: redis-cfg
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: RedisConfig topology, tracking, and Memorystore IAM fields
verification_strategy: regression_tdd
---

# Task `t01`: Implement Rust URL/DSN Normalization, rustls mTLS, Google Cloud IAM Auth, and Error Mapping

## Objective & Scope Boundary

Implement `config.rs` and `errors.rs` in `vertebra-redis` per [`../spec.md` §2](../spec.md) [^gcp-capsule] [^redis-cfg].

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCacheCapsule` extraction and token fetch.
- `src/py/vertebra/db/adapters/redis/config.py` — `RedisConfig` URL, topology, and IAM fields.
- `src/rs/vertebra-py/src/db.rs` — `DatabaseError` / `ImproperConfigurationError` mapping.

## Contracts & Edge Cases

- Normalize `redis://`, `rediss://`, and `unix://` -> `redis+unix://` with RESP3 (`protocol = 3`) default; reject `memory://`, `valkey://`, `valkeys://` (directing Valkey schemes to `vertebra._valkey`), and unknown schemes immediately with `ImproperConfigurationError`.
- Extract `SharedGcpTokenCacheHandle` from `GcpTokenCacheCapsule` for Memorystore for Redis IAM `AUTH` refresh and map `RedisError` kinds/codes to `DatabaseError` subclasses (`is_transient`).
- **Edge Case**: `unix://` combined with `topology = "cluster"` or `"sentinel"` raises `ImproperConfigurationError`.

## Test Matrix

- Rust tests cover `redis://`, `rediss://`, and `unix://` URL schemes, `memory://` + `valkey://` + invalid topology rejection, IAM token refresh hook, and `RedisError` -> `OperationalError`/`ProgrammingError` mapping.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis
```

## Acceptance Criteria

- [ ] URL/TLS/IAM normalization and error mapping pass all Rust unit/integration tests.
- [ ] Zero regressions in the fast verification gate.

[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI Google Cloud IAM token cache capsule.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
