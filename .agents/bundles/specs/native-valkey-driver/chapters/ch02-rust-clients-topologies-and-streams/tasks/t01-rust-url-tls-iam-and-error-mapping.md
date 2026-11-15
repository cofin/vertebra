---
type: Task
id: native-valkey-driver:ch02-rust-clients-topologies-and-streams:t01
flow_id: native-valkey-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Implement Rust Valkey URL/TLS Config, Google Cloud Memorystore IAM, and Error Mapping
description: Implement ValkeyClientConfig (valkey://, valkeys://, unix://), Rustls TLS/mTLS, Google Cloud Memorystore for Valkey IAM token refresh, and DatabaseError mapping in src/rs/vertebra-valkey.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: public_seam_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch01-wheel-scaffold-and-isolation:t04
files:
  - src/rs/vertebra-valkey/src/config.rs
  - src/rs/vertebra-valkey/src/error.rs
  - src/rs/vertebra-valkey/src/lib.rs
  - src/rs/vertebra-valkey/tests/valkey_client.rs
tests:
  - src/rs/vertebra-valkey/tests/valkey_client.rs
read_set:
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/py/vertebra/_vertebra.pyi
estimated_loc: 220
tags:
  - rust
  - valkey
  - tls
  - gcp
  - errors
sources:
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig with Google Cloud Memorystore IAM and tracking fields
  - id: gcp-token
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI
  - id: db-exc
    resource: src/py/vertebra/_vertebra.pyi::DatabaseError
    title: DatabaseError hierarchy declared by vertebra._vertebra
verification_strategy: public_seam_tdd
---

# Task `t01`: Implement Rust Valkey URL/TLS Config, Google Cloud Memorystore IAM, and Error Mapping

## Objective & Scope Boundary

Implement `ValkeyClientConfig` (`valkey://`, `valkeys://`, `unix://`), Rustls TLS + mTLS, Google Cloud Memorystore for Valkey IAM via `GcpTokenCacheCapsule`, and `vertebra._vertebra` `DatabaseError` mapping in `src/rs/vertebra-valkey/src/{config,error,lib}.rs` per [`../spec.md` §2](../spec.md) [^valkey-cfg] [^gcp-token] [^db-exc].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/valkey/config.py` — `ValkeyConfig` fields and URL parsing rules.
- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCache` and `GcpTokenCacheCapsule`.
- `src/py/vertebra/_vertebra.pyi` — `DatabaseError`, `InterfaceError`, `DataError`, `OperationalError`, `IntegrityError`, `ProgrammingError`, `NotSupportedError` (`ImproperConfigurationError` lives in `src/py/vertebra/_typing.py`).

## Contracts & Edge Cases

- Accept `valkey://`, `valkeys://`, and `unix://`; reject `memory://`, `redis://`, `rediss://`, and hostless URLs with `ImproperConfigurationError`.
- Refresh Google Cloud Memorystore for Valkey IAM tokens on connect and reconnect via `GcpTokenCacheCapsule` or the existing `iam_token_provider` / `token_provider` when `enable_gcp_memorystore_iam` / `iam_auth_enabled` is set.
- Map `WRONGPASS`/`NOAUTH` to `OperationalError` (`is_transient=False`; no new exception classes), `READONLY`/`MASTERDOWN`/`CLUSTERDOWN`/`LOADING`/`TRYAGAIN` to `OperationalError`, and `WRONGTYPE`/`BUSYKEY`/`CROSSSLOT`/`NOSCRIPT`/`ERR syntax` to `ProgrammingError`.

## Test Matrix

- Rust integration tests in `src/rs/vertebra-valkey/tests/valkey_client.rs` cover URL scheme validation (`valkey://`, `valkeys://`, `unix://`, rejection of `memory://` and `redis://`), TLS config construction, IAM token injection, and error classification.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey --test valkey_client
```

## Acceptance Criteria

- [ ] `ValkeyClientConfig` validates schemes, TLS, and Google Cloud IAM settings cleanly.
- [ ] All error prefixes map to the canonical `vertebra._vertebra` `DatabaseError` subclasses.
- [ ] Zero regressions in the fast verification gate.

[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig with Google Cloud Memorystore IAM and tracking fields.
[^gcp-token]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Shared Rust Google Cloud OAuth2 token cache and PyCapsule ABI.
[^db-exc]: `src/py/vertebra/_vertebra.pyi::DatabaseError` — DatabaseError hierarchy declared by vertebra._vertebra.
