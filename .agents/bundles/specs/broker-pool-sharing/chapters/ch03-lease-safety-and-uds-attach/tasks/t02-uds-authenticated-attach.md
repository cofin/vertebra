---
type: Task
id: broker-pool-sharing:ch03-lease-safety-and-uds-attach:t02
flow_id: broker-pool-sharing
chapter_id: ch03-lease-safety-and-uds-attach
title: Implement Peer-Credential Authenticated UDS Broker Handshake and FD Passing
description: Build Unix domain socket SO_PEERCRED/LOCAL_PEERCRED handshake and SCM_RIGHTS SHM file-descriptor passing.
status: stable
state: completed
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- broker-pool-sharing:ch03-lease-safety-and-uds-attach:t01
files:
- src/rs/vertebra-core/src/broker_attach.rs
- src/rs/vertebra-core/src/lib.rs
tests:
- src/rs/vertebra-core/tests/it/shm.rs
read_set:
- src/rs/vertebra-core/src/shm.rs
- src/rs/vertebra-core/src/lib.rs
- src/rs/vertebra-core/tests/it/shm.rs
estimated_loc: 225
tags:
- uds
- auth
- shm
sources:
- id: core-shm
  resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
  title: Named POSIX shared-memory regions
- id: shm-it
  resource: src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named
  title: Existing SHM integration test suite
---

# Task `t02`: Implement Peer-Credential Authenticated UDS Broker Handshake and FD Passing

## Objective & Scope Boundary

Implement `broker_attach::{BrokerAttachServer, BrokerAttachClient, AttachError}` in `vertebra-core` per [`../spec.md` §2](../spec.md) ([#97](https://github.com/cofin/vertebra/issues/97)) [^core-shm] [^shm-it]. Out of scope: PyO3 wrapper (`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/shm.rs` — `ShmRegion` raw file descriptor access.
- `src/rs/vertebra-core/src/lib.rs` — Crate exports.
- `src/rs/vertebra-core/tests/it/shm.rs` — SHM and FD integration tests.

## Contracts & Edge Cases

- Verify parent directory mode `0700` and owner UID == `geteuid()`, bind socket `0600`, verify peer credentials (`SO_PEERCRED` / `LOCAL_PEERCRED`), compare 32-byte secret in constant time, and pass the SHM fd via `SCM_RIGHTS`.
- Client verifies server socket UID and `0600` permissions before transmitting the secret.
- **Edge Case**: Group/world-accessible parent dir (`0755`) or wrong secret fails immediately without sending the SHM fd.
- **Edge Case**: Handshake read/write deadlines prevent stalled clients from blocking the accept loop.

## Test Matrix

- Valid secret + `0700` dir passes SHM fd via `SCM_RIGHTS` and maps identical `ShmLeaseTable`.
- Wrong secret, `0755` dir, and truncated frame are each rejected with typed `AttachError` variants.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it shm
```

## Acceptance Criteria

- [x] Unauthenticated or bad-permission UDS attach attempts never receive the SHM fd.
- [x] Secret bytes are redacted in `Debug` and error messages.
- [x] Zero regressions in the fast verification gate.

[^core-shm]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Named POSIX shared-memory regions.
[^shm-it]: `src/rs/vertebra-core/tests/it/shm.rs::stale_region_replaced_on_create_named` — Existing SHM integration test suite.
