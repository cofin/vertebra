---
type: Task
id: "broker-pool-sharing:ch02:t03"
flow_id: broker-pool-sharing
chapter_id: ch02-broker-ipc-identity-and-wire
title: "Versioned dbrpc Binary Header and msgspec Wire Frames"
description: "Add vertebra_core::dbrpc header codecs and matching array-like DbRpcRequest/DbRpcResponse structs."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch02:t02"
files:
  - src/rs/vertebra-core/src/dbrpc/mod.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/py/vertebra/extensions/broker/dbrpc.py
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/tests/unit/extensions/test_broker_transport.py
read_set:
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/py/vertebra/utils/serializers.py
estimated_loc: 215
tags:
  - dbrpc
  - ipc
  - msgspec
sources:
  - id: ring-pair
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: "Bidirectional SHM ring pair transport"
  - id: core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "Shared-memory lease table, statement cache, dialect tags, and worker reclamation"
---

# Task `t03`: Versioned dbrpc Binary Header and msgspec Wire Frames

## Objective & Scope Boundary

Implement `vertebra_core::dbrpc` (`DBRPC_VERSION = 1`, `DbRpcOp`, `DbRpcHeader`) and `vertebra.extensions.broker.dbrpc` (`DbRpcRequest`, `DbRpcResponse`) per [`../spec.md` §2](../spec.md) [^ring-pair] [^core-broker]. Out of scope: engine pool execution (`ch03`–`ch04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/transport/ring_pair.rs` — SHM ring frame payload conventions.
- `src/rs/vertebra-core/src/lib.rs` — Crate module exports.
- `src/py/vertebra/utils/serializers.py` — Centralized msgspec serialization helpers.

## Contracts & Edge Cases

- Encode/decode fixed little-endian `DbRpcHeader` (`version`, `op`, `request_id`, `lease_slot`, `lease_generation`) with checked bounds.
- Declare `DbRpcRequest` and `DbRpcResponse` as `msgspec.Struct(..., frozen=True, gc=False, array_like=True)` using immutable tuple fields so `gc=False` cannot leak cycles.
- **Edge Case**: Truncated buffers or unknown `DbRpcOp` discriminants return `DbRpcError::{Truncated, InvalidOp}`.
- **Edge Case**: `version != DBRPC_VERSION` returns `DbRpcError::UnsupportedVersion(version)`.

## Test Matrix

- Round-trip every `DbRpcOp` (`Execute`..`Close`) across Rust header codec and Python `DbRpcRequest`/`DbRpcResponse`.
- Parameterized malformed/truncated/version-mismatch byte slices fail cleanly without panicking.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py -q
```

## Acceptance Criteria

- [ ] Rust `dbrpc` and Python `dbrpc.py` agree byte-for-byte on header layout and op codes.
- [ ] `DbRpcRequest` and `DbRpcResponse` obey `PY-04` acyclic `gc=False, array_like=True` rules.
- [ ] Zero regressions in the chapter fast verification gate.

[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Bidirectional SHM ring pair transport.
[^core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Shared-memory lease table, statement cache, dialect tags, and worker reclamation.
