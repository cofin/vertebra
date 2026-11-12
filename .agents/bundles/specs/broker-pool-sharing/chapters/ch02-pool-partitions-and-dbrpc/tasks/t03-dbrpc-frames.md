---
type: Task
id: "broker-pool-sharing:ch02:t03"
chapter_id: ch02-pool-partitions-and-dbrpc
title: Versioned dbrpc frames
description: Add vertebra-core/src/dbrpc/mod.rs with a versioned header and op codes and the matching msgspec DbRpcRequest/DbRpcResponse structs in vertebra.extensions.broker.dbrpc, with round-trip tests.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch02:t02"
read_set:
  - src/rs/vertebra-core/src/transport/ring_pair.rs
  - src/rs/vertebra-core/src/lib.rs
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/rs/vertebra-core/tests/it/broker.rs
files:
  - src/rs/vertebra-core/src/dbrpc/mod.rs
  - src/py/vertebra/extensions/broker/dbrpc.py
  - src/rs/vertebra-core/tests/it/broker.rs
tests:
  - src/rs/vertebra-core/tests/it/broker.rs
  - src/py/tests/unit/extensions/test_broker_transport.py
estimated_loc: 230
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: ring-pair
    resource: src/rs/vertebra-core/src/transport/ring_pair.rs
    title: Ring pair transport carrying frames
  - id: rpc-request
    resource: src/py/vertebra/extensions/sqlspec/provider.py::BrokerRpcRequest
    title: Existing ad-hoc broker RPC request struct
tags:
  - dbrpc
  - protocol
---

# Task t03: Versioned dbrpc Frames

## Objective & Scope Boundary

1. Create `src/rs/vertebra-core/src/dbrpc/mod.rs` (declare `pub mod dbrpc;` in `src/rs/vertebra-core/src/lib.rs`, one line) with `DBRPC_VERSION`, `DbRpcOp`, `DbRpcHeader` (fixed 24-byte little-endian layout: `version u16`, `op u8`, `flags u8`, `lease_slot u32`, `request_id u64`, `lease_generation u64`), `encode_header`, `decode_header`, `DbRpcStatus { Ok = 0, Error = 1, Version = 2, Quarantined = 3, Cancelled = 4 }`, and `DbRpcError`.
2. Create `src/py/vertebra/extensions/broker/dbrpc.py` with `DbRpcOp(IntEnum)`, `DbRpcStatus(IntEnum)`, `DbRpcRequest`, `DbRpcResponse` (chapter §2.2) and a `DbRpcCodec` class exposing `encode_request`, `decode_request`, `encode_response`, `decode_response` through the centralized msgspec encoder/decoder instances held as class attributes.
3. `provider.py::BrokerRpcRequest` / `BrokerRpcResponse` [^rpc-request] are replaced by these frames in `ch06`; this task only adds the protocol.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/transport/ring_pair.rs` [^ring-pair].
- `src/rs/vertebra-core/src/lib.rs` — module list.
- `src/py/vertebra/extensions/sqlspec/provider.py` — `BrokerRpcRequest`, `BrokerRpcResponse` field inventory.
- `src/rs/vertebra-core/tests/it/broker.rs`.

## Symbol & Signature Contracts

- `pub const DBRPC_HEADER_LEN: usize = 24;`
- `pub fn decode_header(buf: &[u8]) -> Result<(DbRpcHeader, &[u8]), DbRpcError>`; `DbRpcError::{Truncated, Version { got: u16 }, UnknownOp { op: u8 }}`.
- Python `DbRpcCodec.decode_request(buf: bytes | memoryview) -> DbRpcRequest` raises `msgspec.ValidationError` on malformed frames.

## Edge Cases & Error Semantics

- Buffer shorter than 24 bytes: `Truncated`.
- `version != 1`: `Version { got }`; the host replies `DbRpcStatus.Version`.
- Op `0` or `> 9`: `UnknownOp`.
- `execute_many` with `params` not a list of lists: Python `ValidationError`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-core/tests/it/broker.rs`, `src/py/tests/unit/extensions/test_broker_transport.py`. Dependency category: in-process.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Rust round trip | Header `{version:1, op:Execute, request_id:7, lease_slot:3, lease_generation:9}` + payload `b"x"` | Decodes to same header and payload `b"x"` |
| Truncated | 10-byte buffer | `DbRpcError::Truncated` |
| Bad version | version `2` | `DbRpcError::Version { got: 2 }` |
| Unknown op | op `42` | `DbRpcError::UnknownOp { op: 42 }` |
| Python round trip | parametrize over every `DbRpcOp` member with `sql="SELECT 1"` | `decode_request(encode_request(r)) == r` |
| Response error | `DbRpcResponse(request_id=1, status=DbRpcStatus.Quarantined, error_class="LeaseQuarantinedError")` | Round trips equal |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp --test it broker::dbrpc`.
- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py -q -k dbrpc`.

## Acceptance Criteria

- [ ] All nine operations have versioned frames in Rust and Python (R6).

## Notes & Discoveries

- [2026-11-12 01:40] `src/rs/vertebra-core/src/dbrpc/` does not exist at HEAD; `transport/` holds `mod.rs` and `ring_pair.rs`.

[^ring-pair]: `src/rs/vertebra-core/src/transport/ring_pair.rs` — Ring pair transport.
[^rpc-request]: `src/py/vertebra/extensions/sqlspec/provider.py::BrokerRpcRequest` — Existing RPC request struct.
