---
type: Task
id: native-valkey-driver:ch02-rust-clients-topologies-and-streams:t02
flow_id: native-valkey-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Implement RESP3 Codec, Valkey 9.0 Hash-Field TTL, and Client Tracking L1 Cache
description: Implement RESP3 parser/encoder, HELLO 3 + CLIENT CAPA redirect handshake, Valkey 9.0 hash-field expiration (HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT/HTTL/HPTTL/HPERSIST), and CLIENT TRACKING L1 invalidation in src/rs/vertebra-valkey.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: public_seam_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch02-rust-clients-topologies-and-streams:t01
files:
  - src/rs/vertebra-valkey/src/codec.rs
  - src/rs/vertebra-valkey/src/tracking.rs
  - src/rs/vertebra-valkey/src/lib.rs
  - src/rs/vertebra-valkey/tests/valkey_client.rs
tests:
  - src/rs/vertebra-valkey/tests/valkey_client.rs
read_set:
  - src/rs/vertebra-db/src/resp3/mod.rs
  - src/py/vertebra/db/adapters/valkey/core.py
estimated_loc: 230
tags:
  - rust
  - valkey
  - resp3
  - tracking
  - ttl
sources:
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame
    title: RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore RESP3 command builder
verification_strategy: public_seam_tdd
---

# Task `t02`: Implement RESP3 Codec, Valkey 9.0 Hash-Field TTL, and Client Tracking L1 Cache

## Objective & Scope Boundary

Implement the RESP3 frame codec, `HELLO 3` (`PROTOCOL 2` fallback) + `CLIENT CAPA redirect` handshake, Valkey 9.0 hash-field expiration builders (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`), and `CLIENT TRACKING ON ... BCAST` L1 cache invalidation in `src/rs/vertebra-valkey/src/{codec,tracking,lib}.rs` per [`../spec.md` §2](../spec.md) [^resp3-rs] [^valkey-core].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/resp3/mod.rs` — Reference RESP3 frame parser and CRC16-XMODEM implementation.
- `src/py/vertebra/db/adapters/valkey/core.py` — `ValkeyCore` command builders and tracking logic.

## Contracts & Edge Cases

- Encode Valkey 9.0 hash-field expiration commands (`HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field ...`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL key FIELDS numfields field ...`, `HPTTL`, `HPERSIST`) and return `list[int]` per-field status/TTL codes.
- Negotiate `HELLO 3` + optional `CLIENT CAPA redirect` on connect, falling back cleanly if an older server reports unknown subcommand.
- Evict L1 tracked keys immediately upon receiving RESP3 `>2\r\n$10\r\ninvalidate\r\n...` push frames (`invalidate_key(None)` clears the L1 table on flush).

## Test Matrix

- Rust unit and loopback tests in `valkey_client.rs` verify RESP3 frame roundtrips, `HEXPIRE`/`HPEXPIRE`/`HTTL`/`HPTTL`/`HPERSIST` wire encoding and status decoding, `CLIENT CAPA redirect` negotiation, and L1 cache invalidation on push frames.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey --test valkey_client
```

## Acceptance Criteria

- [ ] RESP3 codec, `CLIENT CAPA redirect`, Valkey 9.0 hash-field TTL commands, and L1 tracking pass all tests in `valkey_client.rs`.
- [ ] Zero regressions in the fast verification gate.

[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::parse_resp3_frame` — RESP3 parser, encoder, and CRC16-XMODEM hash slot implementation.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore RESP3 command builder.
