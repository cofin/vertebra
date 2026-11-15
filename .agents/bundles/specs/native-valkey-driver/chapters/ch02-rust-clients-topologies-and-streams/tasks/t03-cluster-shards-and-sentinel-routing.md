---
type: Task
id: native-valkey-driver:ch02-rust-clients-topologies-and-streams:t03
flow_id: native-valkey-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Implement Valkey Cluster (CLUSTER SHARDS) and Sentinel Discovery
description: Implement 16384-slot Valkey Cluster routing via CLUSTER SHARDS (with CLUSTER SLOTS fallback), MOVED/ASK redirects, READONLY replica read routing, and Sentinel primary discovery in src/rs/vertebra-valkey.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: public_seam_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch02-rust-clients-topologies-and-streams:t02
files:
  - src/rs/vertebra-valkey/src/cluster.rs
  - src/rs/vertebra-valkey/src/sentinel.rs
  - src/rs/vertebra-valkey/src/lib.rs
  - src/rs/vertebra-valkey/tests/valkey_cluster_sentinel.rs
tests:
  - src/rs/vertebra-valkey/tests/valkey_cluster_sentinel.rs
read_set:
  - src/rs/vertebra-db/src/resp3/mod.rs
  - src/py/vertebra/db/adapters/valkey/core.py
estimated_loc: 230
tags:
  - rust
  - valkey
  - cluster
  - sentinel
sources:
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot
    title: CRC16-XMODEM hash slot and redirect parser
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore cluster slot routing
verification_strategy: public_seam_tdd
---

# Task `t03`: Implement Valkey Cluster (CLUSTER SHARDS) and Sentinel Discovery

## Objective & Scope Boundary

Implement Valkey Cluster (`CLUSTER SHARDS` with `CLUSTER SLOTS` fallback, `{hash_tag}` CRC16-XMODEM routing, `MOVED`/`ASK` redirects, and `READONLY` replica read routing) and Sentinel primary discovery (`SENTINEL get-master-addr-by-name` + `ROLE` verification) in `src/rs/vertebra-valkey/src/{cluster,sentinel,lib}.rs` per [`../spec.md` §2](../spec.md) [^resp3-rs] [^valkey-core].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/resp3/mod.rs` — `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.
- `src/py/vertebra/db/adapters/valkey/core.py` — Cluster and Sentinel configuration semantics.

## Contracts & Edge Cases

- Populate the `16384`-slot table via `CLUSTER SHARDS` (preferring healthy `"online"` replicas when replica reads are enabled via `READONLY`) and fall back to `CLUSTER SLOTS` when `CLUSTER SHARDS` is unavailable.
- Retry `MOVED` (updating the slot map) and `ASK` (`ASKING` + command on target node) up to `max_redirections`, raising `OperationalError` when exceeded.
- **Edge Case**: Multi-key commands with mismatched `{hash_tag}` slots on Cluster raise `ProgrammingError` (`CROSSSLOT`).

## Test Matrix

- Loopback tests in `src/rs/vertebra-valkey/tests/valkey_cluster_sentinel.rs` verify `CLUSTER SHARDS` parsing, `MOVED`/`ASK` redirect loops, replica `READONLY` routing, and Sentinel failover discovery.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey --test valkey_cluster_sentinel
```

## Acceptance Criteria

- [ ] Cluster `CLUSTER SHARDS`, `MOVED`/`ASK` redirects, replica reads, and Sentinel discovery pass all tests in `valkey_cluster_sentinel.rs`.
- [ ] Zero regressions in the fast verification gate.

[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs::redis_hash_slot` — CRC16-XMODEM hash slot and redirect parser.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore cluster slot routing.
