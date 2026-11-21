---
type: Task
id: "native-cursor-data-path:network:t19"
chapter_id: network-engines
title: "Oracle relay switch in the native config and dialer"
description: "NativeOracleConfig carries enable_reset_relay and the session dialer starts the in-process TNS relay only when it is true."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/spec.md:197-200"
  - "src/rs/vertebra-oracle/src/config.rs:234-271"
  - "src/rs/vertebra-oracle/src/config.rs:317-330"
  - "src/rs/vertebra-oracle/src/config.rs:440-500"
  - "src/rs/vertebra-oracle/src/dialer.rs:840-900"
  - "src/rs/vertebra-oracle/src/dialer.rs:920-945"
  - "src/rs/vertebra-oracle/src/dialer.rs:986-998"
  - "src/rs/vertebra-oracle/src/dialer.rs:1070-1125"
files:
  - "src/rs/vertebra-oracle/src/config.rs"
  - "src/rs/vertebra-oracle/src/dialer.rs"
tests:
  - "src/rs/vertebra-oracle/src/config.rs"
  - "src/rs/vertebra-oracle/src/dialer.rs"
estimated_loc: 110
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-20T15:30:00Z
commit: null
sources:
  - id: chapter-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md
    title: "Chapter contract for the network engine cursors"
  - id: oracle-dialer
    resource: "src/rs/vertebra-oracle/src/dialer.rs::dial_oracledb_inner"
    title: "Dialer that starts the relay"
tags: [oracle, rust, config]
---

# Oracle relay switch in the native config and dialer

## Objective & Scope Boundary

The Rust side can connect an Oracle session without the in-process TNS relay. The choice is a field on the validated config. The default keeps the relay on, so nothing changes for existing callers.

Out of scope:

- Python-visible constructor arguments (t20) and `OracleConfig` (t21).
- `RawOracleOptions`. Its literals in `client.rs` and `lib.rs` stay untouched.
- The relay implementation itself.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-oracle/src/config.rs:234-271` is `NativeOracleConfig`; `:317-330` is the start of `validate`; `:483-500` is its `Debug` impl.
- `src/rs/vertebra-oracle/src/dialer.rs:895-945` is `dial_oracledb_inner` with the relay branch; `:989-998` are the two public dial functions.

## Contracts & Edge Cases

- Add `pub enable_reset_relay: bool` to `NativeOracleConfig`. `validate` sets it to `true`. The `Debug` impl prints it.
- `connect_oracledb_session(cfg)` calls `dial_oracledb_inner(cfg, cfg.enable_reset_relay)`. `connect_oracledb(cfg)` keeps passing `false`.
- Extract the relay decision into `fn relay_target(enable_reset_relay: bool, protocol: OracleProtocol, connect_string: &str)` returning the `Option` that `parse_single_tcp_target` returns today, and `None` when `enable_reset_relay` is `false` or the protocol is not TCP. `dial_oracledb_inner` calls it.
- With `enable_reset_relay` false the dialer passes `resolved.connect_string` to the driver unchanged, binds no local socket, and starts no relay thread.
- TCPS, multi-address descriptors, and alias-based connect strings never start the relay, as today.

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Default | `NativeOracleConfig::validate(RawOracleOptions::default())` | `enable_reset_relay` is `true` |
| Relay on, TCP | `relay_target(true, Tcp, single host string)` | `Some(target)` |
| Relay off, TCP | `relay_target(false, Tcp, single host string)` | `None` |
| Relay on, TCPS | `relay_target(true, Tcps, ...)` | `None` |
| Relay on, address list | `relay_target(true, Tcp, two-address descriptor)` | `None` |
| Debug output | `format` of the config | Contains `enable_reset_relay` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle config`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle dialer`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Each matrix case has a Rust unit test.
- [ ] Existing dialer and config tests pass unchanged.
