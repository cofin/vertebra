---
type: Task
id: "native-cursor-data-path:network:t20"
chapter_id: network-engines
title: "Oracle relay switch on the client and pool constructors"
description: "The four Oracle client and pool constructors accept enable_reset_relay and pass it to the native config."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:network:t19"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md"
  - "src/rs/vertebra-oracle/src/client.rs:2278-2370"
  - "src/rs/vertebra-oracle/src/client.rs:2866-2960"
  - "src/rs/vertebra-oracle/src/pool.rs:600-690"
  - "src/rs/vertebra-oracle/src/pool.rs:716-800"
  - "src/rs/vertebra-oracle/src/pool.rs:1021-1105"
  - "src/py/vertebra/_oracle.pyi:137-180"
  - "src/py/vertebra/_oracle.pyi:279-322"
  - "src/py/vertebra/_oracle.pyi:421-470"
  - "src/py/vertebra/_oracle.pyi:519-570"
files:
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/rs/vertebra-oracle/src/pool.rs"
  - "src/py/vertebra/_oracle.pyi"
tests:
  - "src/py/tests/unit/db/test_oracle_adapter.py"
estimated_loc: 120
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
  - id: oracle-client
    resource: "src/rs/vertebra-oracle/src/client.rs::SyncOracleClient"
    title: "Constructors that build the native config"
tags: [oracle, rust, pyo3, config]
---

# Oracle relay switch on the client and pool constructors

## Objective & Scope Boundary

Python code can construct `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, and `AsyncOraclePool` with `enable_reset_relay=False`.

Out of scope:

- `OracleConfig` and the Python driver (t21).
- Any other constructor argument.

## Bounded Read Manifest

- `.agents/bundles/specs/native-cursor-data-path/chapters/network-engines/spec.md` section 2 for the shared cursor, error, lease, and driver contracts.
- `src/rs/vertebra-oracle/src/client.rs:2281-2360` and `:2869-2950` are the two client constructors and their calls to `build_raw_oracle_options` followed by `validate` on `NativeOracleConfig`.
- `src/rs/vertebra-oracle/src/pool.rs:600-690` is `build_pool_options_from_py`; `:716-800` and `:1021-1105` are the pool constructors.

## Contracts & Edge Cases

- Each of the four `#[new]` signatures gains a keyword-only argument `enable_reset_relay=true`, placed last.
- After `validate` on `NativeOracleConfig` returns, each constructor sets `cfg.enable_reset_relay = enable_reset_relay` before the config is stored or shared. `build_pool_options_from_py` takes the flag as a new last parameter and sets it on the config it builds.
- `build_raw_oracle_options` is not changed.
- Stub: add `enable_reset_relay: bool = True` as the last keyword-only parameter of the four `__init__` signatures.
- The value has no effect on connections that never use the relay (TCPS, address lists).

## Test Matrix

| Case | Input | Expected |
| --- | --- | --- |
| Accepts flag | Construct each of the four classes with `enable_reset_relay=False` and an unreachable host | Construction succeeds without connecting |
| Default | Construct without the argument | Succeeds |
| Wrong type | `enable_reset_relay='no'` | `TypeError` |
| Stub | `mypy.stubtest vertebra._oracle` | Passes with no allow-list change |

## Verification

- `make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k "relay"`
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] New tests named `test_relay_constructor_*` cover the matrix.
- [ ] The stub diff adds one parameter to four signatures and nothing else.
