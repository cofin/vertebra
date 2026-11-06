---
type: Task
id: runtime-structure-and-throughput:t04
title: Add vertebra-core protobuf wire helpers and rename vertebra-py async_bridge
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/rs/vertebra-core/src/lib.rs
- src/rs/vertebra-core/src/proto.rs
- src/rs/vertebra-core/src/vector.rs
- src/rs/vertebra-db/src/dialer.rs
- src/rs/vertebra-py/src/lib.rs
- src/rs/vertebra-py/src/vector.rs
- src/rs/vertebra-py/src/async_bridge.rs
- src/rs/vertebra-py/src/completion.rs
tests:
- src/py/tests/unit/db/test_vector_wire_codecs.py
- src/py/tests/unit/ffi/test_ffi.py
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T06:35:00Z'
commit: null
tags:
- rust
- proto
- pyo3
requirements:
- R4
---

# Add vertebra-core protobuf wire helpers and rename vertebra-py async_bridge

## Objective

Create `src/rs/vertebra-core/src/proto.rs` with shared protobuf varint/field reader and writer primitives, deduplicate varint helpers across `vertebra-core`, `vertebra-db`, and `vertebra-py`, and rename `src/rs/vertebra-py/src/completion.rs` to `src/rs/vertebra-py/src/async_bridge.rs`.

## Context

Read `../spec.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. Keep `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier` in `src/rs/vertebra-core/src/notify.rs` and preserve all exported Python symbols in `vertebra._vertebra`.

## Steps

1. Write Rust unit tests in `src/rs/vertebra-core/src/proto.rs` covering 1-byte through 10-byte varints, truncated inputs, overflow (`> 64` bits), length-delimited slices, and tag encoding/decoding.
2. Replace duplicated varint encode/decode functions in `src/rs/vertebra-core/src/vector.rs`, `src/rs/vertebra-db/src/dialer.rs`, and `src/rs/vertebra-py/src/vector.rs` with `vertebra_core::proto` calls.
3. Rename `src/rs/vertebra-py/src/completion.rs` via `git mv` to `src/rs/vertebra-py/src/async_bridge.rs` and update `src/rs/vertebra-py/src/lib.rs` module declarations and internal imports.
4. Run verification and commit `refactor(rust): extract vertebra-core proto helpers and rename vertebra-py async_bridge`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-db -p vertebra-py --features gcp
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra
uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/ffi/test_ffi.py -q
```

## Acceptance Criteria

- [ ] R4: `vertebra_core::proto` owns shared varint/wire codecs with zero duplicate implementations in `dialer.rs` or `vector.rs`, and `vertebra-py/src/async_bridge.rs` replaces `completion.rs` cleanly.

## Notes & Discoveries

None.
