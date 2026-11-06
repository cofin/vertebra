---
type: Task
id: runtime-structure-and-throughput:t04
title: Add vertebra-core protobuf wire helpers and rename vertebra-py async_bridge
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
files:
- src/rs/vertebra-core/src/lib.rs
- src/rs/vertebra-core/src/proto.rs
- src/rs/vertebra-core/src/vector.rs
- src/rs/vertebra-db/src/dialer.rs
- src/rs/vertebra-py/src/lib.rs
- src/rs/vertebra-py/src/gcp_auth.rs
- src/rs/vertebra-py/src/vector.rs
- src/rs/vertebra-py/src/async_bridge.rs
- src/rs/vertebra-py/src/completion.rs
- .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
- .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests:
- src/py/tests/unit/db/test_vector_wire_codecs.py
- src/py/tests/unit/ffi/test_ffi.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
claimed_by: agent
claimed_at: '2026-11-06T16:00:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T16:11:30Z'
last_verified_commit: null
verification_evidence: "cargo test -p vertebra-core -p vertebra-db -p vertebra-py --features gcp (226 passed), make rs-test, make dev, mypy.stubtest vertebra._vertebra, pytest (955 passed, 1 skipped), cargo clippy -D warnings, review_setup.py validate"
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T16:11:30Z'
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

- [x] R4: `vertebra_core::proto` owns shared varint/wire codecs with zero duplicate implementations in `dialer.rs` or `vector.rs`, and `vertebra-py/src/async_bridge.rs` replaces `completion.rs` cleanly.

## Notes & Discoveries

None.
