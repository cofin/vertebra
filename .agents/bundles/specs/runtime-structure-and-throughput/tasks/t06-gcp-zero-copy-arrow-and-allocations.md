---
type: Task
id: runtime-structure-and-throughput:t06
title: Slice BigQuery append_arrow_stream zero-copy and trim GCP RPC allocations
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
- runtime-structure-and-throughput:t05
files:
- src/rs/vertebra-bigquery/src/lib.rs
- src/rs/vertebra-bigquery/src/client.rs
- src/rs/vertebra-bigquery/src/arrow_ipc.rs
- src/py/vertebra/db/adapters/bigquery/core.py
- src/rs/vertebra-spanner/src/lib.rs
- src/rs/vertebra-spanner/src/client.rs
- src/rs/vertebra-spanner/src/codec.rs
tests:
- src/rs/vertebra-bigquery/tests/bigquery_wire_tests.rs
- src/rs/vertebra-spanner/tests/spanner_wire_tests.rs
- src/py/tests/unit/db/test_bigquery.py
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-06T17:46:15Z'
last_verified_commit: null
verification_evidence: "cargo test -p vertebra-bigquery -p vertebra-spanner (43 passed, 1 ignored), make dev, mypy.stubtest vertebra._bigquery vertebra._spanner, pytest (112 passed), cargo clippy -D warnings, ruff check/format, mypy, review_setup.py validate"
created_at: '2026-11-06T06:35:00Z'
updated_at: '2026-11-06T17:46:15Z'
commit: null
tags:
- bigquery
- spanner
- performance
- zero-copy
requirements:
- R6
---

# Slice BigQuery append_arrow_stream zero-copy and trim GCP RPC allocations

## Objective

Eliminate whole-payload IPC buffer copies in `vertebra-bigquery` `append_arrow_stream` using `PyBackedBytes` / `Bytes::slice` and remove avoidable per-RPC metadata header and parameter clones in `vertebra-bigquery` and `vertebra-spanner`.

## Context

Read `../spec.md`, `t05-gcp-submodules-and-commit-delay.md`, and `.agents/skills/review-performance/SKILL.md`. `AppendRowsRequest` `ArrowRecordBatch.serialized_record_batch` accepts `prost::bytes::Bytes` (`#[prost(bytes = "bytes", ...)]` or sub-slices of an owned `Bytes` backing buffer) so record batches slice directly into the input IPC stream without copying batch bodies.

## Steps

1. Add tests in `bigquery_wire_tests.rs` and `test_bigquery.py` exercising multi-batch Arrow IPC streams (including dictionary/LZ4-free standard record batches and oversized batch splitting) and verifying exact schema + batch payload delivery.
2. Refactor `src/rs/vertebra-bigquery/src/{lib.rs,arrow_ipc.rs,client.rs}` so Python `bytes` / `PyBackedBytes` wrap once into `bytes::Bytes` and `append_arrow_stream` slices serialized schema and record-batch message windows via `Bytes::slice` instead of `to_vec()` copying.
3. Pre-parse static `MetadataValue` headers (`x-goog-request-params`, resource prefixes) on client construction and pass borrowed/moved parameter maps across `vertebra-spanner` and `vertebra-bigquery` RPC builders instead of cloning strings per call.
4. Run verification and commit `perf(gcp): zero-copy slice BigQuery Arrow append streams and trim RPC clones`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-bigquery -p vertebra-spanner
make dev
uv run --no-sync pytest src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py -q
```

## Acceptance Criteria

- [x] R6: `append_arrow_stream` slices `Bytes` ranges zero-copy without whole-stream `Vec<u8>` duplication, and static gRPC routing headers are precomputed once per client.

## Notes & Discoveries

- `extract_arrow_ipc_bytes` wraps Python `bytes` zero-copy via `PyBackedBytes` -> `Bytes::from_owner` so `ArrowAppendPlan::build_bytes` slices schema and record-batch frames via `Bytes::slice` without heap-copying the IPC stream.
- Because `vertebra_db::py_async::block_on_db` uses `std::thread::scope`, synchronous PyO3 methods in `vertebra-bigquery` and `vertebra-spanner` borrow `&self.inner` and `&str` inputs directly across `py.detach` without per-call `Arc::clone` or `String` allocations.
