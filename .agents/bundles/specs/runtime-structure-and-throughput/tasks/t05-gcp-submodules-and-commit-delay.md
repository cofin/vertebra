---
type: Task
id: runtime-structure-and-throughput:t05
title: Split Spanner and BigQuery Rust submodules, wire max_commit_delay_ms, and unify staging stores
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- runtime-structure-and-throughput:t04
files:
- src/rs/vertebra-spanner/src/lib.rs
- src/rs/vertebra-spanner/src/client.rs
- src/rs/vertebra-spanner/src/codec.rs
- src/rs/vertebra-spanner/src/arrow_ipc.rs
- src/rs/vertebra-bigquery/src/lib.rs
- src/rs/vertebra-bigquery/src/client.rs
- src/rs/vertebra-bigquery/src/codec.rs
- src/rs/vertebra-bigquery/src/arrow_ipc.rs
- src/py/vertebra/_spanner.pyi
- src/py/vertebra/db/_staging.py
- src/py/vertebra/db/adapters/spanner/driver.py
- src/py/vertebra/db/adapters/bigquery/driver.py
tests:
- src/rs/vertebra-spanner/tests/spanner_wire_tests.rs
- src/rs/vertebra-bigquery/tests/bigquery_wire_tests.rs
- src/py/tests/unit/db/test_spanner.py
- src/py/tests/unit/db/test_bigquery.py
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
- spanner
- bigquery
- rust
requirements:
- R5
---

# Split Spanner and BigQuery Rust submodules, wire max_commit_delay_ms, and unify staging stores

## Objective

Decompose `vertebra-spanner` and `vertebra-bigquery` into focused `client.rs`, `codec.rs`, and `arrow_ipc.rs` submodules, wire `SpannerConfig.max_commit_delay_ms` through Rust `SyncSpannerClient` / `AsyncSpannerClient` commit requests, and extract one shared `ObjectStore` staging resolver for BigQuery and Spanner Python drivers.

## Context

Read `../spec.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and `.agents/skills/pyo3-bridge/SKILL.md`. Keep `_spanner.pyi` and `_bigquery.pyi` synchronized with `mypy.stubtest`.

## Steps

1. Add failing wire and Python unit tests in `spanner_wire_tests.rs`, `test_spanner.py`, and `test_bigquery.py` verifying that `max_commit_delay_ms` populates `CommitRequest.max_commit_delay` (`prost_types::Duration`) on both sync and async Spanner commits and that shared staging resolution rejects missing stores cleanly without `memory://` fallbacks.
2. Split `src/rs/vertebra-spanner/src/client.rs` and `src/rs/vertebra-bigquery/src/client.rs` into `client.rs` (gRPC/HTTP transport & client lifecycles), `codec.rs` (deduplicating `json_cell_to_protobuf_value` and parameter/row encoders), and `arrow_ipc.rs` (SHM Arrow IPC builders/writers).
3. Accept optional `max_commit_delay_ms: Option<u64>` in `SyncSpannerClient` / `AsyncSpannerClient` (`lib.rs`, `client.rs`, `_spanner.pyi`) and pass `config.max_commit_delay_ms` from `src/py/vertebra/db/adapters/spanner/driver.py`; extract shared staging store resolution into `src/py/vertebra/db/_staging.py` used by both `spanner/driver.py` and `bigquery/driver.py`.
4. Run verification and commit `refactor(gcp): modularize spanner and bigquery crates and wire max_commit_delay_ms`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner -p vertebra-bigquery
make dev
uv run --no-sync python -m mypy.stubtest vertebra._spanner vertebra._bigquery
uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py -q
```

## Acceptance Criteria

- [ ] R5: Both GCP Rust crates are cleanly split into `client.rs`, `codec.rs`, and `arrow_ipc.rs` with zero duplicated JSON-to-protobuf value converters, `max_commit_delay_ms` reaches `CommitRequest.max_commit_delay`, and staging store helpers are shared cleanly.

## Notes & Discoveries

None.
