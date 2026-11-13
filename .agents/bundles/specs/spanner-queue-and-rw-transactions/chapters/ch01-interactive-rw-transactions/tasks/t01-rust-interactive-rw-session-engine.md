---
type: Task
id: "spanner-queue-and-rw-transactions:ch01-interactive-rw-transactions:t01"
flow_id: spanner-queue-and-rw-transactions
chapter_id: ch01-interactive-rw-transactions
title: "Implement Interactive Multi-Statement Spanner ReadWrite Transaction Engine in vertebra-spanner"
description: "Build stateful ReadWrite transaction handles in src/rs/vertebra-spanner/src/{client,errors}.rs tracking TransactionSelector::Id, seqno, and precommit_token."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/errors.rs
tests:
  - src/py/tests/unit/db/test_spanner.py
read_set:
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/codec.rs
  - src/rs/vertebra-spanner/src/errors.rs
estimated_loc: 235
tags:
  - spanner
  - rust
  - transactions
sources:
  - id: spn-client-rs
    resource: src/rs/vertebra-spanner/src/client.rs
    title: "Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner"
  - id: spn-errors-rs
    resource: src/rs/vertebra-spanner/src/errors.rs
    title: "SpannerError and SpannerAbortedError retry_delay_ms extraction"
---

# Task `t01`: Implement Interactive Multi-Statement Spanner ReadWrite Transaction Engine in vertebra-spanner

## Objective & Scope Boundary

Implement interactive multi-statement `ReadWrite` transaction session handles in `src/rs/vertebra-spanner/src/{client,errors}.rs` per [`../spec.md`](../spec.md) [^spn-client-rs] [^spn-errors-rs].

## Bounded Read Manifest

- `src/rs/vertebra-spanner/src/client.rs` — `SpannerClient` multiplexed session lifecycle and `read_write_transaction()` usage.
- `src/rs/vertebra-spanner/src/errors.rs` — `SpannerAbortedError` (`code = 10`, `retry_delay_ms` from `google.rpc.RetryInfo`).

## Contracts & Edge Cases

- Implement `SpannerReadWriteTx` acquiring an interactive `ReadWrite` transaction (`BeginTransaction` / inline begin with optional `multiplexed_session_previous_transaction_id` for wound-wait lock priority and `transaction_tag`).
- Route `query`, `execute_dml`, and `execute_batch_dml` over the pinned `TransactionSelector::Id` with monotonic 1-based `seqno`, `x-goog-spanner-route-to-leader: true`, `ExecuteBatchDmlResponse.status` inspection, and highest-`seq_num` `MultiplexedSessionPrecommitToken` tracking.
- Implement `commit(mutations)` (attaching latest `precommit_token`, `max_commit_delay_ms`, `exclude_txn_from_change_streams`, and replaying `CommitResponse.MultiplexedSessionRetry` with empty mutations) and `rollback()`, surfacing `SpannerAbortedError` with server `retry_delay_ms`.

## Test Matrix

- Rust stub/unit tests (`Spanner::from_stub`) verifying `BeginTransaction` -> `ExecuteStreamingSql` (`SELECT ... FOR UPDATE`) -> `ExecuteSql` (`UPDATE`) -> `Commit` carries one `transaction_id`, increments `seqno` `1, 2`, forwards highest `precommit_token`, replays `MultiplexedSessionRetry`, and extracts `ABORTED` `retry_delay_ms`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-spanner && uv run --no-sync ruff check src/py/vertebra/_spanner.pyi src/py/vertebra/db/adapters/spanner && uv run --no-sync mypy src/py/vertebra/db/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/db/test_spanner.py
```

## Acceptance Criteria

- [ ] `cargo test -p vertebra-spanner` passes interactive multi-statement `ReadWrite` commit, rollback, precommit-token, and `ABORTED` retry-info tests.
- [ ] Zero regressions in the chapter fast verification gate.

[^spn-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Single-shot execute_dml and execute_batch_dml RPC runners in vertebra-spanner.
[^spn-errors-rs]: `src/rs/vertebra-spanner/src/errors.rs` — SpannerError and SpannerAbortedError retry_delay_ms extraction.
