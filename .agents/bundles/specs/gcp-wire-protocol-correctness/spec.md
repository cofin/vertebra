---
type: Spec
flow_id: gcp-wire-protocol-correctness
title: Rust GCP Wire Correctness
description: Correct BigQuery Storage and Spanner protobuf field numbers with golden-byte tests, eliminate false success paths, make the completion bridge panic-safe and cancellable, and surface typed exceptions.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T03:30:00Z
tags: [rust, gcp, spanner, bigquery, protobuf, bugfix]
---

# Flow Charter: Rust GCP Wire Correctness

## Charter

This flow makes the pure-Rust BigQuery and Spanner clients speak the real wire format and never report success for operations that did not complete. The hand-written zero-copy protobuf cursor stays. Every field number becomes a named per-message `const`, and golden-byte fixtures generated once with `protoc` from the official `.proto` files are checked in. Tests that fail only because an emulator lacks a feature are marked `xfail(strict=True)` with the reason.

## Scope

1. **Field numbers.** Fix the numbers and add consts plus golden fixtures for:
   - BigQuery Storage Read: `vertebra-db/src/bigquery.rs::encode_create_read_session_request`, `decode_read_session_response`, `decode_read_rows_response`. Correct numbers: ReadSession `data_format=3`, `arrow_schema=5`, `table=6`, `read_options=8`, `streams=10`; ReadRowsResponse `arrow_record_batch=4`, `arrow_schema=8`.
   - BigQuery Storage Write: `encode_append_rows_request` (`arrow_rows=5`) and `decode_append_rows_response` (field 5 is the echoed `write_stream`).
   - Spanner: `vertebra-db/src/spanner.rs::get_or_create_multiplexed_session` (`Session.creator_role=5`), `encode_execute_sql_request` (`request_options=11`), `decode_transaction_message` (`Transaction.precommit_token=3`), `commit_read_write_tx` (`CommitRequest.precommit_token=9`).
   - Rewrite `vertebra-db/tests/bigquery_wire_tests.rs` and `spanner_wire_tests.rs` to assert against the golden fixtures.
2. **No false success.**
   - `commit_read_write_tx` returns an error after MultiplexedSessionRetry exhaustion.
   - `update_database_ddl` honors a deadline and returns a pending/timeout error while `done=false`.
   - `append_arrow_stream` verifies the response count and offsets.
   - `grpc.rs::bidi_streaming` propagates encode errors.
3. **Completion bridge.** `vertebra-py/src/completion.rs::spawn_rust_db_task` resolves the Python future with an exception when the task panics (JoinHandle/`catch_unwind`), and aborts the Tokio task when the awaitable is cancelled. Fix the stub `_vertebra.pyi::RustDbTaskAwaitable` (it declares no constructor).
4. **Hardening.**
   - `inspect_arrow_message_flatbuffer`/`rb_vtable` use checked offsets and return errors on malformed input.
   - `split_arrow_ipc_stream` rejects input without the continuation marker and handles DictionaryBatch.
   - `execute_partitioned_arrow` emits real Arrow IPC through the shared writer, or is removed from the binding and stub when no caller needs it.
5. **Placeholder rewriter.** `rewrite_spangres_placeholders` is char-aware and lexes GoogleSQL string forms (backslash escapes, backticks, triple-quoted strings).
6. **Idempotency.**
   - `grpc.rs::server_streaming_from_lpm_frame` resends only when hyper reports the request was not sent, or when the RPC is idempotent (reads and `ExecuteStreamingSql` with a resume token).
   - `execute_streaming_sql` resumes with the transaction id once one is known.
   - `execute_in_read_write_tx` implements the documented explicit BeginTransaction fallback.
7. **Typed exceptions.**
   - Sync and async bindings raise one exception hierarchy carrying the gRPC status code and details (`vertebra-py/src/spanner.rs`, `bigquery.rs`, and the `_vertebra.pyi` stubs).
   - Python substring matching in `db/spanner/driver.py`, `db/spanner/core.py`, and `db/bigquery/core.py` is replaced by code checks.
8. **Auth.**
   - `vertebra-core/src/gcp_auth.rs::fetch_token_from_gcloud_cli` uses `tokio::process`.
   - `GcpTokenCache::new` honors proxy environment variables.
   - `GcpTokenCache::from_options` takes scopes explicitly, with no GCS default, and the `from_gcs_options` alias is removed.
9. **REST correctness.** `bigquery.rs::check_rest_error` treats `errors[]` warnings as non-fatal when `status.errorResult` is absent. `build_get_query_results_url` encodes query parameters with `url`.
10. **Emulator gaps.** Emulator integration tests for features the Spanner/BigQuery emulators lack are marked `xfail(strict=True)` with the reason.

## Out of Scope

- Module splits, retry policy unification, and copy removal (Chapter 8).

## Public Seams

`_vertebra.pyi`: the exception hierarchy, `SpannerClient`, `BigQueryClient`, and `RustDbTaskAwaitable`.

## Dependencies

None. This flow can run in parallel with Chapter 1.

## Acceptance Boundary

- The golden-byte tests pass.
- Panic and cancellation tests resolve or abort the futures.
- `make rs-test` and `make py-test` pass, with the emulator gaps reported as `xfail`.

## Draft Task Outline

- t01 BigQuery Storage Read consts + golden fixtures.
- t02 BigQuery Storage Write consts + golden fixtures + offset verification.
- t03 Spanner session/ExecuteSql/Transaction/Commit consts + golden fixtures.
- t04 Commit retry exhaustion and DDL deadline errors.
- t05 Completion bridge panic and cancel.
- t06 Arrow parser hardening and partitioned-Arrow output.
- t07 Spangres placeholder lexer and idempotent resend rules.
- t08 Typed exception hierarchy plus async token fetch and REST fixes.
