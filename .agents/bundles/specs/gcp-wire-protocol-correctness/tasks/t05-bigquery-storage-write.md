---
type: Task
id: gcp-wire-protocol-correctness:t05
title: BigQuery Storage Write on the official client, hardened Arrow append plan, grpc.rs removal
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: [gcp-wire-protocol-correctness:t03, gcp-wire-protocol-correctness:t04]
files:
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-db/src/bigquery.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/grpc.rs
tests:
  - src/rs/vertebra-db/tests/bigquery_wire_tests.rs
  - src/rs/vertebra-db/tests/grpc_wire_and_pool_tests.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:10:00Z
commit: null
tags: [rust, gcp, bigquery, arrow, bugfix, cleanup]
---

# t05 — BigQuery Storage Write on the official client, hardened Arrow append plan, grpc.rs removal

## Objective

`append_arrow_stream` sends one record-batch message per request through `google_cloud_bigquery::client::Write`, verifies response count and offsets, and rejects malformed Arrow input without panicking. `grpc.rs` and its tests are deleted.

## Context

- Spec R9, R10, R11 (`../spec.md`). Defects removed: `arrow_rows` on field 4, `write_stream` parsed as `RowError`, `StorageError.entity` read from field 1, no response count/offset check, silent `bidi_streaming` encode errors, `split_arrow_ipc_stream` slicing bytes without a continuation marker and treating dictionary batches as record batches, `inspect_arrow_message_flatbuffer` negative-offset panics, several record batches concatenated into one request.
- `regression_tdd`: `ArrowAppendPlan` (build + verify) is a pure in-process seam tested directly; `client::Write` has no stub, so the thin adapter is covered by compilation.
- Verified crate APIs: `google_cloud_bigquery::client::Write::builder()` (`with_endpoint`, `with_credentials`, `build().await`); `write.open_default_stream(table).build_arrow(model::ArrowSchema)`; `write.create_stream::<S>(table).build_arrow(schema)` for `google_cloud_bigquery::write::stream_type::{CommittedStream, PendingStream, BufferedStream}` → `google_cloud_bigquery::write::{CommittedWriter, PendingWriter, BufferedWriter}`; `DefaultWriter::append(rows).send().await -> Result<AppendResponse, AppendError>`; `{Committed,Pending,Buffered}Writer::append(rows).set_offset(o).send().await`; `PendingWriter::{finalize, commit}`; `BufferedWriter::flush(offset)`; `write_stream()`; `google_cloud_bigquery::model_ext::AppendResponse { offset: Option<i64>, .. }`; `google_cloud_bigquery::error::{AppendError::{Rpc { source }, RowErrors { status, row_errors }}, CommitError::{Rpc { source }, FailedTransaction { stream_errors }}, WriterBuilderError}` (`#[non_exhaustive]`); `model::ArrowRecordBatch::new().set_serialized_record_batch(bytes).set_row_count(n)`; `model::RowError { index, code, message }` (`code.value()`); `model::StorageError { entity, error_message, .. }`.
- Arrow IPC encapsulated message: `0xFFFFFFFF`, `u32` LE metadata size, flatbuffer `Message` (vtable slot byte offsets: `header_type` 6, `header` 8, `bodyLength` 10), body. `MessageHeader`: 1 Schema, 2 DictionaryBatch, 3 RecordBatch, 4 Tensor, 5 SparseTensor. `RecordBatch.length` is vtable slot byte offset 4. End of stream: `ARROW_IPC_EOS_MARKER`.
- `generate_request_uuid_v4` (grpc.rs) wraps `vertebra_core::id::uuid4_str`; the REST payload builders switch to the latter.
- `BigQueryClientConfig.ca_pem` is REST-only (t04); `storage_write` does not consult it.

## Steps

1. **Write failing tests (RED)** in `src/rs/vertebra-db/tests/bigquery_wire_tests.rs`. Keep `build_encapsulated_arrow_frame` and `build_mock_arrow_ipc_stream`. Delete `test_bigquery_storage_write_default_pending_and_row_errors`, `struct MockGrpcBody` and its `impl Body for MockGrpcBody`, `fn make_grpc_response`, the `decode_append_rows_request` import, `use vertebra_db::grpc::{GrpcCode, LpmFrameDecoder, ProtoReader, ProtoWriter, WireType, encode_lpm_frame};`, `use std::pin::Pin;`, `use std::task::{Context, Poll};`, `HeaderValue` from the `http` import, `Body` and `Frame` from the `hyper::body` import, and `mpsc` from the `tokio::sync` import; then remove each further import that `cargo clippy -p vertebra-db --all-targets -- -D warnings` reports unused. Add:

   ```rust
   #[test]
   fn append_plan_assigns_offsets_per_record_batch() {
       let ipc = build_mock_arrow_ipc_stream(b"SCHEMA", &[(b"B1", 3), (b"B2", 2), (b"B3", 4)]);
       let plan = ArrowAppendPlan::build(&ipc, BigQueryWriteMode::Pending).expect("plan");
       assert_eq!(plan.requests.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![Some(0), Some(3), Some(5)]);
       assert_eq!(plan.requests.iter().map(|r| r.row_count).collect::<Vec<_>>(), vec![3, 2, 4]);
       assert_eq!(plan.total_rows(), 9);
       assert_eq!(&plan.schema[..], &build_encapsulated_arrow_frame(1, b"SCHEMA", 0)[..]);
       assert_eq!(&plan.requests[1].record_batch[..], &build_encapsulated_arrow_frame(3, b"B2", 2)[..]);
       let default_plan = ArrowAppendPlan::build(&ipc, BigQueryWriteMode::Default).expect("plan");
       assert!(default_plan.requests.iter().all(|r| r.offset.is_none()));
   }

   #[test]
   fn append_plan_verification_rejects_missing_and_mismatched_offsets() {
       let ipc = build_mock_arrow_ipc_stream(b"SCHEMA", &[(b"B1", 3), (b"B2", 2)]);
       let plan = ArrowAppendPlan::build(&ipc, BigQueryWriteMode::Committed).expect("plan");
       assert_eq!(plan.verify_responses(&[Some(0), Some(3)]).expect("verified"), 5);
       assert!(matches!(plan.verify_responses(&[Some(0)]), Err(BigQueryError::AppendVerification(_))));
       assert!(matches!(plan.verify_responses(&[Some(0), Some(4)]), Err(BigQueryError::AppendVerification(_))));
       let default_plan = ArrowAppendPlan::build(&ipc, BigQueryWriteMode::Default).expect("plan");
       assert_eq!(default_plan.verify_responses(&[None, None]).expect("verified"), 5);
   }

   #[test]
   fn append_plan_rejects_malformed_streams() {
       let schema = build_encapsulated_arrow_frame(1, b"SCHEMA", 0);
       let batch = build_encapsulated_arrow_frame(3, b"B1", 3);
       let mut negative_vtable = schema.clone();
       negative_vtable[28..32].copy_from_slice(&1000i32.to_le_bytes());
       let oversized = build_encapsulated_arrow_frame(3, &vec![0u8; MAX_APPEND_ROWS_CHUNK_BYTES + 8], 1);
       let cases: Vec<(&str, Vec<u8>)> = vec![
           ("missing continuation marker", b"ARROW1\0\0garbage-bytes".to_vec()),
           ("record batch before schema", batch.clone()),
           ("dictionary batch", [schema.clone(), build_encapsulated_arrow_frame(2, b"DICT", 0)].concat()),
           ("negative vtable offset", negative_vtable),
           ("oversized record batch", [schema.clone(), oversized].concat()),
           ("truncated body", [schema.clone(), batch[..batch.len() - 8].to_vec()].concat()),
           ("duplicate schema", [schema.clone(), schema.clone()].concat()),
       ];
       for (label, bytes) in cases {
           let outcome = ArrowAppendPlan::build(&bytes, BigQueryWriteMode::Default);
           assert!(matches!(outcome, Err(BigQueryError::InvalidArgument(_))), "{label}: {outcome:?}");
       }
       assert_eq!(ArrowAppendPlan::build(&[], BigQueryWriteMode::Default).expect("empty").requests.len(), 0);
   }
   ```

   Frame byte 28 is the 8-byte prefix plus the root table offset 20; writing `1000` there makes `root - vtable_rel` negative.
   - RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests append_plan` → compile error.

2. **Implement the append plan (GREEN) in `src/rs/vertebra-db/src/bigquery.rs`.**

   ```rust
   /// One `AppendRowsRequest` worth of Arrow data: a single encapsulated record-batch message.
   #[derive(Clone, Debug, PartialEq, Eq)]
   pub struct ArrowAppendRequest {
       pub record_batch: Bytes,
       pub row_count: i64,
       pub offset: Option<i64>,
   }

   /// Ordered Storage Write requests derived from one Arrow IPC stream.
   #[derive(Clone, Debug, Default, PartialEq, Eq)]
   pub struct ArrowAppendPlan {
       pub schema: Bytes,
       pub requests: Vec<ArrowAppendRequest>,
   }

   impl ArrowAppendPlan {
       pub fn build(stream: &[u8], mode: BigQueryWriteMode) -> Result<Self, BigQueryError>;
       pub fn total_rows(&self) -> i64;
       pub fn verify_responses(&self, offsets: &[Option<i64>]) -> Result<u64, BigQueryError>;
   }
   ```

   - `build`: walk messages from byte 0; stop at end of input, at `ARROW_IPC_EOS_MARKER`, or at a continuation marker followed by metadata size `0`. Fewer than 8 remaining bytes → `InvalidArgument("truncated Arrow IPC message header at byte {pos}")`; missing `0xFFFFFFFF` → `InvalidArgument("Arrow IPC message at byte {pos} lacks the 0xFFFFFFFF continuation marker")`; metadata and body lengths are each rounded up to 8 with `checked_add`; overflow or a range past the input → `InvalidArgument("truncated Arrow IPC message at byte {pos}")`. Classify with `ArrowMessageHeader::parse`: first message must be Schema (`InvalidArgument("Arrow IPC stream must start with a Schema message")`); second Schema → `InvalidArgument("Arrow IPC stream contains more than one Schema message")`; DictionaryBatch → `InvalidArgument("dictionary-encoded Arrow columns are not supported by AppendRows; cast dictionary columns to their value type before appending")`; Tensor/SparseTensor/unknown → `InvalidArgument("unsupported Arrow IPC message type {t}")`; RecordBatch with negative length → `InvalidArgument`; a RecordBatch message longer than `MAX_APPEND_ROWS_CHUNK_BYTES` → `InvalidArgument("Arrow record batch {index} is {len} bytes; AppendRows accepts at most {MAX_APPEND_ROWS_CHUNK_BYTES} bytes per request; split the batch")`. Each record batch is one `ArrowAppendRequest` (`Bytes::copy_from_slice` of the whole message); `offset` is `None` for `Default`, otherwise the running row total before the request.
   - `verify_responses`: count mismatch → `AppendVerification(format!("AppendRows returned {got} responses for {expected} requests"))`; for `Some(expected)` offsets a different response → `AppendVerification(format!("AppendRows response {i} reported offset {got:?}; expected {expected}"))`; success → `total_rows() as u64`.
   - Private `struct ArrowMessageHeader { header_type: u8, body_len: usize, row_count: i64 }` with `fn parse(fb: &[u8]) -> Result<Self, BigQueryError>` and private `read_u16_le` / `read_u32_le` / `read_i32_le` / `read_i64_le(fb, at)` using `fb.get(at..at + N)` → `InvalidArgument(format!("malformed Arrow flatbuffer: offset {at} out of range"))`; table positions computed in `i64` and rejected when negative or past `fb.len()`; a vtable slot at or beyond the vtable length is unset (`header_type` unset → error; `bodyLength` / `length` unset → 0); negative `bodyLength` → error.
   - `BigQueryError`: add `#[error("BigQuery Storage Write verification failed: {0}")] AppendVerification(String)`.
   - Delete `split_arrow_ipc_stream`, `try_parse_encapsulated_arrow_stream`, `inspect_arrow_message_flatbuffer`.

3. **Move `append_arrow_stream` onto `client::Write`.**
   - Field `write_client: tokio::sync::OnceCell<google_cloud_bigquery::client::Write>`; `async fn storage_write(&self)` mirrors `storage_read` (t04) with `Write::builder()`.
   - Flow: `let write_mode = BigQueryWriteMode::from_str_mode(mode)?; let table = self.normalize_table_resource(parent_table)?; let plan = ArrowAppendPlan::build(arrow_ipc_bytes, write_mode)?;` empty plan → `Ok(BigQueryAppendResult { appended_rows: 0, write_stream: format!("{table}/streams/_default") })` without an RPC. `schema = model::ArrowSchema::new().set_serialized_schema(plan.schema.clone())`; per request `model::ArrowRecordBatch::new().set_serialized_record_batch(r.record_batch.clone()).set_row_count(r.row_count)`.
     - `Default`: `open_default_stream(&table).build_arrow(schema)`; `append(rows).send()` per request; `write_stream = format!("{table}/streams/_default")`.
     - `Committed` / `Pending` / `Buffered`: `create_stream::<CommittedStream | PendingStream | BufferedStream>(&table).build_arrow(schema)`; `append(rows).set_offset(r.offset.unwrap_or(0)).send()` per request; `write_stream = writer.write_stream().to_string()`. After the appends: `Pending` → `finalize().await?` then `commit().await`; `Buffered` → `flush(plan.total_rows() - 1).await?` when `total_rows() > 0`; `Committed` → nothing.
     - Create all append futures in order, await them in order, push `response.offset`, then `plan.verify_responses(&offsets)?`; return `BigQueryAppendResult { appended_rows, write_stream }`.
   - Error mapping: `AppendError::RowErrors { row_errors, .. }` → `BigQueryError::RowErrors(row_errors.into_iter().map(|e| BigQueryRowError { index: e.index, code: e.code.value().unwrap_or(0), message: e.message }).collect())`; `AppendError::Rpc { source }` and `CommitError::Rpc { source }` → `BigQueryError::Client(source)`; `CommitError::FailedTransaction { stream_errors }` → `AppendVerification(format!("BatchCommitWriteStreams rejected stream {entity}: {error_message}"))` from the first error; `WriterBuilderError` and every other variant → `AppendVerification(err.to_string())`.
   - Delete `create_write_stream`, `get_or_init_grpc_pool`, `build_grpc_metadata`, field `grpc_pool`, `encode_create_write_stream_request`, `decode_write_stream_name`, `encode_append_rows_request`, `DecodedAppendRowsRequest`, `decode_append_rows_request`, `DecodedAppendRowsResponse`, `decode_append_rows_response`, `encode_finalize_write_stream_request`, `encode_batch_commit_write_streams_request`, `encode_flush_rows_request`, `check_batch_commit_response`. Replace `generate_request_uuid_v4()` with `vertebra_core::id::uuid4_str()` in `build_fastpath_query_payload` and `build_jobs_insert_payload`.

4. **Delete the gRPC pool.** Remove `src/rs/vertebra-db/src/grpc.rs`, `pub mod grpc;` and its doc bullet in `lib.rs`, and `src/rs/vertebra-db/tests/grpc_wire_and_pool_tests.rs`. Remove a `[dependencies]` entry of `vertebra-db/Cargo.toml` only when `grep -rn "<crate_ident>::" src/rs/vertebra-db/src src/rs/vertebra-db/tests` prints nothing for it (check `hyper`, `hyper_util`, `http_body_util`, `flate2`, `webpki_root_certs`, `tokio_rustls`, `rustls_pemfile`); keep every crate that still matches.

5. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests append_plan` → compile error.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-db`
  - `cd src/rs && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check`
  - `! grep -rnE "crate::grpc|vertebra_db::grpc" src/rs`
  - `make dev && uv run pytest src/py/tests/unit/db/test_bigquery.py -q`
- Phase gate: `make lint-all && make rs-test && make py-test && make bench-check` (record regressions above threshold in Notes and the PR body).
- Commit: `git commit -S -m "fix(rs-db): append BigQuery Arrow batches through the official Write client and remove grpc.rs"`.

## Acceptance Criteria

- [ ] R9: one record batch per request; offsets for app-created streams; response count and offsets verified.
- [ ] R10: seven malformed inputs return `InvalidArgument` without panicking.
- [ ] R11: `grpc.rs` and its tests are gone; no source references the gRPC module.

## Notes & Discoveries
