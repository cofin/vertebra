---
type: Task
id: gcp-wire-protocol-correctness:t04
title: BigQuery Storage Read on the official client
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: [gcp-wire-protocol-correctness:t01]
files:
  - src/rs/vertebra-db/src/bigquery.rs
  - src/rs/vertebra-py/src/bigquery.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/bigquery/config.py
tests:
  - src/rs/vertebra-db/tests/bigquery_wire_tests.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: executor
claimed_at: 2026-11-03T19:14:15Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T19:23:19Z
last_verified_commit: a75507c
verification_evidence: "cargo test -p vertebra-db --test bigquery_wire_tests (4 passed), clippy, fmt, pytest test_bigquery.py (9 passed), mypy.stubtest"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T19:23:19Z
commit: null
tags: [rust, gcp, bigquery, arrow, bugfix]
---

# t04 — BigQuery Storage Read on the official client

## Objective

`BigQueryClient::read_table_to_shm_arrow_with_compression` creates the read session and reads every stream through `google_cloud_bigquery::client::Read`, replacing the hand-written encoder/decoders (wrong `ReadSession` and `ReadRowsResponse` field numbers), and the BigQuery `ca_pem` scope (REST only) is documented.

## Context

- Spec R8, R12 (`../spec.md`).
- `regression_tdd`: the test asserts request fields by name through the `Read` stub and the VTBARROW bytes.
- Verified crate APIs (`google-cloud-bigquery 0.19.0`): `google_cloud_bigquery::client::Read::{builder(), from_stub(stub)}`; builder `with_endpoint`, `with_credentials`, `build().await`; `client.create_read_session().set_parent(p).set_read_session(rs).set_max_stream_count(n).send().await -> Result<model::ReadSession>`; `model::ReadSession::new().set_table(t).set_data_format(model::DataFormat::Arrow).set_read_options(model::read_session::TableReadOptions::new().set_selected_fields(v).set_row_restriction(s).set_arrow_serialization_options(model::ArrowSerializationOptions::new().set_buffer_compression(model::arrow_serialization_options::CompressionCodec::{Lz4Frame, Zstd})))`; `ReadSession::{streams, arrow_schema()}` (`serialized_schema: Bytes`); `client.read_rows().set_read_stream(name).set_offset(0).into_reader()` → `read::Reader::next().await -> Option<Result<model::ReadRowsResponse>>` with offset resume; `ReadRowsResponse::{row_count, arrow_record_batch()}` (`serialized_record_batch: Bytes`). Stub trait `google_cloud_bigquery::stub::Read::{create_read_session, read_rows}`.
- The Storage clients expose no custom-root option, so `BigQueryClientConfig.ca_pem` applies to the REST `reqwest` client only and Storage Read/Write use the gRPC trust store (OS roots plus bundled roots from t01).
- VTBARROW: `ShmArrowStreamWriter::{create(name, initial, schema_bytes), append_raw_stream_chunk(bytes, rows), row_count(), finish()}`; region layout is a `ShmArrowFrameHeader::SIZE`-byte header followed by `byte_len` payload bytes.
- `BigQueryConfig.ca_pem` is documented by the field attribute docstring directly below the field in `src/py/vertebra/db/bigquery/config.py`.

## Steps

1. **Write failing tests (RED)** in `src/rs/vertebra-db/tests/bigquery_wire_tests.rs`. Delete `test_bigquery_storage_read_multi_stream_offset_recovery_and_shm_arrow` and the `encode_create_read_session_request` import (used only by that test). Add:
   - `RecordingRead` stub (`#[derive(Clone, Debug, Default)]`, `sessions: Arc<std::sync::Mutex<Vec<model::CreateReadSessionRequest>>>`) implementing `google_cloud_bigquery::stub::Read`: `create_read_session` records and returns `ReadSession { name: "projects/p1/locations/US/sessions/s1", arrow_schema.serialized_schema: b"SCHEMA00", streams: [".../streams/0", ".../streams/1"] }`; `read_rows` returns stream `0`: (`row_count 3`, `b"BATCH-A0"`), (`row_count 2`, `b"BATCH-A1"`); stream `1`: (`row_count 4`, `b"BATCH-B0"`), via `ResponseStream::from(tokio::sync::mpsc::Receiver<google_cloud_gax::Result<ReadRowsResponse>>)`.
   - Test:

     ```rust
     #[tokio::test]
     async fn read_table_writes_schema_then_stream_batches_in_order() {
         let stub = RecordingRead::default();
         let mut cfg = BigQueryClientConfig::new("p1");
         cfg.dataset_id = Some("d1".into());
         let client = BigQueryClient::with_read_client(cfg, Read::from_stub(stub.clone())).expect("client");
         let (shm_name, byte_len, rows, region) = client
             .read_table_to_shm_arrow_with_compression("p1", "d1", "t1", &["id".into(), "name".into()], Some("id > 10"), 2, BigQueryArrowCompression::Zstd)
             .await
             .expect("read");
         assert_eq!(rows, 9);
         let req = stub.sessions.lock().unwrap()[0].clone();
         assert_eq!(req.parent, "projects/p1");
         assert_eq!(req.max_stream_count, 2);
         let session = req.read_session.expect("read_session");
         assert_eq!(session.table, "projects/p1/datasets/d1/tables/t1");
         assert_eq!(session.data_format, model::DataFormat::Arrow);
         let opts = session.read_options.expect("read_options");
         assert_eq!(opts.selected_fields, vec!["id".to_string(), "name".to_string()]);
         assert_eq!(opts.row_restriction, "id > 10");
         assert_eq!(
             opts.arrow_serialization_options().map(|o| o.buffer_compression.clone()),
             Some(model::arrow_serialization_options::CompressionCodec::Zstd)
         );
         // SAFETY: `region` is the live mapping returned by the reader and nothing writes to it concurrently.
         let slice = unsafe { region.as_slice() };
         let header = ShmArrowFrameHeader::from_slice(slice).expect("header");
         assert_eq!(header.byte_len as usize, byte_len);
         let payload = &slice[ShmArrowFrameHeader::SIZE..ShmArrowFrameHeader::SIZE + byte_len];
         let expected: Vec<u8> = [b"SCHEMA00".as_slice(), b"BATCH-A0", b"BATCH-A1", b"BATCH-B0", &ARROW_IPC_EOS_MARKER].concat();
         assert_eq!(payload, expected.as_slice());
         assert!(shm_name.starts_with('/'));
     }
     ```

     All payloads are multiples of 8 bytes, so `ShmArrowStreamWriter` adds no padding.
   - RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests read_table` → compile error (`with_read_client` missing).

2. **Implement (GREEN) in `src/rs/vertebra-db/src/bigquery.rs`.**
   - `BigQueryError`: add `#[error("BigQuery client error: {0}")] Client(#[from] google_cloud_bigquery::Error)`.
   - Fields `injected_read: Option<google_cloud_bigquery::client::Read>`, `read_client: tokio::sync::OnceCell<google_cloud_bigquery::client::Read>`; `pub fn with_read_client(config: BigQueryClientConfig, read: Read) -> Result<Self, BigQueryError>` (same as `new` plus the injected client).
   - `async fn storage_read(&self) -> Result<&Read, BigQueryError>`: `OnceCell` returning the injected client or `install_rustls_ring_provider()` then `Read::builder().with_endpoint(self.config.effective_storage_endpoint()).with_credentials(official_credentials(self.token_cache.clone())).build().await` mapped to `BigQueryError::Http(format!("failed to build BigQuery Read client: {e}"))`. `ca_pem` is not consulted.
   - `read_table_to_shm_arrow_with_compression`: build the request as asserted (`Unspecified` → no `set_arrow_serialization_options`; `Lz4Frame` / `Zstd` map to `CompressionCodec`; omit empty `selected_fields` and `None`/empty `row_restriction`); `max_stream_count = max_streams.max(1)`. Missing `arrow_schema()` → `InvalidArgument("CreateReadSession returned no Arrow schema")`. One `JoinSet` task per stream drains `read_rows().set_read_stream(name).set_offset(0).into_reader()` into `Vec<(Bytes, usize)>` (`serialized_record_batch`, `row_count.max(0) as usize`), skipping responses without an Arrow batch; `JoinError` → `BigQueryError::Http(format!("ReadRows task failed: {e}"))`. Append in stream index order; `finish()`.
   - Delete `encode_create_read_session_request`, `DecodedReadSession`, `decode_read_session_response`, `encode_read_rows_request`, `DecodedReadRowsResponse`, `decode_read_rows_response`, `read_single_storage_stream_with_retry`, `is_transient_grpc_error`, and their imports.
   - `BigQueryClientConfig.ca_pem` doc comment: "PEM roots added to the REST (`jobs.query`/`jobs.insert`) client. The Storage Read/Write gRPC clients use the system trust store plus bundled roots and ignore this value."

3. **Docs in code.** Replace the field attribute docstring under `BigQueryConfig.ca_pem` with `"""PEM root certificates added to the REST API client. The Storage Read/Write API clients use the system trust store and ignore this value."""`, and add a `_vertebra.pyi` `BigQueryClient` class docstring: "``ca_pem`` applies to the REST API client; the Storage Read/Write API clients use the system trust store and ignore it." Update the `PyBigQueryClient` doc comment in `src/rs/vertebra-py/src/bigquery.rs` with the same sentence.

4. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests read_table` → compile error.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests`
  - `cd src/rs && cargo clippy -p vertebra-db -p vertebra-py --all-targets -- -D warnings && cargo fmt --check`
  - `make dev && uv run pytest src/py/tests/unit/db/test_bigquery.py -q && uv run python -m mypy.stubtest vertebra._vertebra`
  - `! grep -rnE "decode_read_rows_response|encode_create_read_session_request|read_single_storage_stream_with_retry" src/rs`
- Commit: `git commit -S -m "fix(rs-db): read BigQuery Storage streams through the official Read client"`.

## Acceptance Criteria

- [x] R8: request fields asserted by name; VTBARROW holds the schema then batches in stream order; row count is the sum of `row_count`.
- [x] R12 (BigQuery): `ca_pem` documented as REST-only in Rust, Python config, and the stub.

## Notes & Discoveries

- `ArrowRecordBatch::set_row_count` is deprecated in the generated `google_cloud_bigquery::model` types because `ReadRowsResponse.row_count` carries the batch row count at the top level.
