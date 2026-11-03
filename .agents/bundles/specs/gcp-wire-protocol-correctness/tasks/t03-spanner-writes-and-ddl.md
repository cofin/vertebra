---
type: Task
id: gcp-wire-protocol-correctness:t03
title: Spanner writes and DDL on the official client; delete hand-written Spanner wire code
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: [gcp-wire-protocol-correctness:t02]
files:
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/src/spanner.rs
  - src/rs/vertebra-py/src/spanner.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/spanner/config.py
  - src/py/vertebra/db/spanner/core.py
tests:
  - src/rs/vertebra-db/tests/spanner_wire_tests.rs
  - src/py/tests/unit/db/test_spanner.py
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
tags: [rust, gcp, spanner, pyo3, bugfix]
---

# t03 — Spanner writes and DDL on the official client; delete hand-written Spanner wire code

## Objective

Move `execute_dml`, `execute_batch_dml`, `execute_partitioned_dml`, `apply_mutations`, `batch_write`, and `update_database_ddl` onto the official Spanner client with the commit-timestamp guard, the DDL deadline, and the `admin_endpoint` rule; expose `admin_endpoint` through Python; delete every hand-written Spanner protobuf path.

## Context

- Spec R4, R5, R6, R7 (`../spec.md`). Defects removed: false success after `MultiplexedSessionRetry` exhaustion, DDL `Ok` while `done == false`, resume with the original `begin` selector, missing explicit `BeginTransaction` fallback, wrong `creator_role`/`request_options`/`precommit_token` field numbers.
- `regression_tdd` through the `SpannerClient` seam with the stub ports (`Spanner::from_stub` and `DatabaseAdmin::from_stub` for unary RPCs, and the in-process HTTP/2 `prost::Message` adapter delegating to `RecordingSpanner` for `BatchWrite` server-streaming).
- Verified crate behavior: the transaction runner and `write_only_transaction().write()` return the `CommitResponse` as received and never check `commit_timestamp` (no such check exists outside tests in the crate source), so the guard here produces `SpannerError::Protocol`.
- Verified crate APIs: `db.read_write_transaction()` → `TransactionRunnerBuilder::{set_transaction_tag, set_max_commit_delay(Duration), set_exclude_txn_from_change_streams}` → `.build().run(async |tx| ..).await -> Result<google_cloud_spanner::transaction::TransactionResult<T>>` (`result`, `commit_response`); `transaction::ReadWriteTransaction::{execute_update(stmt) -> Result<i64>, execute_batch_update(batch::BatchDml) -> Result<Vec<i64>>}` (RPCs `ExecuteSql`, `ExecuteBatchDml`, `Commit`, `BeginTransaction`); `google_cloud_spanner::batch::BatchDml::builder().add_statement(stmt).set_request_tag(tag).build()`; `Statement::builder(sql).set_request_tag(tag)`; `db.write_only_transaction().set_transaction_tag(..).set_max_commit_delay(..).set_exclude_txn_from_change_streams(..).build().write(mutations) -> Result<model::CommitResponse>`; `db.partitioned_dml_transaction().set_exclude_txn_from_change_streams(b).build().execute_update(stmt) -> Result<i64>`; `db.batch_write_transaction().set_exclude_txn_from_change_streams(b).build().execute_streaming(impl IntoIterator<Item = mutation::MutationGroup>) -> Result<BatchWriteResponseStream>` (`next().await -> Option<Result<model::BatchWriteResponse { indexes, status, commit_timestamp }>>`); `mutation::Mutation::{new_insert_builder, new_update_builder, new_insert_or_update_builder, new_replace_builder}(table).set(col).to(value)...build()`, `Mutation::delete(table, key::KeySet)`, `key::KeySet::{all(), builder().add_key(key::Key::new(Vec<value::Value>)).build()}`, `mutation::MutationGroup::new(Vec<Mutation>)`.
- DDL: `google_cloud_spanner::client::DatabaseAdmin` is REST over `reqwest` (HTTP/1.1, OS trust store, environment proxies). Build it with `DatabaseAdmin::builder().with_endpoint(admin_endpoint).with_credentials(official_credentials(token_cache)).build().await`; test seam `DatabaseAdmin::from_stub(impl google_cloud_spanner_admin_database_v1::stub::DatabaseAdmin)`. `admin.update_database_ddl().set_database(db).set_statements(stmts).send().await -> Result<google_cloud_longrunning::model::Operation>`; `admin.get_operation().set_name(name).send().await -> Result<Operation>`; `Operation { name, done, result: Option<google_cloud_longrunning::model::operation::Result> }` with `operation::Result::Error(Box<google_cloud_rpc::model::Status { code: i32, message: String, .. }>)`. The Cloud Spanner emulator serves gRPC on 9010 and REST on 9020.
- Python: `SpannerConfig` (`src/py/vertebra/db/spanner/config.py`, msgspec struct; each field carries an attribute docstring on the line below it); `SpannerCore.create_rust_client` (`src/py/vertebra/db/spanner/core.py`) passes config fields to `_RustSpannerClient`.

## Steps

1. **Write failing tests (RED)** in `src/rs/vertebra-db/tests/spanner_wire_tests.rs`:
   - Extend `RecordingSpanner` with `execute_sql` (records; returns `model::ResultSet` with `metadata.transaction.id = b"rw-tx"` and `stats` `row_count_exact = 3`), `execute_batch_dml` (two result sets with `row_count_exact` 2 and 5, OK `status`), `begin_transaction` (`id = b"rw-tx"`), `commit` (records; `CommitResponse` with `commit_timestamp = Timestamp::new(1_700_000_000, 0)` unless `omit_commit_timestamp: bool`), `batch_write` (stream: indexes `[0]` status code 0 with timestamp; indexes `[1]` status code 6 message `"already exists"`).
   - `RecordingAdmin` implementing `google_cloud_spanner_admin_database_v1::stub::DatabaseAdmin`: `update_database_ddl` records statements and returns `Operation { name: "projects/p/instances/i/databases/d/operations/op1", done: false }`; `get_operation` returns per a `mode` field: `NeverDone` → `done: false`; `Succeed` → `done: true`; `Fail` → `done: true` with `Error(Status { code: 3, message: "Duplicate name in schema: t" })`.
   - Helper `stub_client_with_admin(stub, admin, ddl_timeout: Duration) -> SpannerClient` via `SpannerClient::with_clients(Spanner::from_stub(stub), Some(DatabaseAdmin::from_stub(admin)), cfg)` with `cfg.ddl_timeout = ddl_timeout`.
   - Tests:
     - `execute_dml_runs_in_read_write_transaction_with_tags`: `execute_dml("UPDATE t SET v = 1 WHERE id = @p1", &[p1 = Int64(1)], Some("req-a"), Some("tx-a"))` → `3`; recorded `ExecuteSqlRequest.request_options.request_tag == "req-a"`; recorded `CommitRequest.request_options.transaction_tag == "tx-a"`.
     - `execute_batch_dml_returns_row_counts` → `vec![2, 5]`.
     - `apply_mutations_commits_insert_rows`: one insert on `t` (`["id","v"]`, one row) → `commit_timestamp_micros == Some(1_700_000_000_000_000)`; recorded `CommitRequest.mutations` has one `insert` on `t` with those columns.
     - `apply_mutations_rejects_commit_without_timestamp`: `omit_commit_timestamp = true` → `Err(SpannerError::Protocol(msg))` with `msg.contains("commit_timestamp")`.
     - `batch_write_reports_group_status` → `[{indexes: [0], status_code: 0}, {indexes: [1], status_code: 6, status_message: "already exists"}]`.
     - `update_database_ddl_returns_operation_name` (`Succeed`), `update_database_ddl_surfaces_operation_error` (`Fail` → `SpannerError::Ddl { code: 3, .. }`), `update_database_ddl_times_out_while_operation_pending` (`NeverDone`, `ddl_timeout = 200 ms` → `SpannerError::DdlTimeout { operation, timeout_ms: 200 }`, `operation.ends_with("/operations/op1")`, finishes in under 5 s).
     - `admin_endpoint_rule` (sync `#[test]`): explicit `admin_endpoint = Some("http://admin.local:8080")` wins; default endpoint → `"https://spanner.googleapis.com"`; `endpoint = "http://localhost:9010"` → `"http://localhost:9020"`; `endpoint = "http://localhost:7000"` → `SpannerError::InvalidArgument`.
     - `update_database_ddl_against_emulator` with `#[ignore = "requires Spanner emulator: set SPANNER_EMULATOR_HOST and VERTEBRA_SPANNER_EMULATOR_DATABASE"]`: reads (never sets) `SPANNER_EMULATOR_HOST` and `VERTEBRA_SPANNER_EMULATOR_DATABASE`, builds `SpannerClient::new` with `endpoint = format!("http://{host}")`, runs `CREATE TABLE vtb_ddl_probe_<unix_ms> (id INT64) PRIMARY KEY (id)`, and asserts a non-empty operation name.
   - Workspace/dev dependencies: add `google-cloud-spanner-admin-database-v1 = { version = "1.15", default-features = false }`, `google-cloud-rpc = { version = "1.6", default-features = false }`, `prost = "0.14"`, and `prost-types = "0.14"` to `[workspace.dependencies]`, and add them plus `google-cloud-longrunning` to `vertebra-db` `[dev-dependencies]` as `{ workspace = true }`.
   - In `src/py/tests/unit/db/test_spanner.py` add `test_spanner_config_passes_admin_endpoint` asserting `SpannerCore.create_rust_client(SpannerConfig(project_id="p", instance_id="i", database_id="d", endpoint="http://localhost:9010", admin_endpoint="http://localhost:9020")).admin_endpoint == "http://localhost:9020"`.
   - RED: `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests` → compile errors (`ddl_timeout`, `admin_endpoint`, `execute_dml`, `SpannerError::Ddl`); `uv run pytest src/py/tests/unit/db/test_spanner.py -k admin_endpoint -q` → `TypeError` (unknown field).

2. **Implement (GREEN) in `src/rs/vertebra-db/src/spanner.rs`.**
   - Config: `pub const DEFAULT_DDL_TIMEOUT: Duration = Duration::from_secs(600);`, `pub const DEFAULT_SPANNER_ADMIN_ENDPOINT: &str = "https://spanner.googleapis.com";`, fields `admin_endpoint: Option<String>` (doc: REST endpoint for DDL; derived when absent) and `ddl_timeout: Duration` (default `DEFAULT_DDL_TIMEOUT`).
   - `pub fn effective_admin_endpoint(&self) -> Result<String, SpannerError>` on `SpannerClient`: non-empty `config.admin_endpoint` → it; `endpoint_is_googleapis(&effective_endpoint)` → `DEFAULT_SPANNER_ADMIN_ENDPOINT`; `http://` endpoint whose `Url::port() == Some(9010)` → same URL with port 9020 (`set_port(Some(9020))`), trailing `/` trimmed; otherwise `InvalidArgument(format!("set SpannerClientConfig.admin_endpoint: the DDL REST endpoint cannot be derived from {endpoint}"))`.
   - `SpannerError` final variants: `InvalidArgument(String)`, `Auth(String)`, `Client(#[from] google_cloud_spanner::Error)`, `Protocol(String)`, `#[error("UpdateDatabaseDdl operation {operation} failed with code {code}: {message}")] Ddl { operation: String, code: i32, message: String }`, `#[error("UpdateDatabaseDdl operation {operation} did not finish within {timeout_ms} ms; it may still be running")] DdlTimeout { operation: String, timeout_ms: u64 }`. Delete `Grpc`, `Shm`, `From<GrpcStatus>`, `From<ProtoError>`.
   - `async fn database_admin(&self) -> Result<&DatabaseAdmin, SpannerError>`: `OnceCell` returning the injected admin or `DatabaseAdmin::builder().with_endpoint(self.effective_admin_endpoint()?).with_credentials(official_credentials(self.token_cache.clone())).build().await` mapped to `SpannerError::Protocol(format!("failed to build DatabaseAdmin: {e}"))`.
   - `fn official_mutation_rows(m: &SpannerMutation) -> Vec<Mutation>`: write kinds → one official mutation per row with the matching `new_*_builder(table)` and `.set(column).to(official_value(cell))` per column/cell; `Delete { table, keys, all }` → `Mutation::delete(table, if all { KeySet::all() } else { keys.iter().fold(KeySet::builder(), |b, k| b.add_key(Key::new(k.iter().map(official_value).collect()))).build() })`.
   - `fn commit_result(resp: &spanner_model::CommitResponse) -> Result<SpannerCommitResult, SpannerError>`: no `commit_timestamp` → `SpannerError::Protocol("Commit returned no commit_timestamp; the transaction outcome is unknown".into())`; micros = `seconds * 1_000_000 + nanos / 1_000`; `mutation_count = resp.commit_stats.as_ref().map(|s| s.mutation_count)`; `retry_count = 0`.
   - `execute_dml`: runner with `set_transaction_tag` (when given), `set_max_commit_delay(Duration::from_millis(ms))` (when `config.max_commit_delay_ms` is set), `run(async |tx| tx.execute_update(stmt.clone()).await)` where `stmt` carries `request_tag`; `commit_result(&outcome.commit_response)?`; return `outcome.result`.
   - `execute_batch_dml`: empty → `Ok(vec![])`; otherwise the same runner with `tx.execute_batch_update(batch.clone())`; commit guard.
   - `execute_partitioned_dml`: `partitioned_dml_transaction().set_exclude_txn_from_change_streams(b).build().execute_update(stmt).await?`.
   - `apply_mutations`: empty → `SpannerCommitResult { commit_timestamp_micros: None, mutation_count: Some(0), retry_count: 0 }` without an RPC; otherwise `write_only_transaction()` with tag, max commit delay, exclude flag, `.write(mutations.iter().flat_map(official_mutation_rows))`, then `commit_result`.
   - `batch_write`: chunks whose summed `cell_count()` stays `<= MAX_MUTATION_CELLS_PER_BATCH` (a larger single group is its own chunk); per chunk `execute_streaming(groups mapped to MutationGroup::new(flat_map(official_mutation_rows)))`, drain, map to `SpannerBatchWriteResult { indexes: index + chunk_start, status_code, status_message, commit_timestamp_micros }`.
   - `update_database_ddl`: empty → `Ok(String::new())`; `deadline = Instant::now() + config.ddl_timeout`; send; loop: `done` → `Ok(op.name)` or `Err(Ddl { operation, code, message })` for `Error(status)`; past deadline → `Err(DdlTimeout { operation, timeout_ms })`; sleep `min(backoff, deadline - now)` (100 ms doubling to 5 s); refresh with `get_operation`.
   - `SpannerQueryResult`: remove `transaction_id`, `read_timestamp_micros`, `precommit_token`.
   - Delete: `SpannerClient::{get_or_create_multiplexed_session, is_session_not_found, begin_transaction, execute_streaming_sql, execute_in_read_write_tx, commit_read_write_tx, rollback, run_in_read_write_transaction, batch_write_chunk, bearer_token_and_quota, build_metadata, next_request_sequence}`; fields `grpc_pool`, `cached_session`, `client_id`, `rand64_hex`, `req_seq`; types `SpannerSessionHandle`, `SpannerReadWriteTransaction`, `SpannerTransactionOptions`, `SpannerTransactionSelector`, `MultiplexedSessionPrecommitToken`, `PartialResultSetFrame`, `PartialResultSetAssembler`, `DecodedResultSetMetadata`, `DecodedTransactionMessage`, `DecodedBatchDmlResultSet`, `DecodedCommitResponse`; functions `update_highest_precommit_token`, `encode_duration_ms`, `encode_timestamp_micros`, `decode_timestamp_micros`, `encode_struct_type_fields`, `decode_struct_type_fields`, `encode_protobuf_struct`, `decode_protobuf_struct`, `encode_protobuf_list_value`, `decode_protobuf_list_value`, `encode_execute_sql_request`, `decode_result_set_metadata`, `decode_transaction_message`, `decode_result_set_stats`, `decode_batch_dml_result_set`, `decode_commit_response`, `decode_batch_write_response`, `decode_lro_operation`, `decode_embedded_rpc_status`, `extract_aborted_retry_delay_ms`, `current_unix_ms`; methods `SpannerType::{encode, decode}`, `ProtobufValue::{encode, decode, estimated_byte_size, merge_chunked}`, `SpannerMutation::encode`, `SpannerMutationGroup::encode`, `SpannerStaleness::encode_read_only`; constants `MAX_UNCHECKPOINTED_RESUME_BYTES`, `MULTIPLEXED_SESSION_ROTATION_MS`, `NEXT_SPANNER_CLIENT_ID`; every `crate::grpc` import.
   - Test file cleanup in `spanner_wire_tests.rs`: delete `test_partial_result_set_chunk_merger_split_bytes_json_and_nested_list`, `test_read_write_transaction_inline_begin_precommit_retry_and_aborted_wound_wait`, struct `RecordedRpc`, fns `strip_lpm_prefix`, `grpc_ok_response`, `grpc_error_response`, `encode_retry_info_status_bin`, `start_mock_spanner_h2c_server`; imports `vertebra_db::grpc::{DEFAULT_CLIENT_MAX_GRPC_MESSAGE_SIZE, ProtoReader, ProtoWriter, WireType, encode_lpm_frame}`, `MultiplexedSessionPrecommitToken`, `PartialResultSetAssembler`, `PartialResultSetFrame`, `SpannerReadWriteTransaction` from `vertebra_db::spanner`, and the imports only those items used: `base64::Engine as _`, `BASE64_STANDARD`, `std::net::SocketAddr`, `std::sync::atomic::{AtomicUsize, Ordering}`, `HeaderMap`. Keep `test_spangres_placeholder_rewriter`, the `prost`-backed streaming adapter for `ExecuteStreamingSql` and `BatchWrite`, and the stub-based tests.

3. **Binding (`src/rs/vertebra-py/src/spanner.rs`).** Constructor signature gains `admin_endpoint=None` (before `ca_pem`), stored in `cfg.admin_endpoint` when non-empty; add `#[getter] fn admin_endpoint(&self) -> Option<&str>`. `execute_dml[_async]`, `execute_batch_dml[_async]`, `execute_partitioned_dml[_async]`, `apply_mutations[_async]`, `batch_write[_async]`, `update_ddl[_async]` call the matching `SpannerClient` method directly; sync variants keep `py.detach(|| block_on_db(..))` and `PyRuntimeError::new_err(e.to_string())`. Delete `let _ = SpannerTypeCode::Unspecified;` and its import.

4. **Python.**
   - `_vertebra.pyi` `SpannerClient.__new__`: add `admin_endpoint: str | None = None,` before `ca_pem`; add `@property def admin_endpoint(self) -> str | None: ...`.
   - `SpannerConfig`: field `admin_endpoint: str | None = None` after `endpoint`, followed by the field attribute docstring `"""REST endpoint for DDL (`UpdateDatabaseDdl`). Defaults to `https://spanner.googleapis.com` for Google endpoints and to port 9020 of an `http://…:9010` emulator endpoint; required for any other endpoint."""`.
   - `SpannerCore.create_rust_client`: pass `admin_endpoint=config.admin_endpoint`.

5. **Refactor & verify.**

## Verification

- RED: commands in Step 1 fail as described.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests`
  - `cd src/rs && cargo clippy -p vertebra-db -p vertebra-py --all-targets -- -D warnings && cargo fmt --check`
  - `make dev && uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q`
  - `uv run python -m mypy.stubtest vertebra._vertebra`
  - `! grep -nE "crate::grpc|ProtoWriter|ProtoReader|PartialResultSetAssembler|MultiplexedSessionPrecommitToken" src/rs/vertebra-db/src/spanner.rs src/rs/vertebra-db/tests/spanner_wire_tests.rs`
- Phase gate: `make lint && make rs-test`.
- Commit: `git commit -S -m "fix(rs-db): move Spanner writes and DDL to the official client and remove hand-written wire code"`.

## Acceptance Criteria

- [ ] R4: DML, batch DML, PDML, mutations, BatchWrite on official APIs; a missing commit timestamp is `SpannerError::Protocol`.
- [ ] R5: DDL honors `admin_endpoint` rule, returns the operation name, `Ddl` on failure, `DdlTimeout` past the deadline; emulator test is present and ignored by default.
- [ ] R6: hand-written Spanner wire code and its tests are gone.
- [ ] R7: `admin_endpoint` flows from `SpannerConfig` to the binding and stub.

## Notes & Discoveries
