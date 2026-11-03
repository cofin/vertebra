---
type: Spec
flow_id: gcp-wire-protocol-correctness
title: Official Google Cloud Rust Clients for Spanner and BigQuery Storage
description: Replace the hand-written Spanner and BigQuery Storage protobuf clients (wrong field numbers, false success, unsafe resends) with googleapis/google-cloud-rust crates behind the existing vertebra-db and _vertebra seams, harden the Arrow IPC splitter, delete grpc.rs, and document the result.
state: active
parent_prd: integration-remediation
research:
  - research/google-cloud-rust_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: 24e2931
state_revision: 3
current_task: gcp-wire-protocol-correctness:t03
last_verified_checkpoint: fadb99e
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-03T18:46:30Z
tags: [rust, gcp, spanner, bigquery, protobuf, bugfix, dependencies]
---

# Flow: Official Google Cloud Rust Clients for Spanner and BigQuery Storage

## 1. Specification & Context

### 1.1 Problem Statement & Objectives

`vertebra-db` talks to Cloud Spanner and the BigQuery Storage API through hand-written protobuf encoders over a hand-written HTTP/2 gRPC pool (`grpc.rs`). The audit (research `audit-findings_20261103.md` sections 4.1–4.3) and the official protos (research `google-cloud-rust_20261103.md`, table "Verified field numbers") show:

- Wrong field numbers in BigQuery `ReadSession`, `ReadRowsResponse`, `AppendRowsRequest`, `AppendRowsResponse`, `StorageError`, and Spanner `Session`, `ExecuteSqlRequest`, `Transaction`, `CommitRequest`. The tests mock the same wrong numbers.
- False success: `commit_read_write_tx` returns `Ok` with no commit timestamp after retry exhaustion; `update_database_ddl` returns `Ok` while the operation is still running; `append_arrow_stream` never checks the number of responses or their offsets.
- Unsafe resends: `grpc.rs::server_streaming_from_lpm_frame` resends any request after a send error; `bidi_streaming` drops encode errors; `execute_streaming_sql` resumes with the original `begin` selector; `execute_in_read_write_tx` lacks the explicit `BeginTransaction` fallback.
- `execute_partitioned_arrow` writes JSON inside fake Arrow frames; `split_arrow_ipc_stream` slices arbitrary bytes when the continuation marker is missing, concatenates record batches into one request, and treats dictionary batches as record batches; `inspect_arrow_message_flatbuffer` turns negative offsets into huge indexes.

The user cleared the official `googleapis/google-cloud-rust` crates when functional. `google-cloud-spanner` `0.34.8-preview` and `google-cloud-bigquery` `0.19.0` cover every operation the Python binding uses. This flow moves both clients onto those crates behind `vertebra_db::spanner::SpannerClient`, `vertebra_db::bigquery::BigQueryClient`, and the `_vertebra` binding, which removes the hand-written protobuf and the unsafe resend logic. Code that stays ours is hardened: the Arrow IPC append plan, append response verification, the commit-timestamp guard, the DDL deadline, the DDL endpoint rule, and the custom-CA policy. A final task documents the new dependencies, configuration, and behavior.

Non-goals:

- Typed exceptions, completion-bridge panic/cancel safety, auth fixes, BigQuery REST fixes, and the GoogleSQL placeholder lexer ([gcp-driver-resilience](../gcp-driver-resilience/spec.md)).
- BigQuery REST migration; replacing `GcpTokenCache` with `google-cloud-auth` credentials; Arrow encoding of Spanner results in Rust.
- Emulator-backed suites (none exist at HEAD; `src/py/tests/integration/` contains only `storage/`). The one emulator-gated Rust test in t03 is `#[ignore = "requires Spanner emulator"]`. When suites are added, emulator-only gaps use `pytest.mark.xfail(strict=True, reason=...)` and `#[ignore = "reason"]`.
- Python `SpannerTransaction.execute` semantics (PRD backlog); module splits and shared helpers (`runtime-structure-and-throughput`).

### 1.2 Upstream Execution Baseline

- Branch: `fix/gcp-wire-protocol` created from `main` (baseline `142cc49`).
- Merge order: `critical-correctness-fixes` merges first; rebase this branch on `main` after it lands and before opening the PR.
- Parent PRD: [integration-remediation](../integration-remediation/spec.md), Chapter 2.
- Prerequisite: crates.io access for the first `cargo fetch`.
- Follow-up: [gcp-driver-resilience](../gcp-driver-resilience/spec.md) starts from this flow's merged branch.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Spanner client | `src/rs/vertebra-db/src/spanner.rs::{SpannerClient, SpannerClientConfig, SpannerError, SpannerValue, SpannerType, ProtobufValue, SpannerQueryParameter, SpannerQueryResult, SpannerMutation, SpannerMutationGroup, SpannerStaleness, PartialResultSetAssembler, encode_execute_sql_request, decode_transaction_message, commit_read_write_tx, update_database_ddl, execute_partitioned_arrow, rewrite_spangres_placeholders}` | `SpannerClient` stays the seam used by `vertebra-py`. Remote-owned: `google_cloud_spanner::client::{Spanner, DatabaseClient, DatabaseAdmin}`; tests inject `Spanner::from_stub` / `DatabaseAdmin::from_stub` (model-level port, bridged via an in-process HTTP/2 `prost::Message` adapter for the two server-streaming RPCs `ExecuteStreamingSql` and `BatchWrite`). Conversions are in-process. |
| BigQuery Storage | `src/rs/vertebra-db/src/bigquery.rs::{BigQueryClient::{read_table_to_shm_arrow_with_compression, append_arrow_stream, normalize_table_resource}, encode_create_read_session_request, decode_read_session_response, decode_read_rows_response, read_single_storage_stream_with_retry, encode_append_rows_request, decode_append_rows_request, decode_append_rows_response, check_batch_commit_response, split_arrow_ipc_stream, try_parse_encapsulated_arrow_stream, inspect_arrow_message_flatbuffer, BigQueryError}` | `BigQueryClient` stays the seam. Remote-owned: `google_cloud_bigquery::client::{Read, Write}`; Read tests inject `Read::from_stub`; the Write plan and verification are pure functions. |
| gRPC pool | `src/rs/vertebra-db/src/grpc.rs`, `src/rs/vertebra-db/tests/grpc_wire_and_pool_tests.rs` | Deleted after both migrations. |
| GCP auth bridge | `src/rs/vertebra-core/src/gcp_auth.rs::{GcpTokenCache, GcpAuthSource, GcpTokenSnapshot}` | New `vertebra_db::gcp::GcpTokenCredentials` (`CredentialsProvider`). In-process. |
| Python binding | `src/rs/vertebra-py/src/spanner.rs::{PySpannerClient, query_result_to_py, execute_partitioned_arrow[_async], update_ddl[_async]}`, `src/rs/vertebra-py/src/bigquery.rs::PyBigQueryClient`, `src/py/vertebra/_vertebra.pyi::{SpannerClient, BigQueryClient}` | `SpannerClient` gains `admin_endpoint` and `partitioned_query[_async]`, loses `execute_partitioned_arrow[_async]`; the query dict drops `transaction_id` / `read_timestamp_micros` (the official `ResultSet` does not expose them; no Python consumer reads them). |
| Python driver/config | `src/py/vertebra/db/spanner/{config.py::SpannerConfig, core.py::SpannerCore.create_rust_client, driver.py::{SyncSpannerDriver.fetch_arrow, AsyncSpannerDriver.fetch_arrow}}`, `src/py/vertebra/db/bigquery/config.py::BigQueryConfig`, `src/py/tests/unit/db/test_spanner.py::FakeSpannerClient` | `SpannerConfig.admin_endpoint`; `fetch_arrow` owns the partitioned-query fallback and builds `ArrowCursor.from_rows`. |
| Docs | `.agents/bundles/product/tech-stack.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `docs/usage/{databases.md,packaging-and-deployment.md}`, `docs/changelog/index.md` | Documentation updated in t06. |

### 1.4 Relevant Patterns, Domain Terms & Knowledge Chapters

- [Product guidelines](../../product/product-guidelines.md), [tech stack](../../product/tech-stack.md), [PyO3 FFI & GIL discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [PyO3 gotchas](../../knowledge/gotchas/pyo3-and-python-runtime.md), [polyglot test matrix](../../knowledge/testing/polyglot-test-matrix.md), [workflow](../../knowledge/workflow.md), [database drivers](../../knowledge/domains/data/database-drivers-and-dialers.md).
- Skills: `.agents/skills/pyo3-bridge/SKILL.md`, `.agents/skills/runtime-ipc/SKILL.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`.
- Terms: **official client** = a `googleapis/google-cloud-rust` crate client; **stub** = an implementation of the crate's `stub::*` trait used as the test port; **append plan** = the ordered schema + record-batch requests derived from one Arrow IPC stream; **admin endpoint** = the REST endpoint used for DDL.

## 2. Requirements & Architecture

### 2.1 Functional Requirements

- **R1**: `vertebra-db` depends on `google-cloud-spanner =0.34.8-preview`, `google-cloud-bigquery =0.19.0`, `google-cloud-auth 1.18`, `google-cloud-gax 1.15`, `google-cloud-longrunning 1.14` with `default-features = false`, plus `tonic 0.14` with `tls-ring` and `tls-webpki-roots` and `futures 0.3`. The dependency tree contains no `google-cloud-monitoring-v3` and no official crate enables `builtin-metrics`, `default-rustls-provider`, or `default-idtoken-backend`. (OpenTelemetry crates remain, unconditionally required by `google-cloud-gax-internal`.)
- **R2**: Official clients authenticate through `vertebra_db::gcp::GcpTokenCredentials` (one shared `GcpTokenCache`) with `authorization: Bearer <token>` and `x-goog-user-project` when a quota project is known; cleartext endpoints without auth options use anonymous credentials. `install_rustls_ring_provider` runs before any official client is built and is idempotent.
- **R3**: `SpannerClient::query` runs a single-use read with the mapped `TimestampBound`; `SpannerClient::partitioned_query` runs `PartitionQuery` with `max_partitions` and Data Boost and concatenates partitions in order, with no fallback inside Rust. Both map parameters to typed official values and return `SpannerQueryResult { columns, schema, rows, row_count_exact, row_count_lower_bound }`.
- **R4**: `SpannerClient::{execute_dml, execute_batch_dml, execute_partitioned_dml, apply_mutations, batch_write}` run through the official transaction APIs; `execute_dml`, `execute_batch_dml`, and `apply_mutations` return `SpannerError::Protocol` when the `CommitResponse` has no `commit_timestamp`.
- **R5**: `SpannerClient::update_database_ddl` builds `DatabaseAdmin` against the admin endpoint (explicit `SpannerClientConfig.admin_endpoint` wins; a `*.googleapis.com` data endpoint uses `https://spanner.googleapis.com`; an `http://` endpoint on port 9010 uses the same host on port 9020; any other endpoint without `admin_endpoint` is `InvalidArgument`), polls `GetOperation` until `done`, returns the operation name, returns `SpannerError::Ddl` with the operation's status on failure, and returns `SpannerError::DdlTimeout` when `ddl_timeout` elapses while `done == false`.
- **R6**: The hand-written Spanner protobuf code is deleted (list in t03).
- **R7**: `_vertebra.SpannerClient.partitioned_query[_async]` replace `execute_partitioned_arrow[_async]`; `SpannerClient(admin_endpoint=...)` and the `admin_endpoint` property exist; `SpannerConfig.admin_endpoint` is passed through; `fetch_arrow` builds `ArrowCursor.from_rows` and falls back to a single-stream query on the errors its fallback predicate accepts.
- **R8**: `read_table_to_shm_arrow_with_compression` reads through `client::Read` and writes `serialized_schema` and each `serialized_record_batch` into VTBARROW in stream order.
- **R9**: `append_arrow_stream` sends one record-batch message per request through `client::Write` and verifies one successful response per request and every non-default offset; row errors return `BigQueryError::RowErrors`; missing or mismatched responses return `BigQueryError::AppendVerification`.
- **R10**: The append plan rejects streams without the continuation marker, without a leading Schema, with dictionary or tensor messages, with record batches larger than `MAX_APPEND_ROWS_CHUNK_BYTES`, truncated messages, and malformed flatbuffers with `BigQueryError::InvalidArgument`, never panicking.
- **R11**: `grpc.rs` and `grpc_wire_and_pool_tests.rs` are deleted; no source references `crate::grpc` or `vertebra_db::grpc`.
- **R12**: Custom CA policy: Spanner `ca_pem` on a non-`*.googleapis.com` endpoint configures `SpannerBuilderExt::with_omni_tls(omni::TlsConfig::new().with_root_certificate_pem(pem))` (which sets `InstanceType::Omni`); on a `*.googleapis.com` endpoint it is `InvalidArgument`, and on a non-googleapis `http://` endpoint it is `InvalidArgument("ca_pem requires an https:// endpoint")`. BigQuery `ca_pem` applies to the REST client only; Storage Read/Write use the gRPC trust store and ignore it. Both `ca_pem` docstrings and `_vertebra.pyi` say so.
- **R13**: Documentation reflects the change: tech stack (new crates, Rust ≥ 1.91 required by the crates, TLS roots), the database-drivers knowledge chapter, `docs/usage/databases.md` (`fetch_arrow` / `partitioned_query`, `admin_endpoint`, `ca_pem`), `docs/usage/packaging-and-deployment.md` (OS trust store for DDL REST), and `docs/changelog/index.md` 0.3.0.

### 2.2 Public Interfaces, Deep Modules & Seam Contracts

`vertebra_db::gcp` (new module):

```rust
pub fn install_rustls_ring_provider();
#[derive(Clone, Debug)]
pub struct GcpTokenCredentials { /* Arc<GcpTokenCache> */ }
impl GcpTokenCredentials { pub fn new(cache: Arc<GcpTokenCache>) -> Self; }
impl google_cloud_auth::credentials::CredentialsProvider for GcpTokenCredentials { /* headers, universe_domain */ }
pub fn official_credentials(cache: Option<Arc<GcpTokenCache>>) -> google_cloud_auth::credentials::Credentials;
```

`vertebra_db::spanner::SpannerClient` (final shape after t03):

```rust
pub fn new(config: SpannerClientConfig) -> Result<Self, SpannerError>;           // sync, no network
pub fn with_clients(spanner: Spanner, admin: Option<DatabaseAdmin>, config: SpannerClientConfig) -> Result<Self, SpannerError>;
pub fn config(&self) -> &SpannerClientConfig;
pub fn effective_admin_endpoint(&self) -> Result<String, SpannerError>;
pub async fn query(&self, sql: &str, params: &[SpannerQueryParameter], staleness: SpannerStaleness) -> Result<SpannerQueryResult, SpannerError>;
pub async fn partitioned_query(&self, sql: &str, params: &[SpannerQueryParameter], max_partitions: i64, data_boost_enabled: bool, staleness: SpannerStaleness) -> Result<SpannerQueryResult, SpannerError>;
pub async fn execute_dml(&self, sql: &str, params: &[SpannerQueryParameter], request_tag: Option<&str>, transaction_tag: Option<&str>) -> Result<i64, SpannerError>;
pub async fn execute_batch_dml(&self, statements: &[SpannerBatchDmlStatement], request_tag: Option<&str>, transaction_tag: Option<&str>) -> Result<Vec<i64>, SpannerError>;
pub async fn execute_partitioned_dml(&self, sql: &str, params: &[SpannerQueryParameter], exclude_txn_from_change_streams: bool) -> Result<i64, SpannerError>;
pub async fn apply_mutations(&self, mutations: &[SpannerMutation], transaction_tag: Option<&str>, exclude_txn_from_change_streams: bool) -> Result<SpannerCommitResult, SpannerError>;
pub async fn batch_write(&self, groups: &[SpannerMutationGroup], exclude_txn_from_change_streams: bool) -> Result<Vec<SpannerBatchWriteResult>, SpannerError>;
pub async fn update_database_ddl(&self, statements: &[String]) -> Result<String, SpannerError>;
```

`SpannerError` (final): `InvalidArgument(String)`, `Auth(String)`, `Client(google_cloud_spanner::Error)`, `Protocol(String)`, `Ddl { operation, code, message }`, `DdlTimeout { operation, timeout_ms }`. `SpannerClientConfig` gains `admin_endpoint: Option<String>` and `ddl_timeout: Duration` (default 600 s). `SpannerQueryResult` drops `transaction_id`, `read_timestamp_micros`, `precommit_token`.

`vertebra_db::bigquery`: `ArrowAppendPlan`, `ArrowAppendRequest`, `BigQueryError::{Client, AppendVerification}` added; `BigQueryError::{Grpc, Proto}` removed; `split_arrow_ipc_stream` replaced by `ArrowAppendPlan::build`; `BigQueryClient::with_read_client` added.

`_vertebra.pyi` `SpannerClient`: `__new__(..., admin_endpoint: str | None = None, ca_pem: bytes | None = None)`, `admin_endpoint` property, `partitioned_query(...) -> dict[str, Any]`, `partitioned_query_async(...) -> RustDbTaskAwaitable`.

### 2.3 Non-Functional & Safety Constraints

- Rust edition 2024; `cargo clippy --all-targets -- -D warnings`; `cargo fmt --check`; no `unwrap`/`expect` on untrusted input; `// SAFETY:` on every `unsafe`.
- PyO3 0.30: blocking calls in `py.detach(|| block_on_db(..))`; no `Python::attach` inside Tokio tasks.
- Python: no inline comments, Google docstrings, PEP 604/695, top-level imports, `pytest.mark.parametrize`.
- gRPC TLS (Spanner data plane, BigQuery Storage): rustls with the ring provider installed by `install_rustls_ring_provider`; roots are the OS store (`tonic` `tls-native-roots`, enabled by the transport) plus bundled Mozilla roots (`tls-webpki-roots`), so minimal images work. Maximum decoded message size is unlimited (`google-cloud-gax-internal` sets `usize::MAX`), which covers 128 MiB frames.
- DDL REST (`DatabaseAdmin`): HTTP/1.1 through `reqwest`; TLS comes from the workspace `reqwest` `rustls` feature (aws-lc-rs with the OS platform verifier, so the image needs a CA bundle); `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` are honored.
- Release builds use `panic = "abort"`.
- Performance: rows pass through `prost_types::Value`; `make bench-check` runs at the final checkpoint.

### 2.4 Acceptance Scenarios (Given / When / Then)

- Given a `GcpTokenCache` with static token `tok-123` and quota project `billing-proj`, when `GcpTokenCredentials` produces headers, then `authorization` is `Bearer tok-123` and `x-goog-user-project` is `billing-proj`.
- Given a stub returning columns `id INT64, name STRING`, rows `("1","a"), ("2","b")`, and stats `row_count_exact = 2`, when `query` runs with `MaxStalenessMs(15000)`, then the result has those columns and rows, `row_count_exact == Some(2)`, and the stub saw a single-use read-only selector.
- Given a stub whose `PartitionQuery` returns two partitions, when `partitioned_query` runs with Data Boost, then both `ExecuteStreamingSql` requests carry `data_boost_enabled = true` and their tokens, and rows are concatenated in partition order; given `PartitionQuery` fails with `INVALID_ARGUMENT`, then `partitioned_query` returns `SpannerError::Client`.
- Given a stub whose `Commit` returns no `commit_timestamp`, when `apply_mutations` runs, then it returns `SpannerError::Protocol`.
- Given an admin stub whose operation never finishes and `ddl_timeout = 200 ms`, when `update_database_ddl` runs, then it returns `SpannerError::DdlTimeout` naming `.../operations/op1`.
- Given endpoint `http://localhost:9010` and no `admin_endpoint`, then `effective_admin_endpoint()` is `http://localhost:9020`; given `http://localhost:7000`, then it is `InvalidArgument`.
- Given `ca_pem` with endpoint `https://spanner.googleapis.com`, then `SpannerClient::new` returns `InvalidArgument`; with `https://omni.internal:15000`, then construction succeeds; with `http://127.0.0.1:9010`, then it returns `InvalidArgument("ca_pem requires an https:// endpoint")`.
- Given a `Read` stub with two streams, when the table is read, then VTBARROW holds the schema then stream 0 then stream 1 batches.
- Given a stream with one schema and three batches in `Pending` mode, then the plan yields offsets `0, r1, r1+r2`; given two responses for three requests, then `verify_responses` returns `AppendVerification`.
- Given malformed Arrow inputs (seven cases), then `ArrowAppendPlan::build` returns `InvalidArgument` without panicking.

### 2.5 Alternatives Considered

- **Keep the hand-written cursor with per-message consts and golden fixtures** (original charter). Smallest dependency footprint and no `prost_types::Value` allocations, but keeps ~6,000 lines of protocol code, the unsafe resend logic, and the session/precommit state machine. Rejected after the user cleared official crates; golden fixtures are not added because no hand-written protobuf remains.
- **Expose official crates directly to `vertebra-py`.** Spreads pre-1.0 types across the binding. Rejected: `SpannerClient`/`BigQueryClient` stay the deep seams.
- **Wire-level mock servers.** Need a protobuf encoder in tests. Rejected in favor of `from_stub` ports and pure append-plan tests.
- **Adopt `google-cloud-auth` credentials.** Loses gcloud fallback, V4 signing key material, and the Python `GcpTokenCache`. Rejected; bridged instead.
- **Rust Arrow IPC writer for partitioned results.** Needs a full Arrow encoder per Spanner type; `fetch_arrow` already builds Arrow from rows. Rejected.
- **Partitioned-query fallback in Rust and Python.** Two owners for one decision. Rejected: Python `fetch_arrow` owns it.
- **`ca_pem` handling.** (a) Reject `ca_pem` everywhere: breaks Spanner Omni and private test servers. (b) Pass `ca_pem` to Cloud Spanner via Omni TLS: switches a Cloud endpoint into `InstanceType::Omni` behavior. (c) Apply BigQuery `ca_pem` to Storage: the Storage clients expose no custom-root option. Chosen: Omni TLS only for non-googleapis Spanner endpoints, explicit rejection on googleapis endpoints, BigQuery `ca_pem` documented as REST-only.
- **DDL through `Spanner::database_admin_builder()`.** Maps `:9010 → :9020` only when `SPANNER_EMULATOR_HOST` is set, so an explicit emulator endpoint sends REST to the gRPC port. Rejected: `admin_endpoint` with a deterministic rule.

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task(s) | Verification Strategy | Confirmed Public Seam & Canonical Test Surface |
| --- | --- | --- | --- |
| R1 | `t01` | `static_validation` | `cargo tree` gates in `t01` |
| R2 | `t01` | `behavior_tdd` | `src/rs/vertebra-db/tests/gcp_credentials_tests.rs` |
| R3, R12 (Spanner) | `t02` | `behavior_tdd` | `src/rs/vertebra-db/tests/spanner_wire_tests.rs::{query_maps_staleness_params_and_decodes_rows, query_rewrites_positional_placeholders_for_googlesql, partitioned_query_sets_data_boost_and_concatenates_partitions, partitioned_query_propagates_partition_errors, ca_pem_policy_follows_endpoint_host}` |
| R4 | `t03` | `regression_tdd` | `spanner_wire_tests.rs::{execute_dml_runs_in_read_write_transaction_with_tags, execute_batch_dml_returns_row_counts, apply_mutations_commits_insert_rows, apply_mutations_rejects_commit_without_timestamp, batch_write_reports_group_status}` |
| R5 | `t03` | `regression_tdd` | `spanner_wire_tests.rs::{update_database_ddl_returns_operation_name, update_database_ddl_surfaces_operation_error, update_database_ddl_times_out_while_operation_pending, admin_endpoint_rule}`; `update_database_ddl_against_emulator` (ignored) |
| R6 | `t03` | `characterization` | grep gates in `t03` |
| R7 | `t02`, `t03` | `behavior_tdd` | `src/py/tests/unit/db/test_spanner.py::test_sync_spanner_arrow_and_object_store_roundtrip`, stubtest |
| R8, R12 (BigQuery) | `t04` | `regression_tdd` | `bigquery_wire_tests.rs::read_table_writes_schema_then_stream_batches_in_order` |
| R9, R10 | `t05` | `regression_tdd` | `bigquery_wire_tests.rs::{append_plan_assigns_offsets_per_record_batch, append_plan_verification_rejects_missing_and_mismatched_offsets, append_plan_rejects_malformed_streams}` |
| R11 | `t05` | `characterization` | grep gates in `t05` |
| R13 | `t06` | `documentation_validation` | `make docs` |

## 4. Implementation Plan

### Phase 1: Foundation

- [x] [fadb99e] t01 — Official crate dependencies and GCP credential bridge (tasks/t01-official-crate-foundation.md)
- **Phase 1 Recovery Checkpoint**: `cd src/rs && cargo test -p vertebra-db --test gcp_credentials_tests && cargo clippy -p vertebra-db --all-targets -- -D warnings && cargo fmt --check`

### Phase 2: Spanner

- [x] t02 — Spanner queries on the official client (tasks/t02-spanner-queries.md)
- [~] t03 — Spanner writes and DDL on the official client; delete hand-written Spanner wire code (tasks/t03-spanner-writes-and-ddl.md)
- **Phase 2 Recovery Checkpoint**: `make lint && cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests && cd ../.. && make dev && uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q && uv run python -m mypy.stubtest vertebra._vertebra`

### Phase 3: BigQuery Storage

- [ ] t04 — BigQuery Storage Read on the official client (tasks/t04-bigquery-storage-read.md)
- [ ] t05 — BigQuery Storage Write, hardened Arrow append plan, grpc.rs removal (tasks/t05-bigquery-storage-write.md)
- **Phase 3 Recovery Checkpoint**: `make lint-all && make rs-test && make py-test && make bench-check`

### Phase 4: Documentation

- [ ] t06 — Document official GCP clients, configuration, and TLS requirements (tasks/t06-documentation.md)
- **Phase 4 Recovery Checkpoint**: `make docs && make lint-all`

## 5. Continuity Snapshot

- **Flow ID**: `gcp-wire-protocol-correctness` (`state: planned`, `plan_revision: 2`, `plan_commit: 24e2931`)
- **Current Task**: `null`
- **Last Verified Checkpoint**: `null`
- **Key Decisions**:
  - Adopt `google-cloud-spanner =0.34.8-preview` and `google-cloud-bigquery =0.19.0` (Read, Write); keep BigQuery REST and `GcpTokenCache` (bridged).
  - Tests use `from_stub` ports and pure append-plan functions; no golden fixtures.
  - `partitioned_query` returns rows; Python `fetch_arrow` owns the fallback and Arrow construction.
  - DDL uses an explicit `admin_endpoint` rule; Spanner `ca_pem` uses Omni TLS for non-googleapis endpoints; BigQuery `ca_pem` is REST-only.
  - One record batch per `AppendRowsRequest`; dictionary batches rejected.
- **Recent Discoveries**:
  - The official runner and write-only transaction do not check `commit_timestamp`.
  - `DatabaseAdmin` is REST over `reqwest` (HTTP/1.1); the emulator serves REST on 9020.
  - The transport sets an unlimited gRPC decode size and uses `with_enabled_roots()`.
  - OpenTelemetry crates are unconditional dependencies of `google-cloud-gax-internal`.
  - Release builds abort on panic.
