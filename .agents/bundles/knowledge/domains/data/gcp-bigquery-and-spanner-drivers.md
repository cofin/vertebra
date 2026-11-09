---
type: Guide
title: GCP BigQuery & Spanner Native Drivers
description: Standalone free-threaded PyO3 BigQuery and Spanner driver crates, zero-copy VTBARROW and AppendRows framing, shared multicloud ObjectStoreStaging, and adapter ToolProvider contracts.
status: stable
tags:
  - domains
  - database
  - bigquery
  - spanner
  - gcp
  - arrow
  - shm
generated:
  by: flow/0.26.0
  at: "2026-11-07T03:55:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
sources:
  - id: src-vertebra-core-src-gcp-auth-rs
    resource: src/rs/vertebra-core/src/gcp_auth.rs
    title: Shared GcpTokenCache, ProxyPolicy, and GcloudCli ADC provider
  - id: src-vertebra-db-src-gcp-rs
    resource: src/rs/vertebra-db/src/gcp.rs
    title: Optional gcp feature credential adapter and error classification
  - id: src-vertebra-db-src-py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: 5-argument spawn_rust_db_task_with_attr and ShmCapsule bridge
  - id: src-vertebra-py-src-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs
    title: Multi-base BigQueryError and SpannerError exception definitions
  - id: src-vertebra-bigquery-src-lib-rs
    resource: src/rs/vertebra-bigquery/src/lib.rs
    title: gil_used = false PyO3 vertebra._bigquery extension entrypoint
  - id: src-vertebra-bigquery-src-client-rs
    resource: src/rs/vertebra-bigquery/src/client.rs
    title: BigQuery REST v2 and Storage Read/Write v1 client
  - id: src-vertebra-bigquery-src-errors-rs
    resource: src/rs/vertebra-bigquery/src/errors.rs
    title: PyOnceLock BigQueryError and BigQueryRowError exception bridge
  - id: src-vertebra-bigquery-src-arrow-ipc-rs
    resource: src/rs/vertebra-bigquery/src/arrow_ipc.rs
    title: Unregistered VTBARROW writers and zero-copy ArrowAppendPlan framing
  - id: src-vertebra-bigquery-src-codec-rs
    resource: src/rs/vertebra-bigquery/src/codec.rs
    title: BigQueryWriteMode, GoogleSQL parameters, and REST error checks
  - id: src-vertebra-spanner-src-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs
    title: gil_used = false PyO3 vertebra._spanner extension entrypoint
  - id: src-vertebra-spanner-src-client-rs
    resource: src/rs/vertebra-spanner/src/client.rs
    title: Multiplexed Spanner gRPC client and REST DatabaseAdmin DDL LROs
  - id: src-vertebra-spanner-src-errors-rs
    resource: src/rs/vertebra-spanner/src/errors.rs
    title: PyOnceLock SpannerError and SpannerAbortedError exception bridge
  - id: src-vertebra-spanner-src-arrow-ipc-rs
    resource: src/rs/vertebra-spanner/src/arrow_ipc.rs
    title: Unregistered /vtb-sp-* VTBARROW cursor writer
  - id: src-vertebra-spanner-src-codec-rs
    resource: src/rs/vertebra-spanner/src/codec.rs
    title: SpannerDialect, mutations, staleness bounds, and Spangres placeholders
  - id: src-db-staging-py
    resource: src/py/vertebra/db/_staging.py
    title: Shared multicloud ObjectStoreStaging cursor serialization and URI resolution
  - id: src-db-bigquery-init-py
    resource: src/py/vertebra/db/adapters/bigquery/__init__.py
    title: BigQueryConfig, BigQueryCore, and Sync/AsyncBigQueryDriver exports
  - id: src-db-bigquery-tools-py
    resource: src/py/vertebra/db/adapters/bigquery/tools.py
    title: BigQueryToolProvider operations and billing-cap enforcement
  - id: src-db-spanner-init-py
    resource: src/py/vertebra/db/adapters/spanner/__init__.py
    title: SpannerConfig, SpannerCore, and Sync/AsyncSpannerDriver exports
  - id: src-db-spanner-tools-py
    resource: src/py/vertebra/db/adapters/spanner/tools.py
    title: SpannerToolProvider operations and staleness snapshot policies
  - id: src-typing-public-py
    resource: src/py/vertebra/typing.py
    title: Neutral ToolProvider protocol and ToolPolicyError exception
---

# GCP BigQuery & Spanner Native Drivers

<!-- truth: start -->
- **Standalone `gil_used = false` Optional Wheels (`vertebra-bigquery`, `vertebra-spanner`, `vertebra-db/src/gcp.rs`)**: `vertebra-bigquery` (`vertebra._bigquery`, `[bigquery]`, `google-cloud-bigquery =0.19.0`) and `vertebra-spanner` (`vertebra._spanner`, `[spanner]`, `google-cloud-spanner =0.34.8-preview`) compile as optional PyO3 wheels over `vertebra_db::gcp` (`feature = "gcp"`), keeping `vertebra-py` and default `vertebra-db` free of `google-cloud-*` and `tonic`.
- **5-Module Crate Decomposition (`{lib,client,errors,arrow_ipc,codec}.rs`)**: Both `vertebra-bigquery` and `vertebra-spanner` split PyO3 bindings (`lib.rs`), gRPC/REST transport (`client.rs`), lazy `PyOnceLock` exception resolution (`errors.rs`), unregistered `VTBARROW` / IPC framing (`arrow_ipc.rs`), and SQL/mutation codecs (`codec.rs`).
- **Zero-Copy `ArrowAppendPlan` Framing (`vertebra-bigquery/src/arrow_ipc.rs`)**: `ArrowAppendPlan::build` / `ArrowAppendPlan::build_bytes(stream: Bytes, mode: BigQueryWriteMode)` slices schema and record-batch frames zero-copy via `Bytes::slice`, rejects dictionary batches (`header_type == 2`) and frames exceeding `MAX_APPEND_ROWS_CHUNK_BYTES` (`9 MiB`), and validates response counts and stream offsets in `verify_responses`.
- **5-Argument Async Drainer & Cross-`.so` `ShmCapsule` (`vertebra-db/src/py_async.rs`)**: `spawn_rust_db_task_with_attr(py, fut, map_ok, map_err, attr_name)` isolates `_vertebra_bigquery_completion_drainer` and `_vertebra_spanner_completion_drainer` on the active `asyncio` loop, while `construct_py_arrow_cursor` transfers unregistered `Arc<ShmRegion>` segments across `.so` boundaries via `c"vertebra.ShmCapsule"`.
- **Shared Multicloud `ObjectStoreStaging` (`src/py/vertebra/db/_staging.py`)**: `ObjectStoreStaging` validates targets (`validate_staging_target`, `validate_gcs_uri`, rejecting implicit `memory://` URIs) and streams `ArrowCursor` exports/loads (`write_cursor_sync`/`write_cursor_async`, `read_cursor_sync`/`read_cursor_async`) across `SyncObjectStore`, `AsyncObjectStore`, and `ObjectStoreBackend` (`parquet`, `arrow-ipc`, `csv`, `jsonl`).
<!-- truth: end -->

`vertebra-bigquery` and `vertebra-spanner` implement Vertebra's standalone Rust-native Google Cloud analytical and distributed SQL drivers, integrating with the base contracts in [Database Drivers & Cloud Dialers](database-drivers-and-dialers.md), multicloud [Object Storage](../storage/object-storage.md), and the wheel ownership boundaries recorded in [ADR-004](../../decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## Shared GCP Auth, Async Drainer & Exception Bridge

- **Scoped ADC & Transport Classification (`gcp_auth.rs`, `gcp.rs`)**: [`GcpTokenCache::from_options(raw_opts, default_scopes)`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs)[^src-vertebra-core-src-gcp-auth-rs] requires explicit OAuth scopes, applies `ProxyPolicy` to token/IAM endpoints while keeping metadata-server requests direct, and runs `GcpAuthSource::GcloudCli` with `kill_on_drop(true)` and `GCLOUD_CLI_TIMEOUT` (`30s`). [`vertebra_db::gcp`](../../../../../src/rs/vertebra-db/src/gcp.rs)[^src-vertebra-db-src-gcp-rs] adapts `GcpTokenCache` to `google-cloud-auth` `CredentialsProvider` and normalizes HTTP status codes before generic transport fallbacks.
- **5-Argument Completion Drainer & `ShmCapsule` (`py_async.rs`)**: [`spawn_rust_db_task_with_attr(py, fut, map_ok, map_err, attr_name)`](../../../../../src/rs/vertebra-db/src/py_async.rs)[^src-vertebra-db-src-py-async-rs] registers per-extension `AsyncCompletionQueue` drainers (`_vertebra_bigquery_completion_drainer` and `_vertebra_spanner_completion_drainer`), binds Python `Future.cancel()` to Tokio `AbortHandle`, and pairs with `construct_py_arrow_cursor` (`c"vertebra.ShmCapsule"`) so optional `.so` crates never retain leaked `ShmRegion` entries.
- **Multi-Base Exceptions (`vertebra-py/src/errors.rs`)**: [`vertebra_py::errors`](../../../../../src/rs/vertebra-py/src/errors.rs)[^src-vertebra-py-src-errors-rs] defines `BigQueryError(GoogleCloudError, DatabaseError)`, `BigQueryRowError(BigQueryError, DataError)`, `SpannerError(GoogleCloudError, DatabaseError)`, and `SpannerAbortedError(SpannerError, SerializationFailure)` (`sqlstate = "40001"`), resolved lazily via `PyOnceLock` in each wheel's `errors.rs` (falling back to standalone types in Rust-only unit tests).

## `vertebra-bigquery` Submodules & Wire Pipeline (`vertebra._bigquery`)

[`adapters/bigquery/`](../../../../../src/py/vertebra/db/adapters/bigquery/__init__.py)[^src-db-bigquery-init-py] (`BigQueryConfig`, `BigQueryCore`, `SyncBigQueryDriver`, `AsyncBigQueryDriver`) wraps the five-module `vertebra-bigquery` crate:

| Module | Key Types & Responsibilities |
|---|---|
| [`lib.rs`](../../../../../src/rs/vertebra-bigquery/src/lib.rs)[^src-vertebra-bigquery-src-lib-rs] | `#[pymodule(gil_used = false)]` `vertebra._bigquery` exposing `BigQueryClient` (`query`, `query_async`, `read_table_arrow`, `read_table_arrow_async`, `append_arrow`, `append_arrow_async`, `dry_run`, `dry_run_async`) with GIL detachment and `Bytes` buffer extraction. |
| [`client.rs`](../../../../../src/rs/vertebra-bigquery/src/client.rs)[^src-vertebra-bigquery-src-client-rs] | REST v2 (`jobs.query`, `jobs.getQueryResults` URL-encoded via `url::Url`, `jobs.insert` dry-run, optional `ca_pem` custom CA) + Storage Read/Write v1 (`client::Read` `CreateReadSession`/`ReadRows`, `client::Write` `AppendRows` across `Default`, `Committed`, `Pending`, and `Buffered` streams). `BigQueryCore.is_storage_api_fallback_error` falls back to `jobs.query` on `BigQueryError.code in {7, 9, 12, 14}`. |
| [`arrow_ipc.rs`](../../../../../src/rs/vertebra-bigquery/src/arrow_ipc.rs)[^src-vertebra-bigquery-src-arrow-ipc-rs] | Streams `ReadRows` batches into unregistered `/vtb-bq-*` `ShmRegion` segments (`write_read_rows_batches_to_shm` -> `ShmArrowStreamWriter::finish_unregistered`). `ArrowAppendPlan::build` delegates to `ArrowAppendPlan::build_bytes(stream: Bytes, mode: BigQueryWriteMode)`, slicing schema and record-batch frames zero-copy via `stream.slice(pos..frame_end)` (`Bytes::slice`), rejecting dictionary batches (`header_type == 2`) and batches above `MAX_APPEND_ROWS_CHUNK_BYTES` (`9 MiB`), and verifying response counts and offsets in `verify_responses`. |
| [`codec.rs`](../../../../../src/rs/vertebra-bigquery/src/codec.rs)[^src-vertebra-bigquery-src-codec-rs] | `BigQueryWriteMode`, typed GoogleSQL parameter encoding (`BigQueryParamValue`), row decoding, and `check_rest_error` (failing only on `error` or `status.errorResult` so `errors[]` warnings succeed). |
| [`errors.rs`](../../../../../src/rs/vertebra-bigquery/src/errors.rs)[^src-vertebra-bigquery-src-errors-rs] | `PyOnceLock` lookup of `GoogleCloudAuthError`, `BigQueryError`, and `BigQueryRowError`, attaching `code`, `code_name`, `http_status`, `retry_delay_ms`, `reason`, `vendor_code`, `sqlstate`, and `is_transient`. |

## `vertebra-spanner` Submodules & Wire Pipeline (`vertebra._spanner`)

[`adapters/spanner/`](../../../../../src/py/vertebra/db/adapters/spanner/__init__.py)[^src-db-spanner-init-py] (`SpannerConfig`, `SpannerCore`, `SyncSpannerDriver`, `AsyncSpannerDriver`) wraps the five-module `vertebra-spanner` crate across GoogleSQL and Spangres dialects:

| Module | Key Types & Responsibilities |
|---|---|
| [`lib.rs`](../../../../../src/rs/vertebra-spanner/src/lib.rs)[^src-vertebra-spanner-src-lib-rs] | `#[pymodule(gil_used = false)]` `vertebra._spanner` exposing `SpannerClient` sync and `*_async` methods for streaming SQL, batch DML, mutations, `PartitionQuery` Data Boost Arrow reads, read-write transaction runners, and DDL updates. |
| [`client.rs`](../../../../../src/rs/vertebra-spanner/src/client.rs)[^src-vertebra-spanner-src-client-rs] | Multiplexed sessions (`CreateSession`), `ExecuteStreamingSql`, `ExecuteBatchDml`, inline-`BeginTransaction` `Commit` with `MultiplexedSessionPrecommitToken` tracking, `BatchWrite` (`MAX_MUTATION_CELLS_PER_BATCH = 80_000`), `PartitionQuery` (`SpannerCore.is_partitioned_query_fallback_error` on `code in {3, 7, 9, 12, 14}`), `run_in_transaction` retrying `SpannerAbortedError`, REST `DatabaseAdmin` `update_database_ddl` (`admin_endpoint`, `DEFAULT_DDL_TIMEOUT = 600s`), and `ca_pem` Omni TLS for non-`*.googleapis.com` HTTPS endpoints. |
| [`arrow_ipc.rs`](../../../../../src/rs/vertebra-spanner/src/arrow_ipc.rs)[^src-vertebra-spanner-src-arrow-ipc-rs] | Allocates `/vtb-sp-*` `ShmArrowStreamWriter` segments (`DEFAULT_SPANNER_ARROW_SHM_CAPACITY = 64 KiB`) and finalizes unregistered `VTBARROW` `ShmRegion` cursors. |
| [`codec.rs`](../../../../../src/rs/vertebra-spanner/src/codec.rs)[^src-vertebra-spanner-src-codec-rs] | `SpannerDialect` (`GoogleSql`, `Spangres`), `SpannerMutation` / `SpannerMutationGroup`, `SpannerStaleness` timestamp bounds, and char-aware `rewrite_spangres_placeholders`. |
| [`errors.rs`](../../../../../src/rs/vertebra-spanner/src/errors.rs)[^src-vertebra-spanner-src-errors-rs] | `PyOnceLock` lookup of `GoogleCloudAuthError`, `SpannerError`, and `SpannerAbortedError` (`code == 10` -> `sqlstate = "40001"`, `is_transient = True`). |

## Multicloud Staging & Adapter Tool Providers

- **Shared Multicloud Staging (`_staging.py`)**: [`ObjectStoreStaging`](../../../../../src/py/vertebra/db/_staging.py)[^src-db-staging-py] backs driver `export_to_object_store` and `load_from_object_store` pipelines across GCS (`gs://`), S3 (`s3://`), R2 (`r2://`), Azure (`az://`/`abfss://`), OCI (`oci://`), HTTP(S), and local `file://` targets. `validate_staging_target` and `validate_gcs_uri` validate destinations upfront and reject implicit `memory://` URIs without an explicit store instance; `resolve_sync_store` / `resolve_async_store`, `write_cursor_sync` / `write_cursor_async`, and `read_cursor_sync` / `read_cursor_async` bridge `SyncObjectStore`, `AsyncObjectStore`, and `ObjectStoreBackend` across `parquet` (`ZSTD` default), `arrow-ipc`, `csv`, and `jsonl`.
- **Neutral Adapter `ToolProvider` Implementations (`typing.py`, `tools.py`)**: [`BigQueryToolProvider`](../../../../../src/py/vertebra/db/adapters/bigquery/tools.py)[^src-db-bigquery-tools-py] and [`SpannerToolProvider`](../../../../../src/py/vertebra/db/adapters/spanner/tools.py)[^src-db-spanner-tools-py] implement the neutral [`ToolProvider`](../../../../../src/py/vertebra/typing.py)[^src-typing-public-py] protocol without importing agent or HTTP framework layers. Simple `SELECT` reads stay ungated while DML and unclassified `WITH` statements require consumer approval; `BigQueryToolProvider` enforces the minimum provider, per-call, and driver `maximum_bytes_billed` cap (plus optional dry-run budget checks), `SpannerToolProvider` preserves strong or bounded-staleness snapshots and binds `information_schema` inspection parameters, and policy denials raise `ToolPolicyError`.

[^src-vertebra-core-src-gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs` — Shared GcpTokenCache, ProxyPolicy, and GcloudCli ADC provider.
[^src-vertebra-db-src-gcp-rs]: `src/rs/vertebra-db/src/gcp.rs` — Optional gcp feature credential adapter and error classification.
[^src-vertebra-db-src-py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — 5-argument spawn_rust_db_task_with_attr and ShmCapsule bridge.
[^src-vertebra-py-src-errors-rs]: `src/rs/vertebra-py/src/errors.rs` — Multi-base BigQueryError and SpannerError exception definitions.
[^src-vertebra-bigquery-src-lib-rs]: `src/rs/vertebra-bigquery/src/lib.rs` — gil_used = false PyO3 vertebra._bigquery extension entrypoint.
[^src-vertebra-bigquery-src-client-rs]: `src/rs/vertebra-bigquery/src/client.rs` — BigQuery REST v2 and Storage Read/Write v1 client.
[^src-vertebra-bigquery-src-errors-rs]: `src/rs/vertebra-bigquery/src/errors.rs` — PyOnceLock BigQueryError and BigQueryRowError exception bridge.
[^src-vertebra-bigquery-src-arrow-ipc-rs]: `src/rs/vertebra-bigquery/src/arrow_ipc.rs` — Unregistered VTBARROW writers and zero-copy ArrowAppendPlan framing.
[^src-vertebra-bigquery-src-codec-rs]: `src/rs/vertebra-bigquery/src/codec.rs` — BigQueryWriteMode, GoogleSQL parameters, and REST error checks.
[^src-vertebra-spanner-src-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs` — gil_used = false PyO3 vertebra._spanner extension entrypoint.
[^src-vertebra-spanner-src-client-rs]: `src/rs/vertebra-spanner/src/client.rs` — Multiplexed Spanner gRPC client and REST DatabaseAdmin DDL LROs.
[^src-vertebra-spanner-src-errors-rs]: `src/rs/vertebra-spanner/src/errors.rs` — PyOnceLock SpannerError and SpannerAbortedError exception bridge.
[^src-vertebra-spanner-src-arrow-ipc-rs]: `src/rs/vertebra-spanner/src/arrow_ipc.rs` — Unregistered /vtb-sp-* VTBARROW cursor writer.
[^src-vertebra-spanner-src-codec-rs]: `src/rs/vertebra-spanner/src/codec.rs` — SpannerDialect, mutations, staleness bounds, and Spangres placeholders.
[^src-db-staging-py]: `src/py/vertebra/db/_staging.py` — Shared multicloud ObjectStoreStaging cursor serialization and URI resolution.
[^src-db-bigquery-init-py]: `src/py/vertebra/db/adapters/bigquery/__init__.py` — BigQueryConfig, BigQueryCore, and Sync/AsyncBigQueryDriver exports.
[^src-db-bigquery-tools-py]: `src/py/vertebra/db/adapters/bigquery/tools.py` — BigQueryToolProvider operations and billing-cap enforcement.
[^src-db-spanner-init-py]: `src/py/vertebra/db/adapters/spanner/__init__.py` — SpannerConfig, SpannerCore, and Sync/AsyncSpannerDriver exports.
[^src-db-spanner-tools-py]: `src/py/vertebra/db/adapters/spanner/tools.py` — SpannerToolProvider operations and staleness snapshot policies.
[^src-typing-public-py]: `src/py/vertebra/typing.py` — Neutral ToolProvider protocol and ToolPolicyError exception.
