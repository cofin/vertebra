---
type: Research
flow_id: integration-remediation
title: Official Google Cloud Rust crates versus the hand-written GCP wire code
description: Primary-source evaluation of googleapis/google-cloud-rust crates for Spanner, BigQuery Storage, and auth, recording the adopt/keep decisions implemented in gcp-wire-protocol-correctness and gcp-driver-resilience.
status: stable
state: promoted
promoted_to: integration-remediation
created_at: '2026-11-03T00:00:00Z'
updated_at: '2026-11-16T06:50:00Z'
generated:
  by: flow-setup
  at: '2026-11-07T04:15:00Z'
verified:
  - by: agent:flow-archive
    at: '2026-11-16T06:50:00Z'
    commit: f34c45c0
sources:
  - id: crate-spanner
    resource: https://crates.io/api/v1/crates/google-cloud-spanner
    title: google-cloud-spanner crates.io metadata
  - id: crate-bq
    resource: https://crates.io/api/v1/crates/google-cloud-bigquery
    title: google-cloud-bigquery crates.io metadata
  - id: crate-auth
    resource: https://crates.io/api/v1/crates/google-cloud-auth
    title: google-cloud-auth 1.18.0 crates.io metadata
  - id: crate-gaxi
    resource: https://crates.io/api/v1/crates/google-cloud-gax-internal
    title: google-cloud-gax-internal tonic and reqwest transport
  - id: gcp-rust-repo
    resource: https://github.com/googleapis/google-cloud-rust
    title: Official googleapis/google-cloud-rust monorepo
  - id: docs-spanner
    resource: https://docs.rs/google-cloud-spanner/0.34.8-preview
    title: google-cloud-spanner 0.34.8-preview API reference
  - id: docs-bq
    resource: https://docs.rs/google-cloud-bigquery/0.19.0
    title: google-cloud-bigquery 0.19.0 API reference
  - id: spanner-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/spanner.proto
    title: Official Spanner v1 protobuf definitions
  - id: bq-storage-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/cloud/bigquery/storage/v1/storage.proto
    title: Official BigQuery Storage v1 protobuf definitions
  - id: spanner-emulator
    resource: https://cloud.google.com/spanner/docs/emulator
    title: Cloud Spanner emulator gRPC/REST port documentation
tags:
  - research
  - gcp
  - rust
  - spanner
  - bigquery
---

# Official Google Cloud Rust crates versus the hand-written GCP wire code

<!-- truth: start -->
- **Official SDK Adoption**: `google-cloud-spanner =0.34.8-preview` [^crate-spanner] [^docs-spanner] and `google-cloud-bigquery =0.19.0` [^crate-bq] [^docs-bq] from `googleapis/google-cloud-rust` [^gcp-rust-repo] replace hand-written protobuf encoders and eliminate wire field-number mismatches verified against `googleapis` protos [^spanner-proto] [^bq-storage-proto].
- **Auth & TLS Standardization**: Both crates run with `default-features = false`, `tonic/tls-ring` + `tls-webpki-roots` over `google-cloud-gax-internal` [^crate-gaxi], bridging `GcpTokenCache` through `google-cloud-auth` `CredentialsProvider` [^crate-auth] and rewriting emulator admin ports per Spanner emulator rules [^spanner-emulator].
<!-- truth: end -->

## Question

The hand-written Spanner and BigQuery Storage clients in `src/rs/vertebra-db/src/{spanner.rs,bigquery.rs,grpc.rs}` encode protobuf by hand and use wrong field numbers in several messages. The user cleared the official `googleapis/google-cloud-rust` crates for use, including pre-1.0 releases, if they are functional. This note records which crate is official, what each one can do, and the decision per area. Facts come from the crates.io API and the published crate sources at the listed versions.

## Crate identity (crates.io API, 2026-11-03)

| Crate | Latest | Publisher / owners | Notes |
| --- | --- | --- | --- |
| `google-cloud-spanner` | `0.34.8-preview` (2026-10-21) | `cloud-sdk-rust-bot`; owners `codyoss`, `coryan`, team `googleapis:google-cloud-rust-crate-publishers` | `default_version` still reports `0.33.0`, the last release by the previous owner. The `0.34.x-preview` line is the googleapis crate. Pre-release versions need an exact `=` requirement. |
| `google-cloud-bigquery` | `0.19.0` (2026-10-21) | `cloud-sdk-rust-bot`, googleapis team | `0.15.0` and earlier were published by the previous owner (`yoshidan`). `0.16.1-preview` onward is googleapis. |
| `google-cloud-auth` | `1.18.0` | googleapis team | Stable. |
| `google-cloud-gax` | `1.15.0` | googleapis team | Error, retry, streaming types shared by every client. |
| `google-cloud-gax-internal` | `0.7.21` | googleapis team | Transport layer (`tonic` gRPC, `reqwest` REST). |
| `google-cloud-spanner-admin-database-v1` | `1.15.0` | googleapis team | Pulled in by `google-cloud-spanner`; exposes `DatabaseAdmin`. |
| `google-cloud-bigquery-storage-v1` | does not exist | — | Storage Read/Write live inside `google-cloud-bigquery`. |
| `gcloud-spanner` | `1.9.0` | `yoshidan` | Community crate; not official. |

All googleapis crates above declare `rust-version = "1.91.0"`. The Vertebra workspace uses `edition = "2024"` and pins no `rust-version`.

## Capability check

### `google-cloud-spanner` `0.34.8-preview`

- `client::Spanner` (`Spanner::builder()` is a `google_cloud_gax::client_builder::ClientBuilder` with `with_endpoint`, `with_credentials`, retry and backoff policies; `SpannerBuilderExt::{with_channel_pool, with_instance_type, with_omni_tls}`) and `client::DatabaseClient` (`spanner.database_client(db).with_database_role(role: impl Into<String>).with_leader_aware_routing(enabled: bool).build().await`).
- Multiplexed sessions are managed by `DatabaseClient` (`session_maintainer.rs`).
- Read-write transactions: `read_write_transaction()` → `TransactionRunnerBuilder` (`set_transaction_tag`, `set_max_commit_delay`, `set_exclude_txn_from_change_streams`) → `.build().run(async |tx| ..)`, which retries on `ABORTED`, tracks precommit tokens (`precommit.rs`), and uses inline begin with an explicit-begin path (`BeginTransactionOption`). `tx.execute_update` uses `ExecuteSql`; `tx.execute_batch_update` uses `ExecuteBatchDml`. The runner and `write_only_transaction().write()` return the `CommitResponse` as received; neither checks `commit_timestamp`.
- Streaming queries resume from `PartialResultSet.resume_token` (`result_set.rs`). `ResultSet` exposes `metadata()` (`column_names`, `column_types`) and `stats()` (`row_count_exact()`, `row_count_lower_bound()`); it does not expose the transaction id or read timestamp.
- `transaction::TimestampBound::{strong, exact_staleness, max_staleness, read_timestamp, min_read_timestamp}`; `transaction::Timestamp` re-exports `google_cloud_wkt::Timestamp`.
- `batch::{BatchReadOnlyTransaction, Partition, BatchDml}`: `batch_read_only_transaction().set_timestamp_bound(b).build().await?.partition_query(stmt, model::PartitionOptions)` and `partition.set_data_boost(bool).execute(&db)`.
- `partitioned_dml_transaction()`, `write_only_transaction()`, `batch_write_transaction().build().execute_streaming(groups)`; `mutation::{Mutation, MutationGroup}`, `key::{Key::new(values: Vec<Value>), KeySet::{all, builder}}`.
- DDL: `client::DatabaseAdmin` (re-export of `google_cloud_spanner_admin_database_v1::client::DatabaseAdmin`) is a REST client built on `gaxi::http::ReqwestClient` (HTTP/1.1). `update_database_ddl().send()` returns the `google.longrunning.Operation`; `get_operation()` polls it. `Spanner::database_admin_builder()` rewrites an endpoint ending in `:9010` to `:9020` only when the client detected `SPANNER_EMULATOR_HOST`; the emulator serves gRPC on 9010 and REST on 9020.
- Custom CA: `omni::TlsConfig::new().with_root_certificate_pem(pem)` passed to `SpannerBuilderExt::with_omni_tls`; calling it sets `InstanceType::Omni`.
- Emulator: `SPANNER_EMULATOR_HOST` detection plus explicit `with_endpoint`.
- Test seams: `Spanner::from_stub(stub)` (`stub::Spanner` over public `model` types) and `DatabaseAdmin::from_stub`.
- Values: rows expose `value::Value`, a wrapper over `prost_types::Value` (`Kind`, `as_bool`, `as_f64`, `as_str`, `as_list`, `From` impls for `String`, `i64`, `f64`, `bool`, `Vec<u8>`, `Vec<T: Into<Value>>`, `Option<T>`, `Value::null()`). Generated `model` messages used by stubs carry `serde_json::Value` (`wkt::Value`); the client converts them to `prost_types::Value` before the wire. `types::Type` converts to and from `model::Type`.
- Features: `default = ["builtin-metrics", "default-rustls-provider"]`. `builtin-metrics` adds `google-cloud-monitoring-v3` and the OpenTelemetry metrics exporter; `default-rustls-provider` turns on `tonic/tls-aws-lc`. `opentelemetry`, `opentelemetry_sdk`, and `tracing-opentelemetry` are unconditional through `google-cloud-gax-internal`'s `_internal-common` feature.

### `google-cloud-gax-internal` `0.7.21` (transport)

- gRPC: `tonic 0.14` with `tls-native-roots`; TLS is `ClientTlsConfig::new().with_enabled_roots()`, which trusts every root source enabled on `tonic` (`tls-native-roots`, plus `tls-webpki-roots` when a dependent enables it). The client sets `max_decoding_message_size(usize::MAX)`, so the 128 MiB BigQuery/Spanner frames fit.
- REST: `reqwest 0.13` with `default-features = false`; it gets TLS from feature unification (the workspace `reqwest` uses `rustls`, which selects aws-lc-rs and `rustls-platform-verifier`, i.e. the OS trust store) and honors `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`.

### `google-cloud-bigquery` `0.19.0`

- `client::Read` (generated Storage Read client): `create_read_session()` and `read_rows().into_reader()`; `read::Reader` resumes `ReadRows` from the consumed row offset. `model::ArrowSchema.serialized_schema` and `model::ArrowRecordBatch.serialized_record_batch` are `bytes::Bytes`. Test seam: `Read::from_stub`.
- `client::Write`: `open_default_stream(table)`, `create_stream::<write::stream_type::{PendingStream, CommittedStream, BufferedStream}>(table)`, `.build_arrow(ArrowSchema)`, `append(rows).send()`, `AppendWithOffset::set_offset`, `PendingWriter::{finalize, commit}`, `BufferedWriter::flush`. Errors: `error::{AppendError::{Rpc, RowErrors}, CommitError::{Rpc, FailedTransaction}, WriterBuilderError}`. `Write` has no `from_stub`.
- `client::BigQuery` (REST `jobs.query`) with typed rows through `FromSql`.

### `google-cloud-auth` `1.18.0`

- `impl CredentialsProvider for T` then `Credentials::from(T)`; `headers(Extensions)` returns `std::result::Result<CacheableResource<HeaderMap>, google_cloud_auth::errors::CredentialsError>` (the crate's `Result` alias is `pub(crate)`).
- `default = ["default-idtoken-backend", "default-rustls-provider"]`, both aws-lc-rs.

## Decision

| Area | Decision | Evidence |
| --- | --- | --- |
| Spanner data plane and DDL | **Adopt** `google-cloud-spanner =0.34.8-preview` (`default-features = false`) behind `vertebra_db::spanner::SpannerClient`; delete the hand-written Spanner protobuf code. DDL uses `DatabaseAdmin` built with an explicit `admin_endpoint` rule. | Every operation the binding uses maps to a crate API; `from_stub` ports exist. |
| BigQuery Storage Read | **Adopt** `client::Read` + `read::Reader`. | Raw Arrow `Bytes`, offset resume, `Read::from_stub`. |
| BigQuery Storage Write | **Adopt** `client::Write` with the Arrow format; the Arrow IPC splitter stays ours and is hardened. | Default/committed/pending/buffered streams with offsets. |
| BigQuery REST | **Keep and fix** the `reqwest` JSON path. | No field-number risk; two local defects. |
| Auth | **Keep** `GcpTokenCache` and bridge it through a `CredentialsProvider`. | gcloud fallback, V4 signing key material, and the Python `GcpTokenCache` are Vertebra features. |
| `vertebra-db/src/grpc.rs` | **Delete** after both migrations. | No other users. |
| TLS | `default-features = false`; `tonic` gains `tls-ring` and `tls-webpki-roots` (bundled roots keep minimal images working); install `rustls::crypto::ring::default_provider()` before building a client. DDL REST uses the OS trust store through `reqwest`'s platform verifier. | `aws-lc-rs` is already in `Cargo.lock` via `reqwest`'s `rustls` feature. |
| Custom CA | Spanner: `ca_pem` on a non-`*.googleapis.com` endpoint uses `with_omni_tls`; on a googleapis endpoint it is rejected. BigQuery: `ca_pem` applies to REST only; Storage gRPC uses the trust store. | `omni::TlsConfig`; Storage clients expose no custom-root option. |

## Consequences

- Field-number corrections come from deleting the hand-written encoders; no golden-byte fixture generator is added. The verified numbers are recorded below.
- Rows pass through `prost_types::Value`; `make bench-check` gates regressions (follow-up in `runtime-structure-and-throughput`).
- OpenTelemetry crates are compiled in through `google-cloud-gax-internal`; no exporter runs because `builtin-metrics` is off.
- Emulator suites do not exist at HEAD; when added, emulator-only gaps use `xfail(strict=True)`.

## Verified field numbers (official protos)

| Message | Field | Number | Hand-written code used |
| --- | --- | --- | --- |
| `bigquery.storage.v1.ReadSession` | `data_format` / `arrow_schema` / `table` / `read_options` / `streams` | 3 / 5 / 6 / 8 / 10 | 1 / 4 / 2 / 5 / 6 |
| `bigquery.storage.v1.ReadRowsResponse` | `arrow_record_batch` / `arrow_schema` | 4 / 8 | 3 / 7 |
| `bigquery.storage.v1.AppendRowsRequest` | `proto_rows` / `arrow_rows` | 4 / 5 | arrow on 4 |
| `bigquery.storage.v1.AppendRowsResponse` | `row_errors` / `write_stream` | 4 / 5 | field 5 parsed as `RowError` |
| `bigquery.storage.v1.StorageError` | `code` / `entity` / `error_message` | 1 / 2 / 3 | entity read from 1 |
| `spanner.v1.Session` | `creator_role` / `multiplexed` | 5 / 6 | role on 4 |
| `spanner.v1.ExecuteSqlRequest` | `query_options` / `request_options` | 10 / 11 | request options on 10 |
| `spanner.v1.Transaction` | `precommit_token` | 3 | 8 |
| `spanner.v1.CommitRequest` | `precommit_token` | 9 | 12 |
| `spanner.v1.PartialResultSet` | `precommit_token` | 8 | 8 (correct) |

[^crate-spanner]: google-cloud-spanner crates.io metadata (https://crates.io/api/v1/crates/google-cloud-spanner).
[^crate-bq]: google-cloud-bigquery crates.io metadata (https://crates.io/api/v1/crates/google-cloud-bigquery).
[^crate-auth]: google-cloud-auth 1.18.0 crates.io metadata (https://crates.io/api/v1/crates/google-cloud-auth).
[^crate-gaxi]: google-cloud-gax-internal tonic and reqwest transport (https://crates.io/api/v1/crates/google-cloud-gax-internal).
[^gcp-rust-repo]: Official googleapis/google-cloud-rust monorepo (https://github.com/googleapis/google-cloud-rust).
[^docs-spanner]: google-cloud-spanner 0.34.8-preview API reference (https://docs.rs/google-cloud-spanner/0.34.8-preview).
[^docs-bq]: google-cloud-bigquery 0.19.0 API reference (https://docs.rs/google-cloud-bigquery/0.19.0).
[^spanner-proto]: Official Spanner v1 protobuf definitions (https://github.com/googleapis/googleapis/blob/master/google/spanner/v1/spanner.proto).
[^bq-storage-proto]: Official BigQuery Storage v1 protobuf definitions (https://github.com/googleapis/googleapis/blob/master/google/cloud/bigquery/storage/v1/storage.proto).
[^spanner-emulator]: Cloud Spanner emulator gRPC/REST port documentation (https://cloud.google.com/spanner/docs/emulator).
