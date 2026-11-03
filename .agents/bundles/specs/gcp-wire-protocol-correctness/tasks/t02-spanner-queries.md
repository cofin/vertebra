---
type: Task
id: gcp-wire-protocol-correctness:t02
title: Spanner queries on the official client
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: [gcp-wire-protocol-correctness:t01]
files:
  - src/rs/vertebra-db/src/spanner.rs
  - src/rs/vertebra-py/src/spanner.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/spanner/config.py
  - src/py/vertebra/db/spanner/driver.py
tests:
  - src/rs/vertebra-db/tests/spanner_wire_tests.rs
  - src/py/tests/unit/db/test_spanner.py
plan_revision: 2
plan_commit: null
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
tags: [rust, gcp, spanner, pyo3, python]
---

# t02 — Spanner queries on the official client

## Objective

`SpannerClient::query` and the new `SpannerClient::partitioned_query` run through `google_cloud_spanner::client::DatabaseClient`; the Spanner `ca_pem` policy applies at construction; `_vertebra.SpannerClient.partitioned_query[_async]` replaces `execute_partitioned_arrow[_async]`; `fetch_arrow` owns the fallback and builds Arrow in Python. Write operations still use the hand-written path until t03.

## Context

- Spec R3, R7, R12 (`../spec.md`); research capability notes.
- `behavior_tdd`: tests go through the public `SpannerClient` seam with `Spanner::from_stub` (model-level port).
- Verified crate APIs: `google_cloud_spanner::client::{Spanner, DatabaseClient, SpannerBuilderExt}`; `Spanner::builder()` (`with_endpoint`, `with_credentials`, `build().await` → `Result<Spanner, google_cloud_gax::client_builder::Error>`); `SpannerBuilderExt::{with_channel_pool(google_cloud_spanner::channel_pool::StaticChannelPoolConfig::new(n)), with_omni_tls(google_cloud_spanner::omni::TlsConfig)}`; `omni::TlsConfig::new().with_root_certificate_pem(impl Into<Vec<u8>>)`; `Spanner::from_stub(impl google_cloud_spanner::stub::Spanner)`; `spanner.database_client(db).with_database_role(impl Into<String>).with_leader_aware_routing(bool).build().await`; `db.single_use().set_timestamp_bound(b).build().execute_query(stmt).await -> Result<google_cloud_spanner::result::ResultSet>`; `db.batch_read_only_transaction().set_timestamp_bound(b).build().await?.partition_query(stmt, google_cloud_spanner::model::PartitionOptions::new().set_max_partitions(n)).await -> Result<Vec<google_cloud_spanner::batch::Partition>>`; `partition.set_data_boost(bool).execute(&db).await -> Result<ResultSet>`; `ResultSet::{next() -> Option<Result<Row>>, metadata() -> &ResultSetMetadata { column_names(), column_types() }, stats() -> Option<&model::ResultSetStats { row_count_exact() -> Option<&i64>, row_count_lower_bound() -> Option<&i64> }>}`; `Row::into_values() -> Vec<value::Value>`; `value::{Value, Kind}` (`kind()`, `as_bool()`, `as_f64()`, `as_str()`, `as_list()`, `Value::null()`, `From<String|bool|f64|Vec<Value>>`); `types::Type: From<model::Type>`, `model::Type: From<types::Type>`; `model::Type { code, array_element_type, struct_type, type_annotation, proto_type_fqn }` with `set_*` setters; `model::TypeCode: From<i32>` / `.value() -> Option<i32>`; `model::TypeAnnotationCode: From<i32>` / `.value()`; `model::StructType::new().set_fields(..)`; `model::struct_type::Field::new().set_name(..).set_type(..)`; `transaction::{TimestampBound, Timestamp}` (`Timestamp` = `google_cloud_wkt::Timestamp`, `Timestamp::new(seconds: i64, nanos: i32) -> Result<Timestamp, _>`); `statement::Statement::builder(sql).add_typed_param(name, value, ty).build()`.
- Stub plumbing: `google_cloud_gax::streaming::ResponseStream::from(tokio::sync::mpsc::Receiver<google_cloud_gax::Result<T>>)`; unary responses `google_cloud_gax::response::Response::from(body)`; errors `google_cloud_gax::error::Error::service(google_cloud_gax::error::rpc::Status::default().set_code(google_cloud_gax::error::rpc::Code::InvalidArgument).set_message(..))`. Generated `model` messages carry values as `serde_json::Value`.
- `SpannerError` currently derives `Clone, Debug, PartialEq, Eq`. At HEAD no code clones or compares a `SpannerError` (`grep -n "err.clone()\|== SpannerError\|assert_eq!(.*SpannerError" src/rs/vertebra-db src/rs/vertebra-py` returns nothing), so dropping those derives needs no other edit.
- The query dict keys `transaction_id` and `read_timestamp_micros` have no Python consumer (`grep -rn "read_timestamp_micros\|\"transaction_id\"" src/py/vertebra` matches nothing relevant); the official `ResultSet` does not expose them.
- `SpannerConfig.ca_pem` is documented in the `SpannerConfig` class docstring (`src/py/vertebra/db/spanner/config.py`).

## Steps

1. **Write failing tests (RED).**
   - In `src/rs/vertebra-db/tests/spanner_wire_tests.rs` add a `RecordingSpanner` stub (`#[derive(Clone, Debug, Default)] struct RecordingSpanner { sql_requests: Arc<Mutex<Vec<model::ExecuteSqlRequest>>>, partition_requests: Arc<Mutex<Vec<model::PartitionQueryRequest>>>, partitions: Vec<Vec<u8>>, partition_error: bool }`) implementing `google_cloud_spanner::stub::Spanner`:
     - `create_session` → `Response::from(model::Session::new().set_name("projects/p/instances/i/databases/d/sessions/mux1").set_multiplexed(true))`.
     - `begin_transaction` → `Response::from(model::Transaction::new().set_id(b"ro-tx".to_vec()))`.
     - `partition_query` → records the request; when `partition_error` returns `Err(Error::service(Status::default().set_code(Code::InvalidArgument).set_message("query is not root partitionable")))`; otherwise `model::PartitionResponse::new().set_partitions(..)` with one `model::Partition::new().set_partition_token(t)` per entry of `partitions`.
     - `execute_streaming_sql` → records the request and returns one `model::PartialResultSet` with `metadata.row_type` = fields `id` (`TypeCode::Int64`) and `name` (`TypeCode::String`), `stats` with `row_count_exact = 2`, and `values` `["1","a","2","b"]` (`serde_json::Value::String`) when there is no partition token; token `p0` → `["1","a"]`, token `p1` → `["2","b"]`.
   - Helper `fn stub_client(stub: RecordingSpanner) -> SpannerClient` → `SpannerClient::with_clients(Spanner::from_stub(stub), None, SpannerClientConfig::new("projects/p/instances/i/databases/d")).expect("client")`.
   - Tests (replacing `test_multiplexed_session_caching_routing_headers_and_resume_token_replay` and `test_partition_query_and_data_boost_to_vtbarrow_shm`):

     ```rust
     #[tokio::test]
     async fn query_maps_staleness_params_and_decodes_rows() {
         let stub = RecordingSpanner::default();
         let client = stub_client(stub.clone());
         let params = vec![SpannerQueryParameter::from_value("p1", &SpannerValue::Int64(7))];
         let result = client
             .query("SELECT id, name FROM t WHERE id > @p1", &params, SpannerStaleness::MaxStalenessMs(15_000))
             .await
             .expect("query");
         assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
         assert_eq!(result.rows, vec![
             vec![SpannerValue::Int64(1), SpannerValue::String("a".into())],
             vec![SpannerValue::Int64(2), SpannerValue::String("b".into())],
         ]);
         assert_eq!(result.row_count_exact, Some(2));
         let sent = stub.sql_requests.lock().unwrap();
         assert_eq!(sent.len(), 1);
         assert_eq!(sent[0].sql, "SELECT id, name FROM t WHERE id > @p1");
         assert!(sent[0].params.as_ref().is_some_and(|p| p.contains_key("p1")));
         assert_eq!(sent[0].param_types["p1"].code, model::TypeCode::Int64);
     }

     #[tokio::test]
     async fn query_rewrites_positional_placeholders_for_googlesql() {
         let stub = RecordingSpanner::default();
         let client = stub_client(stub.clone());
         let params = vec![SpannerQueryParameter::from_value("p1", &SpannerValue::Int64(7))];
         client.query("SELECT id, name FROM t WHERE id > $1", &params, SpannerStaleness::Strong).await.expect("query");
         assert_eq!(stub.sql_requests.lock().unwrap()[0].sql, "SELECT id, name FROM t WHERE id > @p1");
     }

     #[tokio::test]
     async fn partitioned_query_sets_data_boost_and_concatenates_partitions() {
         let stub = RecordingSpanner { partitions: vec![b"p0".to_vec(), b"p1".to_vec()], ..Default::default() };
         let client = stub_client(stub.clone());
         let result = client
             .partitioned_query("SELECT id, name FROM t", &[], 4, true, SpannerStaleness::Strong)
             .await
             .expect("partitioned query");
         assert_eq!(result.rows, vec![
             vec![SpannerValue::Int64(1), SpannerValue::String("a".into())],
             vec![SpannerValue::Int64(2), SpannerValue::String("b".into())],
         ]);
         assert_eq!(stub.partition_requests.lock().unwrap()[0].partition_options.as_ref().map(|o| o.max_partitions), Some(4));
         let sent = stub.sql_requests.lock().unwrap();
         assert_eq!(sent.iter().map(|r| r.partition_token.to_vec()).collect::<Vec<_>>(), vec![b"p0".to_vec(), b"p1".to_vec()]);
         assert!(sent.iter().all(|r| r.data_boost_enabled));
     }

     #[tokio::test]
     async fn partitioned_query_propagates_partition_errors() {
         let stub = RecordingSpanner { partition_error: true, ..Default::default() };
         let client = stub_client(stub.clone());
         let outcome = client.partitioned_query("SELECT 1", &[], 4, true, SpannerStaleness::Strong).await;
         assert!(matches!(outcome, Err(SpannerError::Client(_))));
         assert!(stub.sql_requests.lock().unwrap().is_empty());
     }

     #[test]
     fn ca_pem_policy_follows_endpoint_host() {
         let pem = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n".to_vec();
         let mut cloud = SpannerClientConfig::new("projects/p/instances/i/databases/d");
         cloud.ca_pem = Some(pem.clone());
         assert!(matches!(SpannerClient::new(cloud), Err(SpannerError::InvalidArgument(_))));
         let mut omni = SpannerClientConfig::new("projects/p/instances/i/databases/d");
         omni.endpoint = "https://omni.internal:15000".into();
         omni.ca_pem = Some(pem.clone());
         assert!(SpannerClient::new(omni).is_ok());
         let mut plaintext = SpannerClientConfig::new("projects/p/instances/i/databases/d");
         plaintext.endpoint = "http://127.0.0.1:9010".into();
         plaintext.ca_pem = Some(pem);
         match SpannerClient::new(plaintext) {
             Err(SpannerError::InvalidArgument(message)) => assert_eq!(message, "ca_pem requires an https:// endpoint"),
             other => panic!("expected InvalidArgument, got {:?}", other.err()),
         }
     }
     ```

     Generated-model field access (`sql`, `params`, `param_types`, `partition_token`, `data_boost_enabled`, `partition_options.max_partitions`) uses public struct fields; `params` is `Option<serde_json::Map<String, serde_json::Value>>`. Delete the two replaced tests and the `use vertebra_db::arrow_shm::{ARROW_IPC_EOS_MARKER, read_shm_arrow_frame, release_shm_arrow_frame};` import (only the partition test used it); the h2c helpers stay until t03 because `test_read_write_transaction_inline_begin_precommit_retry_and_aborted_wound_wait` still uses them.
   - In `src/py/tests/unit/db/test_spanner.py`, rename `FakeSpannerClient.execute_partitioned_arrow` to `partitioned_query` returning `self.query(sql, params_json, staleness_mode, staleness_ms)` and `execute_partitioned_arrow_async` to `partitioned_query_async` returning `self.partitioned_query(...)`; remove the `ArrowCursor` import from the test module only when no other test uses it (`grep -n ArrowCursor src/py/tests/unit/db/test_spanner.py`).
   - RED: `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests` → compile errors; `uv run pytest src/py/tests/unit/db/test_spanner.py -q` → `test_sync_spanner_arrow_and_object_store_roundtrip` fails with `AttributeError` (`execute_partitioned_arrow`).

2. **Implement the query path in `src/rs/vertebra-db/src/spanner.rs` (GREEN).**
   - Imports: `futures::future::try_join_all`, `google_cloud_spanner::channel_pool::StaticChannelPoolConfig`, `google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner, SpannerBuilderExt}`, `google_cloud_spanner::model as spanner_model`, `google_cloud_spanner::omni::TlsConfig`, `google_cloud_spanner::result::ResultSet`, `google_cloud_spanner::statement::Statement`, `google_cloud_spanner::transaction::{Timestamp, TimestampBound}`, `google_cloud_spanner::types::Type as OfficialType`, `google_cloud_spanner::value::{Kind, Value as OfficialValue}`, `tokio::sync::OnceCell`, `url::Url`, `crate::gcp::{install_rustls_ring_provider, official_credentials}`.
   - `SpannerError`: `#[derive(Debug, thiserror::Error)]` with `#[error(..)]` messages equal to the current `Display` arms, plus `#[error("Spanner client error: {0}")] Client(#[from] google_cloud_spanner::Error)`. Delete the manual `Display` / `std::error::Error` impls.
   - `SpannerClient` fields: add `injected_spanner: Option<Spanner>`, `injected_admin: Option<DatabaseAdmin>`, `official: OnceCell<Spanner>`, `database_client: OnceCell<DatabaseClient>`; keep the write-path fields until t03.
   - `fn endpoint_is_googleapis(endpoint: &str) -> bool` (private): `Url::parse(endpoint).ok().and_then(|u| u.host_str().map(|h| h == "googleapis.com" || h.ends_with(".googleapis.com"))).unwrap_or(false)`.
   - `SpannerClient::new` and `with_clients`: when `config.ca_pem.is_some()` and `endpoint_is_googleapis(&config.effective_endpoint())` → `SpannerError::InvalidArgument("SpannerClientConfig.ca_pem applies only to non-googleapis endpoints (Spanner Omni or private test servers); Cloud Spanner uses the system trust store")`; otherwise, when `config.ca_pem.is_some()` and `Url::parse(&config.effective_endpoint())` yields scheme `http` → `SpannerError::InvalidArgument("ca_pem requires an https:// endpoint".into())`. The googleapis check runs first. `with_clients` stores the injected clients and sets `token_cache: None`.
   - `async fn official_spanner(&self) -> Result<&Spanner, SpannerError>`: `OnceCell` returning the injected client, otherwise `install_rustls_ring_provider()` then `Spanner::builder().with_endpoint(self.config.effective_endpoint()).with_credentials(official_credentials(self.token_cache.clone())).with_channel_pool(StaticChannelPoolConfig::new(self.config.pool_size.max(1)))`, adding `.with_omni_tls(TlsConfig::new().with_root_certificate_pem(pem.clone()))` when `config.ca_pem` is `Some(pem)`, then `.build().await` mapped to `SpannerError::Protocol(format!("failed to build Spanner client: {e}"))`.
   - `async fn database_client(&self) -> Result<&DatabaseClient, SpannerError>`: `OnceCell` building `database_client(&self.config.database)`, `.with_database_role(role.clone())` when set, `.with_leader_aware_routing(self.config.route_to_leader_enabled)`, `.build().await?`.
   - Private module-level helpers:
     - `fn timestamp_bound(staleness: SpannerStaleness) -> Result<TimestampBound, SpannerError>`: `Strong → strong()`, `ExactStalenessMs(ms) → exact_staleness(Duration::from_millis(ms))`, `MaxStalenessMs(ms) → max_staleness(..)`, `ReadTimestampUnixMicros(us) → read_timestamp(wkt_timestamp(us)?)`, `MinReadTimestampUnixMicros(us) → min_read_timestamp(wkt_timestamp(us)?)`; `fn wkt_timestamp(us: i64) -> Result<Timestamp, SpannerError>` = `Timestamp::new(us.div_euclid(1_000_000), (us.rem_euclid(1_000_000) * 1_000) as i32).map_err(|e| SpannerError::InvalidArgument(e.to_string()))`.
     - `fn official_value(value: &ProtobufValue) -> OfficialValue`: `Null → OfficialValue::null()`, `Bool(b) → (*b).into()`, `Number(n) → (*n).into()`, `String(s) → s.clone().into()`, `List(items)` and `Struct(entries)` (values in order) → `Vec<OfficialValue>` `.into()`.
     - `fn official_type(ty: &SpannerType) -> OfficialType`: `spanner_model::Type::new().set_code(spanner_model::TypeCode::from(ty.code as i32))`, `set_array_element_type(spanner_model::Type::from(official_type(elem)))` when present, `set_struct_type(spanner_model::StructType::new().set_fields(fields))` when `struct_fields` is non-empty (each `spanner_model::struct_type::Field::new().set_name(&f.name).set_type(spanner_model::Type::from(official_type(&f.field_type)))`), `set_type_annotation(spanner_model::TypeAnnotationCode::from(ty.type_annotation as i32))`, `set_proto_type_fqn(fqn)` when present; return `OfficialType::from(model)`.
     - `fn protobuf_value(value: &OfficialValue) -> Result<ProtobufValue, SpannerError>` by `kind()`: `Null`, `Bool`, `Number`, `String`, `List` (recursive); `Unknown` or a `None` accessor → `SpannerError::Protocol("unsupported Spanner value kind".into())`.
     - `fn spanner_type(ty: &OfficialType) -> SpannerType`: via `spanner_model::Type::from(ty.clone())`; `SpannerTypeCode::from_i32(code.value().unwrap_or(0))`, `SpannerTypeAnnotation::from_i32(type_annotation.value().unwrap_or(0))`, recursion into array and struct fields (`SpannerType::default()` when a field type is absent), non-empty `proto_type_fqn` copied.
     - `async fn collect_query_result(mut rs: ResultSet) -> Result<SpannerQueryResult, SpannerError>`: drain `next()` into `Vec<Vec<OfficialValue>>`; read `metadata()` names/types into `schema: Vec<SpannerStructField>`; decode cells with `SpannerValue::from_protobuf_value(&protobuf_value(cell)?, &schema[i].field_type)?`; `row_count_exact = rs.stats().and_then(|s| s.row_count_exact().copied())`, same for `row_count_lower_bound`; other fields default.
   - `fn statement(&self, sql: &str, params: &[SpannerQueryParameter]) -> Statement`: rewrite with `rewrite_spangres_placeholders` when `dialect == GoogleSql && sql.contains('$')`, then `add_typed_param(&p.name, official_value(&p.value), official_type(&p.param_type))` per parameter, then `.build()`.
   - `pub async fn query(..)`: `db.single_use().set_timestamp_bound(timestamp_bound(staleness)?).build().execute_query(self.statement(sql, params)).await?` then `collect_query_result`.
   - `pub async fn partitioned_query(.., max_partitions: i64, data_boost_enabled: bool, staleness)`: batch read-only transaction with the bound; `partition_query(stmt, spanner_model::PartitionOptions::new().set_max_partitions(max_partitions.max(1))).await?` (errors propagate; no fallback here); execute partitions concurrently with `try_join_all(partitions.into_iter().map(|p| async move { collect_query_result(p.set_data_boost(data_boost_enabled).execute(db).await?).await }))`; concatenate rows in partition order; columns/schema from the first partition with a non-empty schema; row counts summed when present.
   - Delete `query_read_only`, `execute_partitioned_arrow`, `encode_encapsulated_arrow_ipc_frame`, `decode_partition_response`, `NEXT_SPANNER_SHM_SEQ`, and the unused `ShmArrowStreamWriter` / `ShmRegion` imports; update the module doc.

3. **Binding (`src/rs/vertebra-py/src/spanner.rs`).**
   - `query` / `query_async` call `client.query(..)`.
   - Replace `execute_partitioned_arrow[_async]` with `partitioned_query[_async]` (`#[pyo3(signature = (sql, params_json=None, max_partitions=4, data_boost_enabled=false, staleness_mode="strong", staleness_ms=0))]`) mapped with `query_result_to_py`.
   - `query_result_to_py`: remove the `transaction_id` and `read_timestamp_micros` keys; keep `columns`, `schema`, `rows`, `row_count_exact`, `row_count_lower_bound`.
   - Remove `use crate::db::ArrowCursor;` and the VTBARROW mention in the module doc.

4. **Stub and config docs.**
   - `src/py/vertebra/_vertebra.pyi`: replace the `execute_partitioned_arrow*` declarations with `partitioned_query(self, sql: str, params_json: str | None = None, max_partitions: int = 4, data_boost_enabled: bool = False, staleness_mode: str = "strong", staleness_ms: int = 0) -> dict[str, Any]: ...` and `partitioned_query_async(...) -> RustDbTaskAwaitable: ...`; class docstring: `"""Google Cloud Spanner client built on the official Rust client. ``query`` and ``partitioned_query`` return ``columns``, ``schema``, ``rows``, ``row_count_exact``, and ``row_count_lower_bound``. ``ca_pem`` is accepted only for non-googleapis ``https://`` endpoints (Spanner Omni or private test servers)."""`.
   - `src/py/vertebra/db/spanner/config.py`: replace the field attribute docstring under `SpannerConfig.ca_pem` with `"""PEM root certificates for a non-googleapis `https://` endpoint (Spanner Omni or a private test server). Rejected for `*.googleapis.com` endpoints, which use the system trust store, and for `http://` endpoints."""`.

5. **Python driver (`src/py/vertebra/db/spanner/driver.py`).** In `SyncSpannerDriver.fetch_arrow`: `_ = kwargs`; `raw = client.partitioned_query(rewritten_sql, params_json, max(1, int(partitions)), bool(boost), bound.mode, bound.effective_value())`; on an exception accepted by `SpannerCore.is_partitioned_arrow_fallback_error`, `raw = self._run_query_sync(sql, parameters, staleness=bound)`; then `return ArrowCursor.from_rows(raw.get("rows", ()), columns=[str(c) for c in raw.get("columns", ())], threshold_rows=threshold_rows)`. `AsyncSpannerDriver.fetch_arrow` mirrors it with `await client.partitioned_query_async(...)` and `await self._run_query_async(...)`.

6. **Refactor & verify.**

## Verification

- RED: commands in Step 1 fail as described.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-db --test spanner_wire_tests`
  - `cd src/rs && cargo clippy -p vertebra-db -p vertebra-py --all-targets -- -D warnings && cargo fmt --check`
  - `make dev && uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py src/py/tests/unit/extensions/test_sqlspec_spanner.py -q`
  - `uv run python -m mypy.stubtest vertebra._vertebra`
  - `! grep -rn "execute_partitioned_arrow\|encode_encapsulated_arrow_ipc_frame" src/rs src/py/vertebra src/py/tests`
- Commit: `git commit -S -m "feat(rs-db): run Spanner queries and partitioned queries on the official client"`.

## Acceptance Criteria

- [ ] R3: `query` and `partitioned_query` use `DatabaseClient`; staleness, typed params, placeholder rewrite, Data Boost, partition order, row counts, and error propagation are asserted.
- [ ] R7: `partitioned_query[_async]` in binding and stub; `fetch_arrow` owns the fallback and builds `ArrowCursor.from_rows`.
- [ ] R12 (Spanner): `ca_pem` rejected for googleapis endpoints and for `http://` endpoints, applied through Omni TLS otherwise; docstrings updated.

## Notes & Discoveries
