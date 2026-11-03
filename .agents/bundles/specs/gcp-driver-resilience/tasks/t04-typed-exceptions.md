---
type: Task
id: gcp-driver-resilience:t04
title: Typed Google Cloud exception hierarchy across bindings and drivers
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: [gcp-driver-resilience:t01, gcp-driver-resilience:t02, gcp-driver-resilience:t03, gcp-driver-resilience:t05]
files:
  - src/rs/vertebra-py/Cargo.toml
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/rs/vertebra-py/src/completion.rs
  - src/rs/vertebra-py/src/spanner.rs
  - src/rs/vertebra-py/src/bigquery.rs
  - src/rs/vertebra-py/src/gcp_auth.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/spanner/core.py
  - src/py/vertebra/db/spanner/driver.py
  - src/py/vertebra/db/bigquery/core.py
tests:
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-py/src/completion.rs
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_bigquery.py
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
tags: [rust, pyo3, python, exceptions]
---

# t04 — Typed Google Cloud exception hierarchy across bindings and drivers

## Objective

Every Spanner, BigQuery, and `GcpTokenCache` binding (sync and async) raises one exception hierarchy rooted at `vertebra.typing.VertebraError` that carries the canonical gRPC code, and the Python drivers decide fallbacks and retries from codes and classes instead of message substrings.

## Context

- Spec R8, R9 (`../spec.md`).
- `behavior_tdd`: new observable error contract.
- Rust error inputs: `vertebra_db::spanner::SpannerError::{InvalidArgument, Auth, Client(google_cloud_spanner::Error), Protocol, Ddl { operation, code, message }, DdlTimeout { .. }}`; `vertebra_db::bigquery::BigQueryError::{Auth, Http, Api { status, message, reason, canonical_status }, Client(google_cloud_bigquery::Error), Shm, RowErrors(Vec<BigQueryRowError { index, code, message }>), InvalidArgument, AppendVerification}`; `GcpTokenCache` errors are `String`.
- `google_cloud_gax::error::Error::{status() -> Option<&rpc::Status>, http_status_code() -> Option<u16>, is_authentication(), is_timeout(), is_transport(), is_io(), service(rpc::Status)}`; `google_cloud_gax::error::rpc::{Status { code, message, details }, Code}` with `Status::default().set_code(Code::Aborted).set_message(..).set_details(..)` and `Code::name() -> &'static str`; `rpc::StatusDetails::{RetryInfo(google_cloud_rpc::model::RetryInfo { retry_delay: Option<wkt::Duration> }), ErrorInfo(google_cloud_rpc::model::ErrorInfo { reason, .. })}`; `wkt::Duration::{seconds(), nanos()}`.
- PyO3 0.30: `PyErr::new_type(py, name: &CStr, doc: Option<&CStr>, base: Option<&Bound<PyType>>, dict: Option<Py<PyAny>>) -> PyResult<Py<PyType>>`; `PyErr::from_type(ty: Bound<PyType>, args) -> PyErr`; `PyErr::value(py)`; `pyo3::sync::PyOnceLock::get_or_try_init(py, f)`; `Bound::cast_into::<PyType>()`; `Python::get_type::<pyo3::exceptions::PyException>() -> Bound<'_, PyType>`. `vertebra._typing` imports only stdlib and `msgspec`; `vertebra.typing` re-exports its `VertebraError`.
- `cargo test -p vertebra-py` runs an embedded interpreter whose `sys.path` does not include the `vertebra` package, so Rust unit tests never import `vertebra`; they build the hierarchy on a stand-in base class, and the `VertebraError` ancestry is asserted in pytest.
- Canonical codes: `OK 0, CANCELLED 1, UNKNOWN 2, INVALID_ARGUMENT 3, DEADLINE_EXCEEDED 4, NOT_FOUND 5, ALREADY_EXISTS 6, PERMISSION_DENIED 7, RESOURCE_EXHAUSTED 8, FAILED_PRECONDITION 9, ABORTED 10, OUT_OF_RANGE 11, UNIMPLEMENTED 12, INTERNAL 13, UNAVAILABLE 14, DATA_LOSS 15, UNAUTHENTICATED 16`. HTTP → code when `canonical_status` is absent: 400→3, 401→16, 403→7, 404→5, 409→10, 412→9, 416→11, 429→8, 499→1, 500→13, 501→12, 503→14, 504→4, otherwise 2.
- stubtest does not require instance attributes declared in a class body to exist on the runtime class.
- `t01` tests call `spawn_rust_db_task(.., pyo3::exceptions::PyRuntimeError::new_err)`; the `map_err` signature changes here.

## Steps

1. **Write failing tests (RED).**
   - `src/py/tests/unit/db/test_spanner.py` (top-level imports `from collections.abc import Callable`, `from vertebra._vertebra import BigQueryError, BigQueryRowError, GoogleCloudAuthError, GoogleCloudError, SpannerAbortedError, SpannerClient, SpannerError`, `from vertebra.typing import VertebraError`):

     ```python
     @pytest.mark.parametrize(
         ("child", "parent"),
         [
             (GoogleCloudError, VertebraError),
             (GoogleCloudAuthError, GoogleCloudError),
             (SpannerError, GoogleCloudError),
             (SpannerAbortedError, SpannerError),
             (BigQueryError, GoogleCloudError),
             (BigQueryRowError, BigQueryError),
         ],
     )
     def test_google_cloud_error_hierarchy_derives_from_vertebra_error(child: type[Exception], parent: type[Exception]) -> None:
         """Every Google Cloud exception class sits under VertebraError in the documented order."""
         assert issubclass(child, parent)
         assert isinstance(child("m"), VertebraError)


     def test_spanner_client_invalid_params_raise_typed_error() -> None:
         """Sync binding raises SpannerError with INVALID_ARGUMENT for malformed parameters."""
         client = SpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
         with pytest.raises(SpannerError) as excinfo:
             client.query("SELECT 1", "not json")
         assert excinfo.value.code == 3
         assert excinfo.value.code_name == "INVALID_ARGUMENT"
         assert isinstance(excinfo.value, VertebraError)


     @pytest.mark.anyio
     async def test_spanner_client_invalid_params_raise_typed_error_async() -> None:
         """Async binding raises the same SpannerError as the sync binding."""
         client = SpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
         with pytest.raises(SpannerError) as excinfo:
             await client.query_async("SELECT 1", "not json")
         assert excinfo.value.code == 3


     @pytest.mark.parametrize(("code", "expected"), [(3, True), (7, True), (9, True), (12, True), (14, True), (5, False), (10, False)])
     def test_partitioned_query_fallback_uses_status_codes(code: int, expected: bool) -> None:
         """Fallback decisions use canonical codes, not message text."""
         err = SpannerError("partition failure")
         err.code = code
         assert SpannerCore.is_partitioned_query_fallback_error(err) is expected


     def test_partitioned_query_fallback_ignores_untyped_errors() -> None:
         """A RuntimeError mentioning a status name is not a fallback signal."""
         assert SpannerCore.is_partitioned_query_fallback_error(RuntimeError("PERMISSION_DENIED")) is False


     @pytest.mark.parametrize(("error_factory", "expected_calls"), [(lambda: SpannerAbortedError("aborted"), 2), (lambda: RuntimeError("aborted"), 1)])
     def test_run_in_transaction_retries_only_aborted(error_factory: Callable[[], Exception], expected_calls: int) -> None:
         """Only SpannerAbortedError triggers a retry."""
         driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
         calls: list[int] = []

         def work(tx: object) -> str:
             calls.append(1)
             if len(calls) == 1:
                 raise error_factory()
             return "ok"

         if expected_calls == 2:
             assert driver.run_in_transaction(work, max_attempts=3) == "ok"
         else:
             with pytest.raises(RuntimeError):
                 driver.run_in_transaction(work, max_attempts=3)
         assert len(calls) == expected_calls
     ```

   - `src/py/tests/unit/db/test_bigquery.py` (top-level `from vertebra._vertebra import BigQueryClient, BigQueryError`): `test_bigquery_client_invalid_params_raise_typed_error` (`BigQueryClient(project_id="p1", rest_endpoint="http://127.0.0.1:9").query("SELECT 1", "not json")` → `BigQueryError` with `code == 3`), the async twin with `query_async`, and `test_storage_api_fallback_uses_status_codes` parametrized over `[(7, True), (9, True), (12, True), (14, True), (5, False), (3, False)]`, plus a case asserting `RuntimeError("status 403")` → `False`.
   - Rust tests in `src/rs/vertebra-py/src/errors.rs` `#[cfg(test)] mod tests` with `Python::attach`. The tests never import `vertebra`; a helper `fn stand_in_types(py: Python<'_>) -> GcpExceptionTypes` builds `let base = PyErr::new_type(py, c"tests.VertebraError", None, Some(&py.get_type::<pyo3::exceptions::PyException>()), None).expect("base").into_bound(py);` and returns `exception_types_with_base(py, &base).expect("types")`. Mapper tests call the `*_with` mappers with those types:
     - `hierarchy_derives_from_stand_in_base`: `issubclass(google_cloud, base)`, `issubclass(spanner_aborted, spanner)`, `issubclass(bigquery_row, bigquery)`, `issubclass(auth, google_cloud)` via `Bound::<PyType>::is_subclass`.
     - `aborted_status_maps_to_spanner_aborted_error`: `spanner_error_with(py, &types, SpannerClientError::Client(google_cloud_gax::error::Error::service(Status::default().set_code(Code::Aborted).set_message("txn aborted").set_details([StatusDetails::RetryInfo(google_cloud_rpc::model::RetryInfo::new().set_retry_delay(google_cloud_wkt::Duration::new(0, 250_000_000).expect("duration")))]))))` → `err.is_instance(py, types.spanner_aborted.bind(py).as_any())`, `code == 10`, `code_name == "ABORTED"`, `retry_delay_ms == 250`.
     - `rest_api_error_prefers_canonical_status`: `bigquery_error_with(py, &types, BigQueryClientError::Api { status: 400, message: "m".into(), reason: Some("notFound".into()), canonical_status: Some("NOT_FOUND".into()) })` → `code == 5`, `http_status == 400`, `reason == "notFound"`.
     - `rest_api_error_maps_http_status`: 429 without canonical → 8.
     - `row_errors_map_to_bigquery_row_error`: instance of `types.bigquery_row`, `row_errors == [(0, 1, "bad field")]`.
     - `ddl_timeout_maps_to_deadline_exceeded`: code 4.
   - RED: `make dev && uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py -q` → `ImportError` for `SpannerError`; `cd src/rs && cargo test -p vertebra-py errors::tests` → module missing.

2. **Implement `src/rs/vertebra-py/src/errors.rs` (GREEN).**
   - Imports with aliases so Rust error enums and Python classes never collide: `use vertebra_db::spanner::SpannerError as SpannerClientError;`, `use vertebra_db::bigquery::BigQueryError as BigQueryClientError;`.
   - Exception types:

     ```rust
     pub struct GcpExceptionTypes {
         pub google_cloud: Py<PyType>,
         pub auth: Py<PyType>,
         pub spanner: Py<PyType>,
         pub spanner_aborted: Py<PyType>,
         pub bigquery: Py<PyType>,
         pub bigquery_row: Py<PyType>,
     }

     static EXCEPTION_TYPES: PyOnceLock<GcpExceptionTypes> = PyOnceLock::new();

     fn exception_types_with_base(py: Python<'_>, base: &Bound<'_, PyType>) -> PyResult<GcpExceptionTypes>;
     pub fn exception_types(py: Python<'_>) -> PyResult<&'static GcpExceptionTypes>;
     ```

     `exception_types_with_base` is uncached and builds all six classes on `base`: `PyErr::new_type(py, c"vertebra._vertebra.GoogleCloudError", Some(c"Error returned by a Google Cloud client; carries the canonical gRPC status in `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, and `reason`."), Some(base), None)?`, and in the same way `GoogleCloudAuthError` (base `GoogleCloudError`, doc "Credentials could not be obtained or were rejected."), `SpannerError` (base `GoogleCloudError`, "Error returned by a Cloud Spanner operation."), `SpannerAbortedError` (base `SpannerError`, "Spanner aborted the transaction; the whole transaction can be retried."), `BigQueryError` (base `GoogleCloudError`, "Error returned by a BigQuery operation."), `BigQueryRowError` (base `BigQueryError`, "BigQuery Storage Write rejected rows; see `row_errors`."). `exception_types` is `EXCEPTION_TYPES.get_or_try_init(py, || { let base = py.import("vertebra._typing")?.getattr("VertebraError")?.cast_into::<PyType>()?; exception_types_with_base(py, &base) })`.
   - `struct GcpErrorInfo { code: i32, message: String, http_status: Option<u16>, retry_delay_ms: Option<u64>, reason: Option<String> }`; `const CODE_NAMES: [&str; 17]`; `fn code_name(code: i32) -> &'static str` (out of range → `"UNKNOWN"`); `fn code_number(name: &str) -> i32` (unknown → 2); `fn http_code(status: u16) -> i32` (table above).
   - `fn gax_error_info(err: &google_cloud_gax::error::Error) -> GcpErrorInfo`: with a status → `code_number(status.code.name())`, `status.message.clone()`, `retry_delay_ms` from the first `RetryInfo` (`seconds() * 1000 + nanos() / 1_000_000`), `reason` from the first `ErrorInfo`; without a status → 16 if `is_authentication()`, 4 if `is_timeout()`, 14 if `is_transport() || is_io()`, else 2, message `err.to_string()`; `http_status = err.http_status_code()`.
   - `fn raise(py: Python<'_>, ty: &Py<PyType>, info: GcpErrorInfo) -> PyErr`: `let err = PyErr::from_type(ty.bind(py).clone(), info.message.clone());` then set `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, `reason` on `err.value(py)`; a failed `setattr` returns that error.
   - `fn spanner_error_with(py: Python<'_>, types: &GcpExceptionTypes, err: SpannerClientError) -> PyErr`: `InvalidArgument` → `SpannerError` 3; `Auth` → `GoogleCloudAuthError` 16; `Client(e)` → `SpannerAbortedError` when the code is 10, else `SpannerError`; `Protocol` → `SpannerError` 13; `Ddl { operation, code, message }` → `SpannerError` with `code` and message `"{operation}: {message}"`; `DdlTimeout` → `SpannerError` 4 with `err.to_string()`.
   - `pub fn spanner_error_to_py(py: Python<'_>, err: SpannerClientError) -> PyErr`: `match exception_types(py) { Ok(types) => spanner_error_with(py, types, err), Err(e) => e }`.
   - `fn bigquery_error_with(py: Python<'_>, types: &GcpExceptionTypes, err: BigQueryClientError) -> PyErr`: `Auth` → `GoogleCloudAuthError` 16; `Http` → `BigQueryError` 14; `Api` → `BigQueryError` with `canonical_status.map(code_number).unwrap_or(http_code(status))`, `http_status = Some(status)`, `reason`; `Client(e)` → gax info; `Shm` and `AppendVerification` → 13; `InvalidArgument` → 3; `RowErrors(rows)` → `BigQueryRowError` 3 plus attribute `row_errors: Vec<(i64, i32, String)>`.
   - `pub fn bigquery_error_to_py(py: Python<'_>, err: BigQueryClientError) -> PyErr`: same wrapper shape over `bigquery_error_with`.
   - `fn auth_error_with(py: Python<'_>, types: &GcpExceptionTypes, message: String) -> PyErr` → `GoogleCloudAuthError` 16; `pub fn auth_error_to_py(py: Python<'_>, message: String) -> PyErr` wraps it the same way.
   - `pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()>`: `let types = exception_types(m.py())?;` then `m.add("GoogleCloudError", types.google_cloud.bind(m.py()))?` and the other five names; call it from `src/rs/vertebra-py/src/lib.rs` after `completion::register`.
   - `src/rs/vertebra-py/Cargo.toml` `[dependencies]`: `google-cloud-gax`, `google-cloud-rpc`, `google-cloud-wkt = { version = "1", default-features = false }` (add to `[workspace.dependencies]` too), each `{ workspace = true }`.
   - `completion.rs::spawn_rust_db_task`: `EMap: FnOnce(Python<'_>, E) -> PyErr + Send + 'static`; the producer calls `map_err(py, err)`. Update the three `t01` tests to pass `|_, e| pyo3::exceptions::PyRuntimeError::new_err(e)`.
   - Bindings: sync methods map with `|e| spanner_error_to_py(py, e)` / `bigquery_error_to_py(py, e)` (remove `.map_err(|e| e.to_string())` inside `block_on_db` and the outer `PyRuntimeError::new_err`); parameter-parse errors use the same mappers; async methods pass `spanner_error_to_py` / `bigquery_error_to_py`; `PyGcpTokenCache::{get_token, get_token_async}` use `auth_error_to_py`. Delete `spanner_err_to_py` and `bq_err_to_py`. `GcpTokenCache(options)` construction keeps `ValueError` for invalid options.

3. **Stub (`src/py/vertebra/_vertebra.pyi`).** Add `from vertebra.typing import VertebraError` to the stub imports, the classes from spec §2.2 (each with the docstring used in `new_type`), and their names in `__all__`.

4. **Python drivers.**
   - `db/spanner/core.py`: `_PARTITIONED_QUERY_FALLBACK_CODES: Final[frozenset[int]] = frozenset({3, 7, 9, 12, 14})`; replace `is_partitioned_arrow_fallback_error` with `is_partitioned_query_fallback_error(exc: BaseException) -> bool` returning `isinstance(exc, SpannerError) and getattr(exc, "code", None) in _PARTITIONED_QUERY_FALLBACK_CODES`; the Google docstring explains each code (non-partitionable query, Data Boost not permitted, failed precondition, PartitionQuery unimplemented, endpoint unavailable). Import `SpannerError` in the existing `from vertebra._vertebra import ...` block.
   - `db/spanner/driver.py`: both `fetch_arrow` methods call `SpannerCore.is_partitioned_query_fallback_error`; both `run_in_transaction` methods replace `except Exception as exc: if attempt >= attempts or "aborted" not in str(exc).lower(): raise` with `except SpannerAbortedError: if attempt >= attempts: raise` (sync keeps `_time.sleep(0.01 * attempt)`).
   - `db/bigquery/core.py`: `_STORAGE_API_FALLBACK_CODES: Final[frozenset[int]] = frozenset({7, 9, 12, 14})`; `is_storage_api_fallback_error` returns `isinstance(exc, BigQueryError) and getattr(exc, "code", None) in _STORAGE_API_FALLBACK_CODES`.

5. **Refactor & verify.**

## Verification

- RED: commands in Step 1 fail as described.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-py && cargo clippy -p vertebra-py --all-targets -- -D warnings && cargo fmt --check`
  - `make dev && uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/extensions/test_sqlspec_spanner.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q`
  - `uv run python -m mypy.stubtest vertebra._vertebra`
  - `! grep -nE "str\(exc\)\.lower\(\)|in msg$|for token in" src/py/vertebra/db/spanner/core.py src/py/vertebra/db/spanner/driver.py src/py/vertebra/db/bigquery/core.py`
- Phase gate: `make lint-all && make rs-test && make py-test`.
- Commit: `git commit -S -m "feat(rs-py): raise typed Google Cloud exceptions with gRPC status codes"`.

## Acceptance Criteria

- [ ] R8: sync and async bindings raise the same typed exceptions rooted at `VertebraError` with the documented attributes.
- [ ] R9: Python fallbacks and retries use codes and classes; no substring matching remains.

## Notes & Discoveries
