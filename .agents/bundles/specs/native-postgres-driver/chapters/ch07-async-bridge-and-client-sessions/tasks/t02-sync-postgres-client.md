---
type: Task
id: "native-postgres-driver:ch07:t02"
chapter_id: ch07-async-bridge-and-client-sessions
title: Expose physical synchronous native PostgreSQL sessions
description: Implement PySyncPostgresClient (SyncPostgresClient in vertebra._postgres) and _postgres.pyi stubs with the complete 25-parameter constructor, GIL-detached connect/close lifecycle, copied parameter snapshots, and canonical core exception mapping.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch07:t01"
read_set:
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-spanner/src/lib.rs
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
estimated_loc: 240
plan_revision: 5
plan_commit: 0147d80c
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: adf34226
sources:
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::block_on_db
    title: Synchronous Tokio runtime driver helper block_on_db
  - id: spanner-py
    resource: src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient
    title: Reference PyO3 conditional freelist, frozen, and immutable_type attributes
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: Canonical PostgreSQL driver unit test module
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Complete 25-parameter SyncPostgresClient constructor and lifecycle contract
tags:
  - postgres
  - pyo3
  - sync
  - client
---

# Task t02: Expose Physical Synchronous Native PostgreSQL Sessions

## Objective & Scope Boundary

Implement `PySyncPostgresClient` (`SyncPostgresClient` in `vertebra._postgres`) in `src/rs/vertebra-postgres/src/sync_client.rs`, register it in `src/rs/vertebra-postgres/src/lib.rs`, and publish its exact `@final` `def __new__(cls, *, ...) -> SyncPostgresClient` type signature in `src/py/vertebra/_postgres.pyi` (preserving `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool`) [^py-async-rs] [^spanner-py] [^pg-contracts]. `SyncPostgresClient` exposes the complete 25-parameter keyword-only constructor from `contracts.md` (pure validation and `Mapping[str, str]` snapshot copy without network I/O), drives `connect()` and `close()` inside `py.detach(|| block_on_db(...))`, exposes `closed: bool` and copied `parameters: dict[str, str]` without leaking `BackendKeyData`, and converts `PostgresError` through `ch05:t02` `errors.rs::postgres_error_to_py` while attached to Python [^pg-driver-tests]. `AsyncPostgresClient` belongs to `ch07:t03`, and exhaustive constructor characterization belongs to `ch08:t01`.

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `BeginConnect::{Attempt(ConnectAttempt), AlreadyReady}`, `PendingSession::commit`, `close()`, `is_closed()`, and `parameters()`.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError` (`PostgresError::Server(ServerError)`) and `postgres_error_to_py` (`PyOnceLock` cache of `vertebra._vertebra` exception classes from `ch05:t02`).
- `src/rs/vertebra-db/src/py_async.rs` — `block_on_db` runtime bridge [^py-async-rs].
- `src/rs/vertebra-spanner/src/lib.rs` — `#[cfg_attr(not(Py_GIL_DISABLED), pyclass(..., freelist = 128))]` and `#[cfg_attr(Py_GIL_DISABLED, pyclass(...))]` free-threaded pattern [^spanner-py].
- `src/py/tests/unit/db/test_postgres_driver.py` — Existing unit test module to extend with `native_sync_*` tests and reusable loopback PostgreSQL socket server fixture [^pg-driver-tests].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs`:
  - Annotate `PySyncPostgresClient`:
    ```rust
    #[cfg_attr(
        not(Py_GIL_DISABLED),
        pyclass(name = "SyncPostgresClient", module = "vertebra._postgres", frozen, immutable_type, freelist = 128)
    )]
    #[cfg_attr(
        Py_GIL_DISABLED,
        pyclass(name = "SyncPostgresClient", module = "vertebra._postgres", frozen, immutable_type)
    )]
    pub struct PySyncPostgresClient {
        session: Arc<SessionHandle>,
    }
    ```
  - Provide crate-shared `pub(crate) fn extract_string_map(obj: Option<&Bound<'_, PyAny>>, field_name: &str) -> PyResult<Option<HashMap<String, String>>>` iterating `PyMapping::items()` while attached and raising `PyTypeError` if the object is not a mapping or any key/value is not an exact Python `str` (`extract::<String>()` without `str()` coercion).
  - `#[new]` with `#[pyo3(signature = (*, user, database, host = "localhost", port = 5432, password = None, unix_socket = None, sslmode = "verify-full", ca_pem = None, client_cert_pem = None, client_key_pem = None, server_name = None, channel_binding = "prefer", connect_timeout = 10.0, application_name = "vertebra", session_gucs = None, keepalives = true, keepalives_idle = None, keepalives_interval = None, keepalives_count = None, cloud_sql_instance = None, cloud_sql_ip_type = "PRIVATE", alloydb_instance_uri = None, alloydb_ip_type = "PRIVATE", enable_iam_auth = false, cloud_auth_options = None, iam_auth_options = None))]`:
    - Extracts owned primitives and copied `HashMap<String, String>` maps while attached, validates through `PostgresConfig`, and constructs `SessionHandle::new(config)` in `Configured` (`closed == true`, `parameters == {}`) with zero network/file/DNS I/O.
  - `pub fn connect(&self, py: Python<'_>) -> PyResult<()>`:
    - Calls `self.session.begin_connect().map_err(|e| postgres_error_to_py(py, e))?`.
    - For `BeginConnect::AlreadyReady`, returns `Ok(())`.
    - For `BeginConnect::Attempt(attempt)`, runs `let pending = py.detach(|| block_on_db(attempt.run())).map_err(|e| postgres_error_to_py(py, e))?;` and calls `pending.commit().map_err(|e| postgres_error_to_py(py, e))`.
  - `pub fn close(&self, py: Python<'_>) -> PyResult<()>`:
    - Synchronously transitions `SessionHandle` to `Closed` and extracts any live transport (raising `InterfaceError` if `Connecting`), then performs bounded `Terminate`/shutdown cleanup inside `py.detach(|| block_on_db(...))`. Idempotent when `Configured` or already `Closed`.
  - `#[getter] pub fn closed(&self) -> bool` and `#[getter] pub fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>>` (allocates a new `PyDict` from `session.parameters()` on every access; never exposes `BackendKeyData` `process_id` or `secret_key`).
- `src/rs/vertebra-postgres/src/lib.rs` & `src/py/vertebra/_postgres.pyi`:
  - Register `m.add_class::<sync_client::PySyncPostgresClient>()?;` while keeping `__version__`, `FREE_THREADED`, and `GIL_USED` on `vertebra._postgres`.
  - In `src/py/vertebra/_postgres.pyi`, preserve `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool`, update `__all__`, and declare `@final class SyncPostgresClient:` with `def __new__(cls, *, ...) -> SyncPostgresClient` (not `def __init__`, matching PyO3 `#[pyclass(frozen, immutable_type)]` `mypy.stubtest` rules), `connect`, `close`, `closed`, and `parameters` annotations and Google-style docstrings.

## Edge Cases & Error Semantics

- Calling `connect()` after a failed `connect()` or after `close()` raises exact `vertebra._vertebra.InterfaceError` (`isinstance(exc, vertebra.db.exceptions.InterfaceError)`).
- Server `ErrorResponse` `28P01` (`PostgresError::Server(err)` where `err.sqlstate == "28P01"`) during `connect()` raises exact `vertebra._vertebra.DatabaseError` with `exc.sqlstate == "28P01"`; `08006` / EOF / timeout raises exact `vertebra._vertebra.ConnectionFailure`; static/IAM provider rejection raises exact `vertebra._vertebra.GoogleCloudAuthError` with `getattr(exc, "sqlstate", None) is None`.
- Mutating the returned `client.parameters` dict in Python (`params["server_version"] = "mutated"`) leaves a subsequent `client.parameters` read untouched, and `parameters` remains populated after `client.close()`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_driver.py` (`test_native_sync_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Pure construction, connect, Ready no-op & idempotent close (`test_native_sync_lifecycle`) | Loopback socket server fixture speaking PostgreSQL 3.0 trust + `ParameterStatus("client_encoding"="UTF8", "server_version"="17.2")` + `BackendKeyData` + `ReadyForQuery` (`TransactionStatus::Idle`, `b'I'`) | Before `connect()`: `client.closed is True`, `client.parameters == {}`, `0` server accepts; after `connect()`: `client.closed is False`, `client.parameters["server_version"] == "17.2"`, `!hasattr(client, "backend_key")` / `!hasattr(client, "secret_key")`; second `connect()` is no-op; `close()` twice sends `Terminate` once, `client.closed is True`, `connect()` after `close()` raises `InterfaceError`. |
| Parameter dict copy isolation | Mutate `d = client.parameters; d["server_version"] = "0.0"` | Fresh `client.parameters["server_version"] == "17.2"` both before and after `client.close()`. |
| GIL release during blocking `connect()` (`test_native_sync_releases_gil`) | Server holds `AuthenticationOk` until a second Python thread sets `threading.Event` while thread 1 is blocked inside `client.connect()` | Progress thread sets `Event` immediately without deadlock, unblocking server greeting and completing `client.connect()`. |
| Core exception identity (`28P01`, `08006`, `GoogleCloudAuthError`) | Server returns `ErrorResponse` `28P01`, `08006` / abrupt EOF, or expired static `iam_auth_options` | Raises exact `vertebra._vertebra.DatabaseError` (`sqlstate == "28P01"`), `vertebra._vertebra.ConnectionFailure`, and `vertebra._vertebra.GoogleCloudAuthError` respectively, leaving `client.closed is True`. |

## Verification

- Initial Red baseline:
  - `make dev && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_sync -q`
- Final Green + Stubtest gate:
  - `make dev`
  - `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_sync -q`
  - `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres`

## Acceptance Criteria

- [x] `SyncPostgresClient` is exported from `vertebra._postgres` (`frozen`, `immutable_type`, conditional `freelist`) with the exact 25-parameter keyword-only signature in `_postgres.pyi` passing `mypy.stubtest` (R6).
- [x] `connect()` and `close()` release the GIL during blocking runtime waits, enforce `Configured -> Connecting -> Ready -> Closed` lifecycle rules, copy `parameters` without exposing backend keys, and map errors to exact `vertebra._vertebra` exception classes (R6).
- [x] Commit `feat(postgres): expose synchronous physical connection client` passes `pytest -k native_sync`, `stubtest`, and `cargo test` (R6).

## Notes & Discoveries

- None.

[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::block_on_db` — Shared synchronous Tokio runtime bridge.
[^spanner-py]: `src/rs/vertebra-spanner/src/lib.rs::PySyncSpannerClient` — Reference PyO3 `freelist` and `immutable_type` configuration.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — Canonical PostgreSQL driver unit test suite.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public `SyncPostgresClient` constructor and error contracts.
