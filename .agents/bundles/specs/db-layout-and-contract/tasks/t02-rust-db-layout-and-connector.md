---
type: Task
id: db-layout-and-contract:t02
title: Reorganize vertebra-db shared core, add connection.rs + py_async.rs + ShmCapsule, and register DatabaseError hierarchy
description: "Reorganize `src/rs/vertebra-db/src/` into `<engine>/mod.rs` subdirectories with explicit `lib.rs` exports, implement the shared Rust transport/TLS/dialer connection core in `connection.rs`, add optional `pyo3`-gated `py_async.rs` (`spawn_rust_db_task_with_attr`) and `#[repr(C)] ShmCapsule` (`ArrowCursor._from_shm_capsule`) so per-driver `.so` wheels share completion rings and zero-copy `ArrowCursor` handoff cleanly, and register the unified `DatabaseError` exception hierarchy on `vertebra._vertebra`, `vertebra.db.driver`, and `vertebra.db`."
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-layout-and-contract:t01
files:
  - .agents/skills/review-core/references/areas.toml
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - src/rs/vertebra-core/src/shm.rs
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/build.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-db/src/gcp.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-db/src/bigquery/mod.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/mssql/mod.rs
  - src/rs/vertebra-db/src/mysql/mod.rs
  - src/rs/vertebra-db/src/oracle/mod.rs
  - src/rs/vertebra-db/src/oracle/tns.rs
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-db/src/resp3/mod.rs
  - src/rs/vertebra-db/src/spanner/mod.rs
  - src/rs/vertebra-db/src/sqlite/mod.rs
  - src/rs/vertebra-py/Cargo.toml
  - src/rs/vertebra-py/src/completion.rs
  - src/rs/vertebra-py/src/db.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/db/driver/__init__.py
tests:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/db.rs
  - src/rs/vertebra-py/src/errors.rs
  - src/py/tests/unit/db/test_native_db_drivers.py
plan_revision: 5
plan_commit: "2c0d365"
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-05T00:35:59Z"
last_verified_commit: "4ce057e5aff909fcda2488cc0f6c4882ad0d38f0"
verification_evidence: "make py-test (1738 passed, 69 skipped, 170 xfailed); make rs-test (473 passed, 1 ignored); make ts-test; make lint-all; make docs; isolated wheel and sdist rebuild/install/uninstall probes (exit 0). CARGO_TARGET_DIR and TMPDIR use disk-backed paths to avoid host temporary-filesystem quota."
created_at: 2026-11-03T21:40:00Z
updated_at: "2026-11-05T00:35:59Z"
commit: "2fa781f"
tags: [rust, vertebra-db, pyo3, connection, errors]
---

## Objective

Reorganize `src/rs/vertebra-db/src/` into `<engine>/mod.rs` subdirectories with explicit `lib.rs` exports, implement the shared Rust transport/TLS/dialer connection core in `connection.rs`, add optional `pyo3`-gated `py_async.rs` (`spawn_rust_db_task_with_attr`) and `#[repr(C)] ShmCapsule` (`ArrowCursor._from_shm_capsule`) so per-driver `.so` wheels share completion rings and zero-copy `ArrowCursor` handoff cleanly, and register the unified `DatabaseError` exception hierarchy on `vertebra._vertebra`, `vertebra.db.driver`, and `vertebra.db`.

## Context

- **Verification strategy (`behavior_tdd`)**: Loopback TCP, Unix socket, TLS, and AlloyDB metadata-exchange `TransportStream::connect` handshakes, per-extension `attr_name` drainer isolation on one Python event loop, zero-copy `ShmCapsule` `ArrowCursor` buffer transfer, and Python exception multi-inheritance (`NotFoundError(DataError, ValueError)`, `SpannerError(GoogleCloudError, DatabaseError)`, `SpannerAbortedError(SpannerError, SerializationFailure)`) are deterministic unit-testable behaviors.
- **Architectural invariants**:
  - `src/rs/Cargo.toml` provides `tokio-rustls = "0.26"`, `rustls-pemfile = "2"`, `webpki-root-certs = "1"`, and `pyo3 = "0.30.1"`; define `install_rustls_ring_provider()` directly in `connection.rs` (re-exported from `gcp.rs`) so `connection.rs` works when `gcp` is disabled in `t03`.
  - Optional `pyo3 = ["dep:pyo3"]` feature on `vertebra-db`: `src/rs/vertebra-db/src/py_async.rs` provides `db_runtime_handle`, `block_on_db`, `RustDbTaskAwaitable`, `construct_py_arrow_cursor`, and `spawn_rust_db_task_with_attr(..., attr_name: &'static str)` over `AsyncCompletionQueue` + `Notifier`, resolving `TaskPanicError` dynamically from `vertebra._vertebra` (falling back to `PyRuntimeError` in standalone Rust tests when `vertebra._vertebra` is not registered). `vertebra-py/src/completion.rs` delegates to `vertebra_db::py_async` with `attr_name = "_vertebra_core_completion_drainer"`.
  - Cross-`cdylib` zero-copy `ArrowCursor` handoff: `vertebra_core::shm::ShmCapsule` (`#[repr(C)] { ptr: *mut u8, len: usize, ctx: *mut c_void, drop_fn: unsafe extern "C" fn(*mut c_void) }`) wraps an `Arc<ShmRegion>` allocated by any driver `.so` (`ShmArrowStreamWriter::finish_unregistered` / `write_shm_arrow_region_unregistered`, without inserting into the driver `.so`'s private `DashMap`) and hands it via `PyCapsule` (`c"vertebra.ShmCapsule"`) to `vertebra._vertebra.ArrowCursor._from_shm_capsule(shm_name, byte_len, row_count, capsule)` so `ArrowCursor` anchors and drops the mapping through the originating `.so`'s `drop_fn` (`ShmRegion::unlink_now` unlinking `/dev/shm` on `release()` while keeping the `mmap` valid until `ArrowCursor` drops).
  - Clean break in Rust (`product-guidelines.md`): leave zero `tns` compatibility re-exports in `src/rs/vertebra-db/src/lib.rs`; update `src/rs/vertebra-py/src/db.rs` to `use vertebra_db::oracle::tns::{parse_tnsnames_ora, resolve_tns_alias}`.
  - Construct multi-base Python exception types in `src/rs/vertebra-py/src/errors.rs` via `py.get_type::<PyType>().call1((name, (base_a, base_b), dict))?.cast_into::<PyType>()` (zero `unsafe`), setting `__module__ = "vertebra._vertebra"`, `__doc__`, and class-level `sqlstate`, `vendor_code`, and `is_transient` defaults.

## Steps

1. **Write failing Rust and Python unit tests (RED)**:
   - Add Rust unit tests in `src/rs/vertebra-db/src/connection.rs` covering loopback TCP, Unix socket (`#[cfg(unix)]`), `tokio-rustls` TLS (`VerifyFull` + hostname mismatch rejection), AlloyDB `:5433` mTLS + `MetadataExchangeRequest` handshake, and `TransportError::{is_transient, sqlstate}`.
   - Add Rust unit tests in `src/rs/vertebra-db/src/py_async.rs` verifying two callers using distinct `attr_name` keys (`"_test_drainer_a"` and `"_test_drainer_b"`) coexist on the same `asyncio` event loop without type-cast collision, plus panic (`TaskPanicError` / `RuntimeError`) and task cancellation propagation.
   - Add Rust/Python tests in `src/rs/vertebra-py/src/db.rs`, `src/rs/vertebra-py/src/errors.rs`, and `src/py/tests/unit/db/test_native_db_drivers.py` asserting:
     - `ArrowCursor._from_shm_capsule` exposes zero-copy `memoryview(cursor)` bytes and unlinks the POSIX `/dev/shm` entry on `cursor.release()`.
     - Every `DatabaseError` subclass (`InterfaceError`, `DataError`, `NotFoundError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`, `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) is exported on `vertebra._vertebra`, `vertebra.db.driver`, and `vertebra.db` with `sqlstate`, `vendor_code`, and `is_transient`.
     - `issubclass(NotFoundError, (DataError, DatabaseError, VertebraError, ValueError))`, `issubclass(SpannerError, (GoogleCloudError, DatabaseError))`, `issubclass(SpannerAbortedError, (SpannerError, SerializationFailure))`, `issubclass(BigQueryError, (GoogleCloudError, DatabaseError))`, and `issubclass(BigQueryRowError, (BigQueryError, DataError))` are `True`.

2. **Reorganize `vertebra-db`, implement `connection.rs` + `py_async.rs` + `ShmCapsule`, and register `DatabaseError` (GREEN)**:
   - Add `ShmCapsule` and `#[cfg(unix)] pub fn unlink_now(&self)` on `ShmRegion` in `src/rs/vertebra-core/src/shm.rs`, and add `write_shm_arrow_region_unregistered` + `ShmArrowStreamWriter::finish_unregistered` in `src/rs/vertebra-db/src/arrow_shm.rs`.
   - Add `pyo3 = { workspace = true, optional = true }`, `rustls-pemfile = { workspace = true }`, `tokio-rustls = { workspace = true }`, and `webpki-root-certs = { workspace = true }` to `src/rs/vertebra-db/Cargo.toml` (`[features] pyo3 = ["dep:pyo3"]`), and enable `vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }` in `src/rs/vertebra-py/Cargo.toml`.
   - Move `src/rs/vertebra-db/src/{postgres,mysql,sqlite,oracle,mssql,duckdb,resp3,bigquery,spanner}.rs` to `src/rs/vertebra-db/src/<engine>/mod.rs` and `tns.rs` to `oracle/tns.rs`, replace glob `pub use *` in `lib.rs` with explicit exports, update `vertebra-py/src/db.rs` and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and add `"src/rs/vertebra-db/**"` to `[areas.runtime-ipc].paths` in `.agents/skills/review-core/references/areas.toml`.
   - Implement `src/rs/vertebra-db/src/connection.rs` (`install_rustls_ring_provider`, `TlsMode`, `ConnectOptions`, `TransportError`, `EndpointTarget`, `TransportStream`, `CancelHandle`, `Connection`) and `src/rs/vertebra-db/src/py_async.rs` (`db_runtime_handle`, `block_on_db`, `RustDbTaskAwaitable`, `spawn_rust_db_task_with_attr`, `construct_py_arrow_cursor`), updating `src/rs/vertebra-py/src/completion.rs` and `src/rs/vertebra-py/src/db.rs` (`ArrowCursor._from_shm_capsule`).
   - Extend `src/rs/vertebra-py/src/errors.rs` (`DbExceptionTypes` + multi-base `GcpExceptionTypes`, `db_error_to_py`), update `src/py/vertebra/_vertebra.pyi`, and re-export all `DatabaseError` classes in `src/py/vertebra/db/driver/__init__.py` and `src/py/vertebra/db/__init__.py`.

3. **Refactor & verify**:
   - Rebuild debug extensions and verify `cargo clippy`, `mypy.stubtest`, Rust tests (`vertebra-core`, `vertebra-db`, `vertebra-py`), Python unit tests, and `make review-check`.

## Verification

- **Initial (RED)**:
  - `cargo test -p vertebra-db` / `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py` -> fails before `connection.rs`, `py_async.rs`, `ShmCapsule`, and `DatabaseError` exist.
- **Final (GREEN)**:
  - `make dev && cargo test -p vertebra-core && cargo test -p vertebra-db --features pyo3 && cargo test -p vertebra-py` -> exits `0`.
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py` -> exits `0`.
  - `make lint-all && make review-check` -> exits `0`.

## Acceptance Criteria

- [x] R3: `src/rs/vertebra-db/src/` uses `<engine>/mod.rs` (`oracle/tns.rs`), `lib.rs` contains zero wildcard `pub use *` items or `tns` compat re-exports, `areas.toml` routes `src/rs/vertebra-db/**`, `connection.rs` passes TCP/Unix/`tokio-rustls`/AlloyDB tests, and `py_async.rs` + `ShmCapsule` pass multi-drainer and zero-copy `ArrowCursor` tests.
- [x] R4: All `DatabaseError` subclasses are registered in `vertebra._vertebra`, `NotFoundError` inherits `(DataError, ValueError)`, `SpannerError`/`BigQueryError` satisfy both `GoogleCloudError` and `DatabaseError`, and `mypy.stubtest` passes cleanly.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->

- [2026-11-04 14:42] Observed expected RED failure via `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py -q` (`exit code 1`: `16 failed, 21 passed`) before implementing `DatabaseError` subclasses and `ArrowCursor._from_shm_capsule`.
- [2026-11-04 15:08] Added `src/rs/vertebra-db/build.rs` (`CARGO_FEATURE_PYO3`-gated `pyo3_build_config::use_pyo3_cfgs()` and `-Wl,-rpath,{lib_dir}`, matching `vertebra-http/build.rs`) and `Python::initialize()` in `py_async::tests` so standalone `vertebra-db` builds without `--features pyo3` link zero Python symbols while `--features pyo3` test binaries locate standalone `uv` `libpython` cleanly.
- [2026-11-04 15:10] Implemented cross-`.so` `PyCapsule` (`c"vertebra.ShmCapsule"`) ownership transfer in `vertebra_db::py_async::wrap_shm_region_capsule` with `shm_pycapsule_destructor` so `Box<ShmCapsule>` and `Arc<ShmRegion>` (`drop_fn`) always deallocate inside the producing `.so`'s allocator, while `ArrowCursor._from_shm_capsule` atomically zeroes `cap_ref.ctx` to reject double consumption.
- [2026-11-04 15:15] Subagent sandbox permission checks blocked unsandboxed `git mv` and `cargo test`; verified all in-sandbox Python/OKF gates (`ruff check`, `ruff format --check`, `mypy`, `scan_public_docstrings --check-leaks --check-overloads --check-typeddicts`, `review_setup validate` -> all exit `0`). Parent orchestrator completion sequence: (a) run `git mv` for `{postgres,mysql,sqlite,oracle,mssql,duckdb,resp3,bigquery,spanner}.rs` -> `<engine>/mod.rs` and `tns.rs` -> `oracle/tns.rs`, (b) move `pub mod tns;` from `src/rs/vertebra-db/src/lib.rs` into `src/rs/vertebra-db/src/oracle/mod.rs` and switch `use vertebra_db::tns::` to `use vertebra_db::oracle::tns::` in `src/rs/vertebra-py/src/db.rs`, (c) update the moved `<engine>/mod.rs` and `oracle/tns.rs` resource paths in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and (d) run `make dev`, `cargo test`, `mypy.stubtest`, `pytest`, `make lint-all`, `make review-check`, and `git commit`.

- [2026-11-05T00:35:59Z] Review checkpoint `4ce057e`: current source passes aggregate verification; installed-wheel checks cover direct and source-distribution builds. Correctness findings are resolved. Fresh quality review precedes completion.
