---
type: Task
id: db-layout-and-contract:t03
title: Extract vertebra-bigquery and vertebra-spanner standalone PyO3 wheels and optional install extras
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-layout-and-contract:t01
  - db-layout-and-contract:t02
files:
  - Makefile
  - pyproject.toml
  - uv.lock
  - .agents/skills/review-core/references/areas.toml
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - docs/reference/native-stubs.md
  - src/rs/Cargo.toml
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-db/src/gcp.rs
  - src/rs/vertebra-py/Cargo.toml
  - src/rs/vertebra-py/src/errors.rs
  - src/rs/vertebra-py/src/lib.rs
  - src/rs/vertebra-bigquery/Cargo.toml
  - src/rs/vertebra-bigquery/build.rs
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bigquery/src/lib.rs
  - src/rs/vertebra-bigquery/src/client.rs
  - src/rs/vertebra-bigquery/src/errors.rs
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/build.rs
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-spanner/src/lib.rs
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/_bigquery.pyi
  - src/py/vertebra/_spanner.pyi
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/spanner/driver.py
tests:
  - src/rs/vertebra-bigquery/src/client.rs
  - src/rs/vertebra-bigquery/src/errors.rs
  - src/rs/vertebra-spanner/src/client.rs
  - src/rs/vertebra-spanner/src/errors.rs
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_native_db_drivers.py
plan_revision: 4
plan_commit: "679a981"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-04T01:05:00Z
updated_at: 2026-11-04T01:05:00Z
commit: null
tags: [rust, pyo3, wheels, bigquery, spanner, packaging]
---

## Objective

Extract BigQuery (`SyncBigQueryClient`, `AsyncBigQueryClient`) and Spanner (`SyncSpannerClient`, `AsyncSpannerClient`) Rust clients and PyO3 bindings out of `vertebra-db` and `vertebra-py` into standalone optional PyO3 extension crates `src/rs/vertebra-bigquery` (`vertebra._bigquery`) and `src/rs/vertebra-spanner` (`vertebra._spanner`), gate `vertebra_db::gcp` behind optional Cargo feature `gcp`, remove all `google-cloud-*` and `tonic` dependencies from `vertebra-py/Cargo.toml` and default `vertebra-db/Cargo.toml`, and wire `[bigquery]`, `[spanner]`, and `[storage]` install extras, `Makefile` build/stubtest targets, `areas.toml`, and `docs/reference/native-stubs.md`.

## Context

- **Verification strategy (`behavior_tdd`)**: Module exports (`vertebra._bigquery.{SyncBigQueryClient, AsyncBigQueryClient}`, `vertebra._spanner.{SyncSpannerClient, AsyncSpannerClient}`), clean-break removal from `vertebra._vertebra`, `MissingDependencyError` diagnostics when an optional wheel is absent, cross-`.so` `ArrowCursor` (`ShmCapsule`) and `GoogleCloudError` + `DatabaseError` exception raising, `cargo tree -p vertebra-py -p vertebra-db` dependency purity, and `mypy.stubtest` parity are deterministic and directly verifiable.
- **Architectural invariants**:
  - Match `src/rs/vertebra-storage/{Cargo.toml,pyproject.toml}` and `src/rs/vertebra-py/build.rs`:
    - `src/rs/vertebra-bigquery`: `[lib] name = "_bigquery"; crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)] fn _bigquery`, `pyproject.toml` with `name = "vertebra-bigquery"`, `module-name = "vertebra._bigquery"`, `python-source = "../../py"`, `features = ["extension-module"]`.
    - `src/rs/vertebra-spanner`: `[lib] name = "_spanner"; crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)] fn _spanner`, `pyproject.toml` with `name = "vertebra-spanner"`, `module-name = "vertebra._spanner"`, `python-source = "../../py"`, `features = ["extension-module"]`.
  - Lift `GcpErrorInfo`, `code_name`, `code_number`, `http_code`, and `gax_error_info` into `src/rs/vertebra-db/src/gcp.rs` behind `#[cfg(feature = "gcp")]` (`gcp = ["dep:google-cloud-auth", "dep:google-cloud-gax", "dep:google-cloud-rpc", "dep:google-cloud-wkt"]`). Keep all Python exception class registrations (`DatabaseError` + `GoogleCloudError` hierarchy) in `vertebra-py/src/errors.rs` (`vertebra._vertebra`) using pure PyO3 (`0` `google-cloud-*` crates in `vertebra-py/Cargo.toml`), while `vertebra-bigquery/src/errors.rs` and `vertebra-spanner/src/errors.rs` resolve those exception classes from `vertebra._vertebra` via `PyOnceLock` (with stand-in base helpers for standalone `cargo test`).
  - Use `spawn_rust_db_task_with_attr(..., "_vertebra_bigquery_completion_drainer")` in `vertebra-bigquery` and `"_vertebra_spanner_completion_drainer"` in `vertebra-spanner`, and return `vertebra_db::py_async::construct_py_arrow_cursor` (`ShmCapsule`) from `SyncBigQueryClient::read_table_arrow` and `AsyncBigQueryClient::read_table_arrow`.
  - Clean break (`PY-07`): delete `src/rs/vertebra-py/src/{bigquery,spanner}.rs` and remove `SyncBigQueryClient`, `AsyncBigQueryClient`, `SyncSpannerClient`, and `AsyncSpannerClient` from `vertebra._vertebra` and `src/py/vertebra/_vertebra.pyi`.

## Steps

1. **Write failing Python and Rust packaging & isolation tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add tests asserting:
     - `vertebra._bigquery` exposes `SyncBigQueryClient` and `AsyncBigQueryClient` (`hasattr(vertebra._vertebra, "SyncBigQueryClient") is False`), and `vertebra._spanner` exposes `SyncSpannerClient` and `AsyncSpannerClient` (`hasattr(vertebra._vertebra, "SyncSpannerClient") is False`).
     - When `vertebra._bigquery` or `vertebra._spanner` is absent from `sys.modules` (`monkeypatch.setitem(sys.modules, "vertebra._bigquery", None)` + `reset_dependency_cache("vertebra._bigquery")`), `SyncBigQueryDriver._ensure_client()` / `SyncSpannerDriver._ensure_client()` raises `MissingDependencyError` naming `vertebra-core[bigquery]` / `vertebra-core[spanner]`.
     - `src/rs/vertebra-py/Cargo.toml` and default `src/rs/vertebra-db/Cargo.toml` `[dependencies]` do not enable `google-cloud-bigquery`, `google-cloud-spanner`, or `tonic` unconditionally.
   - Update `src/py/tests/unit/db/test_bigquery.py` and `src/py/tests/unit/db/test_spanner.py` native-client imports from `vertebra._vertebra` to `vertebra._bigquery` and `vertebra._spanner`.

2. **Extract `vertebra-bigquery` and `vertebra-spanner` crates and wire build/install metadata (GREEN)**:
   - Update `src/rs/vertebra-db/Cargo.toml` and `src/rs/vertebra-db/src/{lib,gcp}.rs`: gate `gcp` behind `gcp = ["dep:google-cloud-auth", "dep:google-cloud-gax", "dep:google-cloud-rpc", "dep:google-cloud-wkt"]`, lift `GcpErrorInfo`, `code_name`, `code_number`, `http_code`, and `gax_error_info` into `gcp.rs`, and move `src/rs/vertebra-db/src/bigquery/mod.rs` -> `src/rs/vertebra-bigquery/src/client.rs` (moving any `vertebra-db/tests/*bigquery*` tests into `src/rs/vertebra-bigquery/`) and `src/rs/vertebra-db/src/spanner/mod.rs` -> `src/rs/vertebra-spanner/src/client.rs` (moving any `vertebra-db/tests/*spanner*` tests into `src/rs/vertebra-spanner/`), deleting `google-cloud-bigquery`, `google-cloud-spanner`, `google-cloud-longrunning`, `google-cloud-spanner-admin-database-v1`, `tonic`, `prost`, and `prost-types` from `src/rs/vertebra-db/Cargo.toml`.
   - Create `src/rs/vertebra-bigquery/{Cargo.toml,build.rs,pyproject.toml,src/lib.rs,src/client.rs,src/errors.rs}` and `src/rs/vertebra-spanner/{Cargo.toml,build.rs,pyproject.toml,src/lib.rs,src/client.rs,src/errors.rs}` (moving PyO3 bindings and error-mapping unit tests from `src/rs/vertebra-py/src/{bigquery,spanner,errors}.rs`), add `"vertebra-bigquery"` and `"vertebra-spanner"` to `src/rs/Cargo.toml` `members`, delete `src/rs/vertebra-py/src/{bigquery,spanner}.rs`, and remove `google-cloud-gax`, `google-cloud-rpc`, and `google-cloud-wkt` from `src/rs/vertebra-py/Cargo.toml`.
   - Create `src/py/vertebra/_bigquery.pyi` and `src/py/vertebra/_spanner.pyi` (removing `SyncBigQueryClient`, `AsyncBigQueryClient`, `SyncSpannerClient`, and `AsyncSpannerClient` from `src/py/vertebra/_vertebra.pyi`), update `src/py/vertebra/db/adapters/bigquery/driver.py` (`ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")`) and `src/py/vertebra/db/adapters/spanner/driver.py` (`ensure_module("vertebra._spanner", package="vertebra-core[spanner]", extra="spanner")`).
   - Update `pyproject.toml` (`[project.optional-dependencies]` adding `bigquery = ["vertebra-bigquery==0.3.0"]`, `spanner = ["vertebra-spanner==0.3.0"]`, `storage = ["vertebra-storage==0.3.0"]`, and including them in `all`; `[tool.uv.sources]` path entries for `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-storage`; and `[[tool.bumpversion.files]]` entries for `src/rs/vertebra-{bigquery,spanner}/pyproject.toml` plus the `=={current_version}` pins in `pyproject.toml`) and refresh `uv.lock` via `uv lock --offline`.
   - Update `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`) to exclude `vertebra-bigquery` and `vertebra-spanner` from non-extension `cargo build --workspace`, run `maturin develop --uv -m` for both crates, and run `mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner`; update `.agents/skills/review-core/references/areas.toml` (`[areas.pyo3-bridge].paths` adding `src/rs/vertebra-bigquery/**`, `src/rs/vertebra-spanner/**`, `src/py/vertebra/_bigquery.pyi`, `src/py/vertebra/_spanner.pyi`); and update `docs/reference/native-stubs.md` and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.

3. **Refactor & verify**:
   - Verify `cargo tree -p vertebra-py` and `cargo tree -p vertebra-db` contain zero `tonic` or `google-cloud-*` crates, build all 5 PyO3 extensions, and run `stubtest`, Rust tests, Python unit tests, `make docs`, and `make review-check`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py` -> fails before `vertebra._bigquery` and `vertebra._spanner` exist (`ModuleNotFoundError`).
- **Final (GREEN)**:
  - `make dev && cargo test -p vertebra-db -p vertebra-py -p vertebra-bigquery -p vertebra-spanner` -> exits `0`.
  - `uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner` -> exits `0`.
  - `uv run pytest src/py/tests/unit` -> exits `0`.
  - `make lint-all && make docs && make review-check` -> exits `0`.

## Acceptance Criteria

- [ ] R5: `vertebra-bigquery` (`vertebra._bigquery`) and `vertebra-spanner` (`vertebra._spanner`) compile as standalone `gil_used = false` PyO3 wheels with `[bigquery]`, `[spanner]`, and `[storage]` extras in `pyproject.toml` (`uv.lock` synced), `vertebra._vertebra` and default `vertebra-db` have zero `google-cloud-*` or `tonic` dependencies, `BigQuery` zero-copy `read_table_arrow` returns `vertebra._vertebra.ArrowCursor`, and `mypy.stubtest` passes across all 5 native extensions.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
