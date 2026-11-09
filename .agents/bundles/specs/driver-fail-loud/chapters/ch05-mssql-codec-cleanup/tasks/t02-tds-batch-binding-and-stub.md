---
type: Task
id: "driver-fail-loud:ch05:t02"
chapter_id: ch05-mssql-codec-cleanup
title: Remove the encode_tds_sql_batch binding and stub
description: Delete the encode_tds_sql_batch pyfunction, its registration, and its vertebra_db import from vertebra-py, and the matching _vertebra.pyi definition and __all__ entry, in one commit.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on:
  - "driver-fail-loud:ch05:t01"
read_set:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
files:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 20
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:35:00Z"
last_verified_commit: c87687c0
verification_evidence: "mypy.stubtest vertebra._vertebra (exit 0), cargo build -p vertebra-py"
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-09T03:35:00Z"
commit: c87687c0
sources:
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::encode_tds_sql_batch
    title: encode_tds_sql_batch pyfunction and register_db_bindings
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi::encode_tds_sql_batch
    title: Stub definition and __all__ entry
tags:
  - pyo3
  - mssql
  - cleanup
---

# Task t02: Remove the `encode_tds_sql_batch` Binding and Stub

## Objective & Scope Boundary

Delete the `encode_tds_sql_batch` PyO3 function from `src/rs/vertebra-py/src/db.rs` (definition, registration, and the `rs_encode_tds_sql_batch` import) and its stub from `src/py/vertebra/_vertebra.pyi` (definition and `__all__` entry) in one commit, so `mypy.stubtest vertebra._vertebra` stays at exit 0.[^db-bindings][^vertebra-pyi]

t01 already removed every Python caller, so no Python module references the binding when it leaves.

Out of scope: `vertebra_db::mssql::encode_tds_sql_batch` and other Rust TDS framing (t03); every other binding in `register_db_bindings`.

Precondition: `grep -rn "encode_tds_sql_batch" src/py/vertebra/db src/py/tests` prints nothing (t01 completed).

## Bounded Read Manifest

- `src/rs/vertebra-py/src/db.rs` — `use vertebra_db::mssql::{...}` import, `encode_tds_sql_batch` pyfunction, `register_db_bindings`.[^db-bindings]
- `src/py/vertebra/_vertebra.pyi` — `__all__` entry `"encode_tds_sql_batch"` and `def encode_tds_sql_batch(...)`.[^vertebra-pyi]

Grep-only verification: `grep -rn "encode_tds_sql_batch" src/py src/rs/vertebra-py` lists only these two files.

## Symbol & Signature Contracts

- `src/rs/vertebra-py/src/db.rs`:
  - Import becomes `use vertebra_db::mssql::rewrite_mssql_placeholders as rs_rewrite_mssql_placeholders;`.
  - Delete the doc comment, `#[pyfunction]`, `#[pyo3(signature = (sql, tx_descriptor=0))]`, and body of `pub fn encode_tds_sql_batch<'py>(py: Python<'py>, sql: &str, tx_descriptor: u64) -> PyResult<Bound<'py, PyBytes>>`.
  - Delete `m.add_function(wrap_pyfunction!(encode_tds_sql_batch, m)?)?;` from `register_db_bindings`.
  - `PyBytes` stays imported (other bindings in the file use it).
- `src/py/vertebra/_vertebra.pyi`: delete `"encode_tds_sql_batch",` from `__all__` and `def encode_tds_sql_batch(sql: str, tx_descriptor: int = 0) -> bytes: ...`.

## Edge Cases & Error Semantics

- `hasattr(vertebra._vertebra, "encode_tds_sql_batch")` is `False` after `make dev`; a stale build before `make dev` still exposes it, so stubtest runs only after `make dev`.
- The Rust crate still defines `pub fn vertebra_db::mssql::encode_tds_sql_batch` after this task; a `pub` item in a `pub` module raises no `dead_code` warning.
- Removing the stub without the pyfunction (or the reverse) fails stubtest; both changes ship in this commit.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (in-process; no edits) plus `mypy.stubtest` as the binding-surface gate.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Binding gone | `uv run --no-sync python -c "import vertebra._vertebra as v; print(hasattr(v, 'encode_tds_sql_batch'))"` after `make dev` | prints `False` |
| Rewriter binding kept | `uv run --no-sync python -c "import vertebra._vertebra as v; print(v.rewrite_mssql_placeholders('a = ?'))"` | `('a = @P1', 1)` |
| Stub parity | `uv run python -m mypy.stubtest vertebra._vertebra` | exits 0 |
| MSSQL test | `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k mssql_tds -q` | `1 passed` |

## Verification

- Baseline: `uv run --no-sync python -c "import vertebra._vertebra as v; print(hasattr(v, 'encode_tds_sql_batch'))"` prints `True`.
- Final: `make dev && uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner && uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && (cd src/rs && cargo fmt --all --check && cargo clippy -p vertebra-py --all-targets --features gcp -- -D warnings) && make rs-test`
- Residue: `grep -rn "encode_tds_sql_batch" src/py src/rs/vertebra-py` prints nothing (exit 1).

## Acceptance Criteria

- [x] `encode_tds_sql_batch` is absent from `register_db_bindings`, `db.rs`, and `_vertebra.pyi` (R9.1).
- [x] stubtest across all five extensions exits 0 (R9.1).
- [x] `make rs-test` and `vertebra-py` clippy exit 0.

## Notes & Discoveries

- [2026-11-07 00:00] `parse_cluster_redirect` in `db.rs` stays; it is the live caller of `vertebra_db::resp3::{ClusterRedirect, RedirectKind}`.

[^db-bindings]: `src/rs/vertebra-py/src/db.rs::encode_tds_sql_batch` — encode_tds_sql_batch pyfunction and register_db_bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi::encode_tds_sql_batch` — Stub definition and __all__ entry.
