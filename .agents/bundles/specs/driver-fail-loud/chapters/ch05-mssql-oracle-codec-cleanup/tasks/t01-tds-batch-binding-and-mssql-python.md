---
type: Task
id: "driver-fail-loud:ch05:t01"
chapter_id: ch05-mssql-oracle-codec-cleanup
title: Remove the encode_tds_sql_batch binding, stub, and MSSQL Python wrappers
description: Delete the encode_tds_sql_batch pyfunction and stub, MSSQLCore.encode_tds_sql_batch, MSSQLConfig.encode_sql_batch, the package export, and the encode_tds_packet native probe, and rewrite MSSQL adapter docstrings without the future import.
status: draft
state: pending
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 70
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::encode_tds_sql_batch
    title: encode_tds_sql_batch pyfunction and register_db_bindings
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi::encode_tds_sql_batch
    title: Stub definition and __all__ entry
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore
    title: MSSQL TDS helpers with native probes
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch
    title: Config helper wrapping encode_tds_sql_batch
  - id: olap-test
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter
    title: TDS framing and placeholder rewriter test
tags:
  - pyo3
  - mssql
  - cleanup
  - python
---

# Task t01: Remove the `encode_tds_sql_batch` Binding, Stub, and MSSQL Python Wrappers

## Objective & Scope Boundary

Delete the `encode_tds_sql_batch` PyO3 binding and its stub in one commit, delete every Python path that reaches it (`MSSQLCore.encode_tds_sql_batch`, the module alias, the package lazy export, `MSSQLConfig.encode_sql_batch`), delete the dead native probe in `MSSQLCore.encode_tds_packet`, rewrite MSSQL adapter module and class docstrings to describe helpers instead of a native TDS driver, and drop `from __future__ import annotations` from the three MSSQL files touched.[^db-bindings][^vertebra-pyi][^mssql-core][^mssql-config]

Production files exceed 3 because the chapter holds at most 4 tasks and every change here is a single-symbol deletion or a one-line docstring rewrite of the same symbol across its binding, stub, wrapper, export, and config consumer. Splitting the Rust binding from the Python wrappers would also be safe: `MSSQLCore.encode_tds_sql_batch` reaches the binding through `import_optional` plus `hasattr` and falls back to pure Python, so no intermediate commit would import a removed name.

Out of scope: `vertebra_db::mssql::encode_tds_sql_batch` and other Rust TDS framing (t02); `MSSQLCore.encode_tds_packet` / `decode_tds_packet` pure-Python bodies and `TDS_PKT_*` constants (kept); `mssql/driver.py` (ch01); `MSSQLCore.normalize_for_sqlite` (ch01); `MSSQLConfig` field docstrings.

Precondition: `grep -n "encode_tds_sql_batch\|_last_tds_batch" src/py/vertebra/db/adapters/mssql/driver.py` prints nothing. If it prints a line, set `state: blocked` with `blocked_reason: "ch01 has not removed _last_tds_batch from mssql/driver.py"`.

## Bounded Read Manifest

- `src/rs/vertebra-py/src/db.rs` — `use vertebra_db::mssql::{...}` import, `encode_tds_sql_batch` pyfunction, `register_db_bindings`.[^db-bindings]
- `src/py/vertebra/_vertebra.pyi` — `__all__` entry `"encode_tds_sql_batch"` and `def encode_tds_sql_batch(...)`.[^vertebra-pyi]
- `src/py/vertebra/db/adapters/mssql/core.py` — `MSSQLCore`, module aliases, `__all__`.[^mssql-core]
- `src/py/vertebra/db/adapters/mssql/config.py` — `MSSQLConfig.encode_sql_batch`.[^mssql-config]
- `src/py/vertebra/db/adapters/mssql/__init__.py` — `TYPE_CHECKING` imports, `__all__`, `_EXPORT_MAP`.
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — import block and `test_mssql_tds_framing_and_placeholder_rewriter`.[^olap-test]

Grep-only verification (no full read): `grep -rn "encode_tds_sql_batch\|encode_sql_batch" src/py src/rs/vertebra-py docs` must list only the lines this task deletes.

## Symbol & Signature Contracts

- `src/rs/vertebra-py/src/db.rs`:
  - Import becomes `use vertebra_db::mssql::rewrite_mssql_placeholders as rs_rewrite_mssql_placeholders;`.
  - Delete the doc comment, `#[pyfunction]`, `#[pyo3(signature = (sql, tx_descriptor=0))]`, and body of `pub fn encode_tds_sql_batch<'py>(py: Python<'py>, sql: &str, tx_descriptor: u64) -> PyResult<Bound<'py, PyBytes>>`.
  - Delete `m.add_function(wrap_pyfunction!(encode_tds_sql_batch, m)?)?;` from `register_db_bindings`.
- `src/py/vertebra/_vertebra.pyi`: delete `"encode_tds_sql_batch",` from `__all__` and `def encode_tds_sql_batch(sql: str, tx_descriptor: int = 0) -> bytes: ...`.
- `src/py/vertebra/db/adapters/mssql/core.py`:
  - Module docstring: `"""SQL Server TDS packet helpers and `@P1` placeholder rewriter."""`.
  - Delete `from __future__ import annotations`; no annotation in the file needs quoting.
  - Delete `"encode_tds_sql_batch"` from `__all__`, the `MSSQLCore.encode_tds_sql_batch` static method, and `encode_tds_sql_batch = MSSQLCore.encode_tds_sql_batch`.
  - `MSSQLCore.encode_tds_packet(packet_type: int, packet_id: int, is_eom: bool, payload: bytes) -> bytes` loses the three-line `native = import_optional(...)` / `hasattr(native, "encode_tds_packet")` probe; the `struct.pack(">BBHHBB", ...)` body is unchanged.
  - `import_optional` and `struct` imports stay (`rewrite_placeholders`, `encode_tds_packet`, `decode_tds_packet` use them).
- `src/py/vertebra/db/adapters/mssql/config.py`:
  - Module docstring: `"""SQL Server connection configuration."""`.
  - `MSSQLConfig` class docstring: `"""Configuration for the SQL Server driver backed by the native `vertebra._mssql` module."""`.
  - Delete `from __future__ import annotations` and the `encode_sql_batch` static method. `to_cloud_dialer_config(self) -> CloudSQLConfig | None` needs no quoting (`CloudSQLConfig` is imported at runtime).
- `src/py/vertebra/db/adapters/mssql/__init__.py`:
  - Module docstring: `"""SQL Server configuration, TDS packet helpers, and `@P1` placeholder rewriter; the driver requires the native `vertebra._mssql` module."""`.
  - Delete `from __future__ import annotations`, `encode_tds_sql_batch` from the `TYPE_CHECKING` import, `__all__`, and `_EXPORT_MAP`.

## Edge Cases & Error Semantics

- `from vertebra.db.adapters.mssql import encode_tds_sql_batch` raises `ImportError`; `vertebra.db.adapters.mssql.encode_tds_sql_batch` raises `AttributeError` from `lazy_exports`.
- `MSSQLConfig.encode_sql_batch` raises `AttributeError`; no alias is added.
- `hasattr(vertebra._vertebra, "encode_tds_sql_batch")` is `False` after `make dev`; a stale build before `make dev` still exposes it, so stubtest runs only after `make dev`.
- `encode_tds_packet` output is byte-identical because `_vertebra` never exported `encode_tds_packet`; the probe branch never ran.
- The Rust crate still defines `vertebra_db::mssql::encode_tds_sql_batch` after this task; `cargo clippy` does not flag an unused `pub fn`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (in-process; real `_vertebra` extension).
- **Test change**: drop `encode_tds_sql_batch` and `TDS_PKT_SQL_BATCH` from the `vertebra.db.adapters.mssql` import (the deleted batch block is the only use of `TDS_PKT_SQL_BATCH`, so leaving it fails ruff `F401`; `struct` stays imported for another test); delete the seven-line `batch = encode_tds_sql_batch(...)` block from `test_mssql_tds_framing_and_placeholder_rewriter`; docstring becomes `"""Verify TDS packet framing and single-pass @P1 placeholder rewriting."""`. If ch01 renamed the test, locate it with `grep -n "encode_tds_sql_batch" src/py/tests`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| TDS packet framing kept | `encode_tds_packet(TDS_PKT_PRE_LOGIN, 1, True, b"\x00\x00\x1a\x00\x06\x04")` | `b"\x12\x01\x00\x0e\x00\x00\x01\x00\x00\x00\x1a\x00\x06\x04"`; `decode_tds_packet` returns `(0x12, True, 1, b"\x00\x00\x1a\x00\x06\x04")` |
| Rewriter kept | `rewrite_mssql_placeholders("SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?")` | rewritten ends `c = @P3`, count `3` |
| Binding gone | `python -c "import vertebra._vertebra as v; print(hasattr(v, 'encode_tds_sql_batch'))"` | prints `False` |
| Export gone | `python -c "from vertebra.db.adapters.mssql import encode_tds_sql_batch"` | exits 1 with `ImportError` |
| Config helper gone | `python -c "from vertebra.db.adapters.mssql import MSSQLConfig; MSSQLConfig.encode_sql_batch"` | exits 1 with `AttributeError` |
| Stub parity | `uv run python -m mypy.stubtest vertebra._vertebra` | exits 0 |

## Verification

- Baseline: `uv run --no-sync python -c "import vertebra._vertebra as v; print(hasattr(v, 'encode_tds_sql_batch'))"` prints `True`; `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -q` passes.
- Final: `make dev && uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._bigquery vertebra._spanner && uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/mssql src/py/tests/unit/db/test_enterprise_olap_drivers.py && uv run --no-sync mypy src/py/vertebra/db/adapters/mssql && (cd src/rs && cargo clippy -p vertebra-py --all-targets --features gcp -- -D warnings) && make rs-test`
- Residue check: `grep -rn "encode_tds_sql_batch\|encode_sql_batch" src/py src/rs/vertebra-py` prints nothing (exit 1).

## Acceptance Criteria

- [ ] `encode_tds_sql_batch` is absent from `register_db_bindings`, `db.rs`, and `_vertebra.pyi`; stubtest exits 0 (R9.1).
- [ ] `MSSQLCore.encode_tds_sql_batch`, its alias, its package export, and `MSSQLConfig.encode_sql_batch` are deleted (R9.1).
- [ ] `MSSQLCore.encode_tds_packet` has no native probe and returns the bytes in the matrix (R9.1).
- [ ] MSSQL module and class docstrings name helpers and the native `vertebra._mssql` requirement; no `from __future__ import annotations` remains in the three MSSQL files (R9.1).
- [ ] `test_mssql_tds_framing_and_placeholder_rewriter` passes without the batch block, and the test module no longer imports `encode_tds_sql_batch` or `TDS_PKT_SQL_BATCH`.
- [ ] `make rs-test` exits 0.

## Notes & Discoveries

- [2026-11-07 00:00] `parse_cluster_redirect` in `db.rs` stays; it is the live caller of `vertebra_db::resp3::{ClusterRedirect, RedirectKind}`.

[^db-bindings]: `src/rs/vertebra-py/src/db.rs::encode_tds_sql_batch` — encode_tds_sql_batch pyfunction and register_db_bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi::encode_tds_sql_batch` — Stub definition and __all__ entry.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore` — MSSQL TDS helpers with native probes.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch` — Config helper wrapping encode_tds_sql_batch.
[^olap-test]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter` — TDS framing and placeholder rewriter test.
