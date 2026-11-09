---
type: Task
id: "driver-fail-loud:ch05:t01"
chapter_id: ch05-mssql-codec-cleanup
title: Remove the MSSQL Python batch wrappers and encode_tds_packet probe
description: Delete MSSQLCore.encode_tds_sql_batch, its module alias and package export, MSSQLConfig.encode_sql_batch, and the encode_tds_packet native probe, and rewrite MSSQL adapter docstrings without the future import.
status: stable
state: completed
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 55
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T03:35:00Z"
last_verified_commit: 9e7ad81c
verification_evidence: "pytest test_enterprise_olap_drivers.py (90 passed), ruff, mypy, stubtest"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T03:35:00Z"
commit: 9e7ad81c
sources:
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore
    title: MSSQL TDS helpers, batch wrapper, and native probes
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch
    title: Config helper wrapping MSSQLCore.encode_tds_sql_batch
  - id: mssql-init
    resource: src/py/vertebra/db/adapters/mssql/__init__.py
    title: MSSQL package lazy exports
  - id: olap-test
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter
    title: TDS framing and placeholder rewriter test
tags:
  - mssql
  - cleanup
  - python
---

# Task t01: Remove the MSSQL Python Batch Wrappers and `encode_tds_packet` Probe

## Objective & Scope Boundary

Delete every Python path that reaches the `encode_tds_sql_batch` binding (`MSSQLCore.encode_tds_sql_batch`, the module alias, the package lazy export, `MSSQLConfig.encode_sql_batch`), delete the dead native probe in `MSSQLCore.encode_tds_packet`, rewrite the MSSQL adapter module and class docstrings to describe helpers instead of a native TDS driver, and drop `from __future__ import annotations` from the three MSSQL files.[^mssql-core][^mssql-config][^mssql-init]

This task runs before the binding removal (t02). The binding and its stub stay registered after this commit with no Python caller, so `mypy.stubtest vertebra._vertebra` stays green and no code path changes behavior. The reverse order is also green (the wrapper falls back to a pure-Python encoder with the same bytes), but wrappers-first never relies on that fallback.

Out of scope: the `encode_tds_sql_batch` pyfunction and stub (t02); Rust TDS framing (t03); `MSSQLCore.encode_tds_packet` / `decode_tds_packet` pure-Python bodies and `TDS_PKT_*` constants (kept); `mssql/driver.py` and `MSSQLCore.normalize_for_sqlite` (ch01); `MSSQLConfig` field docstrings.

Precondition: `grep -n "encode_tds_sql_batch\|_last_tds_batch" src/py/vertebra/db/adapters/mssql/driver.py` prints nothing. If it prints a line, set `state: blocked` with `blocked_reason: "ch01 has not removed _last_tds_batch from mssql/driver.py"`.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/mssql/core.py` — module docstring, `__all__`, `MSSQLCore.encode_tds_packet`, `MSSQLCore.encode_tds_sql_batch`, module aliases.[^mssql-core]
- `src/py/vertebra/db/adapters/mssql/config.py` — module docstring, `MSSQLConfig` docstring, `MSSQLConfig.encode_sql_batch`, `to_cloud_dialer_config`.[^mssql-config]
- `src/py/vertebra/db/adapters/mssql/__init__.py` — module docstring, `TYPE_CHECKING` imports, `__all__`, `_EXPORT_MAP`.[^mssql-init]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — import block and `test_mssql_tds_framing_and_placeholder_rewriter`.[^olap-test]

Grep-only verification (no full read): `grep -rn "encode_tds_sql_batch\|encode_sql_batch" src/py/vertebra src/py/tests docs` lists only the lines this task deletes plus `src/py/vertebra/_vertebra.pyi` (t02).

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/mssql/core.py`:
  - Module docstring: `"""SQL Server TDS packet helpers and `@P1` placeholder rewriter."""`.
  - Delete `from __future__ import annotations`; no annotation in the file names an unbound class.
  - Delete `"encode_tds_sql_batch"` from `__all__`, the `MSSQLCore.encode_tds_sql_batch(sql: str, transaction_descriptor: int = 0) -> bytes` static method, and `encode_tds_sql_batch = MSSQLCore.encode_tds_sql_batch`.
  - `MSSQLCore.encode_tds_packet(packet_type: int, packet_id: int, is_eom: bool, payload: bytes) -> bytes` loses the `native = import_optional("vertebra._vertebra")` / `if native is not None and hasattr(native, "encode_tds_packet"):` branch; the `struct.pack(">BBHHBB", ...)` body is unchanged.
  - `import_optional` and `struct` imports stay (`rewrite_placeholders`, `encode_tds_packet`, `decode_tds_packet` use them); `TDS_PKT_SQL_BATCH` stays exported.
- `src/py/vertebra/db/adapters/mssql/config.py`:
  - Module docstring: `"""SQL Server connection configuration."""`.
  - `MSSQLConfig` class docstring: `"""Configuration for the SQL Server driver backed by the native `vertebra._mssql` module."""`.
  - Delete `from __future__ import annotations` and the `encode_sql_batch` static method; the `MSSQLCore` import stays (`MSSQLConfig.rewrite_placeholders` uses it). `to_cloud_dialer_config(self) -> CloudSQLConfig | None` needs no quoting (`CloudSQLConfig` is imported at runtime).
- `src/py/vertebra/db/adapters/mssql/__init__.py`:
  - Module docstring: `"""SQL Server configuration, TDS packet helpers, and `@P1` placeholder rewriter; the driver requires the native `vertebra._mssql` module."""`.
  - Delete `from __future__ import annotations` and `encode_tds_sql_batch` from the `TYPE_CHECKING` import, `__all__`, and `_EXPORT_MAP`.

## Edge Cases & Error Semantics

- `from vertebra.db.adapters.mssql import encode_tds_sql_batch` raises `ImportError`; attribute access raises `AttributeError` from `lazy_exports`.
- `MSSQLConfig.encode_sql_batch` raises `AttributeError`; no alias is added.
- `encode_tds_packet` output is byte-identical because `_vertebra` never exported `encode_tds_packet`; the probe branch never ran.
- `vertebra._vertebra.encode_tds_sql_batch` still exists after this commit; stubtest stays green because binding and stub are unchanged.
- If ch01 renamed the test, locate it with `grep -n "encode_tds_sql_batch" src/py/tests`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (in-process; real `_vertebra` extension).
- **Test change**: drop `encode_tds_sql_batch` and `TDS_PKT_SQL_BATCH` from the `vertebra.db.adapters.mssql` import (the deleted batch block is the only use of `TDS_PKT_SQL_BATCH`, so leaving it fails ruff `F401`; `struct` stays imported for another test); delete the seven-line `batch = encode_tds_sql_batch(...)` block; the docstring becomes `"""Verify TDS packet framing and single-pass @P1 placeholder rewriting."""`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| TDS packet framing kept | `encode_tds_packet(TDS_PKT_PRE_LOGIN, 1, True, b"\x00\x00\x1a\x00\x06\x04")` | `b"\x12\x01\x00\x0e\x00\x00\x01\x00\x00\x00\x1a\x00\x06\x04"`; `decode_tds_packet` returns `(0x12, True, 1, b"\x00\x00\x1a\x00\x06\x04")` |
| Rewriter kept | `rewrite_mssql_placeholders("SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?")` | rewritten ends `c = @P3`, count `3` |
| Export gone | `uv run --no-sync python -c "from vertebra.db.adapters.mssql import encode_tds_sql_batch"` | exits 1 with `ImportError` |
| Config helper gone | `uv run --no-sync python -c "from vertebra.db.adapters.mssql import MSSQLConfig; MSSQLConfig.encode_sql_batch"` | exits 1 with `AttributeError` |
| Binding untouched | `uv run --no-sync python -c "import vertebra._vertebra as v; print(hasattr(v, 'encode_tds_sql_batch'))"` | prints `True` (t02 removes it) |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k mssql_tds -q` passes.
- Final: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/mssql src/py/tests/unit/db/test_enterprise_olap_drivers.py && uv run --no-sync mypy src/py/vertebra/db/adapters/mssql && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && make rs-test`
- Residue: `grep -rn "encode_tds_sql_batch\|encode_sql_batch" src/py/vertebra/db src/py/tests` prints nothing; `grep -n "from __future__" src/py/vertebra/db/adapters/mssql/{core,config,__init__}.py` prints nothing.

## Acceptance Criteria

- [x] `MSSQLCore.encode_tds_sql_batch`, its alias, its package export, and `MSSQLConfig.encode_sql_batch` are deleted (R9.1).
- [x] `MSSQLCore.encode_tds_packet` has no native probe and returns the bytes in the matrix (R9.1).
- [x] MSSQL module and class docstrings name helpers and the native `vertebra._mssql` requirement; no `from __future__ import annotations` remains in the three files (R9.1).
- [x] `test_mssql_tds_framing_and_placeholder_rewriter` passes without the batch block and imports neither `encode_tds_sql_batch` nor `TDS_PKT_SQL_BATCH`.
- [x] stubtest and `make rs-test` exit 0.

## Notes & Discoveries

- [2026-11-08 00:00] `MSSQLCore.encode_tds_sql_batch` reaches the binding through `import_optional` plus `hasattr` with a pure-Python fallback, so wrappers and binding land in separate commits without an import error.

[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore` — MSSQL TDS helpers, batch wrapper, and native probes.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.encode_sql_batch` — Config helper wrapping MSSQLCore.encode_tds_sql_batch.
[^mssql-init]: `src/py/vertebra/db/adapters/mssql/__init__.py` — MSSQL package lazy exports.
[^olap-test]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_tds_framing_and_placeholder_rewriter` — TDS framing and placeholder rewriter test.
