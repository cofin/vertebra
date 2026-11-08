---
type: Task
id: "driver-fail-loud:ch06:t02"
chapter_id: ch06-oracle-codec-cleanup
title: Remove Oracle TNS probes and rewrite Oracle adapter docstrings
description: Delete the encode_tns_connect_packet and encode_tns_data_packet native probes, rewrite Oracle module and class docstrings that claim a native Thin driver, and drop the future import from the three Oracle modules with five self-referencing annotations string-quoted.
status: stable
state: pending
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 35
plan_revision: 2
plan_commit: fd571e3a
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
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle TNS helpers with native probes and VECTOR/OSON codecs
  - id: oracle-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Oracle configuration, connect descriptors, and wallets
  - id: oracle-init
    resource: src/py/vertebra/db/adapters/oracle/__init__.py
    title: Oracle package docstring and lazy exports
  - id: olap-test
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs
    title: Oracle codec and TNS framing test
tags:
  - python
  - oracle
  - cleanup
---

# Task t02: Remove Oracle TNS Probes and Rewrite Oracle Adapter Docstrings

## Objective & Scope Boundary

Delete the `OracleCore` branches that probe `vertebra._vertebra` for `encode_tns_connect_packet` and `encode_tns_data_packet` (neither is exported), rewrite Oracle module and class docstrings that describe a native Thin TNS/TTC driver, and drop `from __future__ import annotations` from `oracle/core.py`, `oracle/config.py`, and `oracle/__init__.py`, string-quoting the five self-referencing return annotations in `oracle/config.py`.[^oracle-core][^oracle-config][^oracle-init]

Out of scope: pure-Python bodies of `OracleCore.encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet` (kept and exported); probes for exported natives (`parse_tnsnames_ora`, `resolve_tns_alias`, `encode_ttc_session_attrs`, `{encode,decode}_oracle_vector_*`) stay; `oracle/driver.py` and `OracleCore.normalize_sql` (ch01); Oracle field-level docstrings; Redis and DuckDB (t03).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/core.py` — imports, module docstring, `OracleCore` docstring, `build_session_context_sql`, `encode_tns_connect_packet`, `encode_tns_data_packet`.[^oracle-core]
- `src/py/vertebra/db/adapters/oracle/config.py` — module docstring, `OracleConnectDescriptor.from_tns_descriptor`, `OracleConnectDescriptor.resolve_tns_alias`, `OracleWallet.from_zip_bytes`, `OracleWallet.from_path`, `OracleConfig` docstring, `OracleConfig.derive_pool`.[^oracle-config]
- `src/py/vertebra/db/adapters/oracle/__init__.py` — module docstring and future import.[^oracle-init]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs`, `test_oracle_tns_descriptor_and_wallet_extraction`.[^olap-test]

Grep-only verification: `grep -rn "hasattr(native, \"encode_tns" src/py` lists only the two lines this task deletes.

## Symbol & Signature Contracts

- `oracle/core.py`:
  - Module docstring: `"""Oracle TNS packet helpers, tnsnames.ora parsing, TTC session attributes, and 26ai VECTOR/OSON codecs."""`.
  - `OracleCore` docstring: `"""TNS packet helpers, TTC session attributes, and Oracle 26ai `VECTOR` and `OSON` binary codecs."""`.
  - `build_session_context_sql` docstring: `"""Build the bound `DBMS_SESSION.SET_CONTEXT` call that scopes a session to a tenant value."""`.
  - `encode_tns_connect_packet(descriptor: str) -> bytes` and `encode_tns_data_packet(data_flags: int, payload: bytes) -> bytes` lose the `native = import_optional("vertebra._vertebra")` / `if native is not None and hasattr(native, ...)` branch; `struct.pack` bodies unchanged. `import_optional` stays imported (other codecs use it).
  - Delete `from __future__ import annotations`; no annotation needs quoting. The annotation names `TenantScopeConfig` (`from vertebra.db.rls import TenantScopeConfig`) and `Sequence` (`from collections.abc import Sequence`) are imported at module top and resolve at runtime; ch01 removes the `Mapping` import.
- `oracle/config.py`:
  - Module docstring: `"""Oracle connection configuration, TNS connect descriptors, Cloud Wallets, and 26ai feature flags."""`.
  - `OracleConfig` docstring: `"""Configuration for the Oracle driver backed by the native `vertebra._oracle` module."""`.
  - Delete `from __future__ import annotations`; string-quote every annotation that names its own enclosing class (the class name is unbound while its body executes). The complete set is five method return annotations:
    - `OracleConnectDescriptor.from_tns_descriptor(cls, descriptor: str) -> "OracleConnectDescriptor"`
    - `OracleConnectDescriptor.resolve_tns_alias(cls, contents: str, alias: str) -> "OracleConnectDescriptor | None"`
    - `OracleWallet.from_zip_bytes(cls, data: bytes) -> "OracleWallet"`
    - `OracleWallet.from_path(cls, path: str | Path) -> "OracleWallet"`
    - `OracleConfig.derive_pool(self, *, service_name: str | None = None, edition: str | None = None) -> "OracleConfig"`
  - Annotations that name an earlier class stay unquoted: `OracleWallet.resolve_alias -> OracleConnectDescriptor | None`, `OracleConfig.connect_descriptor -> OracleConnectDescriptor`, `OracleConfig.session_attrs -> OracleSessionAttrs`, the `OracleConfig.topology: OracleTopologyConfig | None` and `driver_features: OracleDriverFeatures` fields, and module-level `resolve_tns_alias -> OracleConnectDescriptor | None`; `OracleConfig.from_url -> Self` stays as written. No annotation names a class defined later in the module, and no name is imported under `TYPE_CHECKING`.
- `oracle/__init__.py`: module docstring `"""Oracle configuration, TNS descriptor and wallet parsing, and 26ai VECTOR/OSON codecs; the driver requires the native `vertebra._oracle` module."""`; delete `from __future__ import annotations`.

## Edge Cases & Error Semantics

- `_vertebra` exports neither `encode_tns_connect_packet` nor `encode_tns_data_packet`, so the deleted branches never ran; outputs are byte-identical.
- Any self-reference left unquoted inside its own class body raises `NameError` at import; `python -c "import vertebra.db.adapters.oracle.config"` catches it.
- `msgspec.Struct` subclasses resolve string annotations on methods without effect on encoding.
- If ch01 already rewrote `build_session_context_sql`, keep ch01's text and apply only the remaining edits.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (in-process; no test edits).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| TNS Data packet | `encode_tns_data_packet(0x0040, b"TTC_QUERY")` | `b"\x00\x13\x00\x00\x06\x00\x00\x00\x00\x40TTC_QUERY"`; `decode_tns_packet` returns `(6, 0x40, b"TTC_QUERY")` |
| TNS Connect packet | `encode_tns_connect_packet("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))")` | `decode_tns_packet(...)` returns `(1, 0, <descriptor utf-8 bytes>)` |
| Import without future | `uv run --no-sync python -c "import vertebra.db.adapters.oracle.config, vertebra.db.adapters.oracle.core, vertebra.db.adapters.oracle"` | exits 0 |
| Derived pool config | `OracleConfig(service_name="FREEPDB1").derive_pool(service_name="HRPDB").service_name` | `"HRPDB"` |
| Descriptor classmethod | `OracleConnectDescriptor.from_tns_descriptor("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.example.com)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=svc_tp)))").port` | `1522` |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k oracle -q` passes.
- Final: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle && make rs-test`
- Residue: `grep -rn "hasattr(native, \"encode_tns" src/py` prints nothing; `grep -n "from __future__" src/py/vertebra/db/adapters/oracle/{__init__,core,config}.py` prints nothing.

## Acceptance Criteria

- [ ] No Python branch probes `encode_tns_connect_packet` or `encode_tns_data_packet` (R9.2).
- [ ] Oracle module and class docstrings describe helpers and codecs and name the native `vertebra._oracle` requirement (R9.2).
- [ ] No future import remains in the three Oracle modules; the five self-referencing return annotations in `oracle/config.py`, including `OracleConfig.derive_pool`, are string-quoted (R9.2).
- [ ] Oracle codec and TNS descriptor tests pass unchanged; `make rs-test` exits 0.

## Notes & Discoveries

- [2026-11-08 00:00] `_vertebra` exports `parse_tnsnames_ora`, `resolve_tns_alias`, and `encode_ttc_session_attrs`; their probes in `OracleCore` are live and stay.

[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle TNS helpers with native probes and VECTOR/OSON codecs.
[^oracle-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Oracle configuration, connect descriptors, and wallets.
[^oracle-init]: `src/py/vertebra/db/adapters/oracle/__init__.py` — Oracle package docstring and lazy exports.
[^olap-test]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs` — Oracle codec and TNS framing test.
