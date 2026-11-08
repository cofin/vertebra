---
type: Task
id: "driver-fail-loud:ch05:t04"
chapter_id: ch05-mssql-oracle-codec-cleanup
title: Remove Oracle and Redis native probes and rewrite Oracle and DuckDB adapter docstrings
description: Delete the encode_tns_connect_packet, encode_tns_data_packet, and redis_hash_slot_native probes, rewrite Oracle and DuckDB module and class docstrings that claim native Thin or SHM-brokered drivers, and drop the future import from the touched Oracle and DuckDB modules.
status: draft
state: pending
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
estimated_loc: 40
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
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle TNS helpers with native probes and VECTOR/OSON codecs
  - id: oracle-config
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleConfig
    title: Oracle configuration, descriptors, and wallets
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot
    title: Redis hash slot with redis_hash_slot_native probe
  - id: duckdb-init
    resource: src/py/vertebra/db/adapters/duckdb/__init__.py
    title: DuckDB adapter package docstring
  - id: olap-test
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs
    title: Oracle codec and TNS framing test
tags:
  - python
  - oracle
  - redis
  - duckdb
  - cleanup
---

# Task t04: Remove Oracle and Redis Native Probes and Rewrite Oracle and DuckDB Adapter Docstrings

## Objective & Scope Boundary

Delete Python branches that probe `vertebra._vertebra` for functions it never exports (`encode_tns_connect_packet`, `encode_tns_data_packet`, `redis_hash_slot_native`), rewrite Oracle and DuckDB module and class docstrings that describe a native Thin TNS/TTC driver or SHM-brokered DuckDB coordination, and drop `from __future__ import annotations` from the three Oracle modules and `duckdb/__init__.py`.[^oracle-core][^oracle-config][^redis-core][^duckdb-init]

Production files exceed 3 because the chapter holds at most 4 tasks and every change is a probe deletion, a docstring line, or a future-import removal with annotation quoting; no behavior changes.

Precondition: ch02 t04 (DuckDB requires the `duckdb` package) and ch04 t01 (`test_redis_valkey_resp3.py` rewrite) are completed; the `duckdb/__init__.py` docstring below describes ch02 t04's driver, and the Final command runs ch04 t01's test file.

Out of scope: pure-Python bodies of `OracleCore.encode_tns_connect_packet`, `encode_tns_data_packet`, `decode_tns_packet` (kept and exported); `oracle/driver.py`, `OracleCore.normalize_sql` (ch01); `redis/core.py` future import and every other Redis line (owned by `native-redis-driver`); Oracle and DuckDB field-level docstrings.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/core.py` — module docstring, `OracleCore` docstring, `build_session_context_sql`, `encode_tns_connect_packet`, `encode_tns_data_packet`.[^oracle-core]
- `src/py/vertebra/db/adapters/oracle/config.py` — module docstring, `OracleConnectDescriptor.from_tns_descriptor`, `OracleConnectDescriptor.resolve_tns_alias`, `OracleWallet.from_zip_bytes`, `OracleWallet.from_path`, `OracleConfig` docstring, `OracleConfig.derive_pool`.[^oracle-config]
- `src/py/vertebra/db/adapters/oracle/__init__.py` — module docstring and future import.
- `src/py/vertebra/db/adapters/redis/core.py` — `RedisCore.hash_slot`.[^redis-core]
- `src/py/vertebra/db/adapters/duckdb/__init__.py` — module docstring and future import.[^duckdb-init]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs`, `test_oracle_tns_descriptor_and_wallet_extraction`.[^olap-test]

Grep-only verification: `grep -rn "redis_hash_slot_native\|hasattr(native, \"encode_tns" src/py` lists only the lines this task deletes.

## Symbol & Signature Contracts

- `oracle/core.py`:
  - Module docstring: `"""Oracle TNS packet helpers, tnsnames.ora parsing, TTC session attributes, and 26ai VECTOR/OSON codecs."""`.
  - `OracleCore` docstring: `"""TNS packet helpers, TTC session attributes, and Oracle 26ai `VECTOR` and `OSON` binary codecs."""`.
  - `build_session_context_sql` docstring: `"""Build the bound `DBMS_SESSION.SET_CONTEXT` call that scopes a session to a tenant value."""`.
  - `encode_tns_connect_packet(descriptor: str) -> bytes` and `encode_tns_data_packet(data_flags: int, payload: bytes) -> bytes` lose the three-line `native = import_optional(...)` / `hasattr(native, ...)` probe; `struct.pack` bodies unchanged. `import_optional` stays imported (other codecs use it).
  - Delete `from __future__ import annotations`; no annotation needs quoting (`TenantScopeConfig`, `re.Match[str]`, `Sequence`, `Mapping` resolve at runtime).
- `oracle/config.py`:
  - Module docstring: `"""Oracle connection configuration, TNS connect descriptors, Cloud Wallets, and 26ai feature flags."""`.
  - `OracleConfig` docstring: `"""Configuration for the Oracle driver backed by the native `vertebra._oracle` module."""`.
  - Delete `from __future__ import annotations`; string-quote every annotation that names its own enclosing class (the class name is unbound while its body executes). The complete set is five method return annotations: `OracleConnectDescriptor.from_tns_descriptor(cls, descriptor: str) -> "OracleConnectDescriptor"`, `OracleConnectDescriptor.resolve_tns_alias(cls, contents: str, alias: str) -> "OracleConnectDescriptor | None"`, `OracleWallet.from_zip_bytes(cls, data: bytes) -> "OracleWallet"`, `OracleWallet.from_path(cls, path: str | Path) -> "OracleWallet"`, and `OracleConfig.derive_pool(self, *, service_name: str | None = None, edition: str | None = None) -> "OracleConfig"`. No annotation names a class defined later in the module, and no name is imported under `TYPE_CHECKING`. Annotations that name an earlier class stay unquoted: `OracleWallet.resolve_alias -> OracleConnectDescriptor | None`, `OracleConfig.connect_descriptor -> OracleConnectDescriptor`, `OracleConfig.session_attrs -> OracleSessionAttrs`, the `OracleConfig.topology: OracleTopologyConfig | None` and `driver_features: OracleDriverFeatures` fields, and module-level `resolve_tns_alias -> OracleConnectDescriptor | None`; `OracleConfig.from_url -> Self` stays as written.
- `oracle/__init__.py`: module docstring `"""Oracle configuration, TNS descriptor and wallet parsing, and 26ai VECTOR/OSON codecs; the driver requires the native `vertebra._oracle` module."""`; delete `from __future__ import annotations`.
- `redis/core.py::RedisCore.hash_slot(key: str | bytes) -> int`: `native_fn = getattr(native, "redis_hash_slot", None)`; the `or getattr(native, "redis_hash_slot_native", None)` clause is deleted.
- `duckdb/__init__.py`: module docstring `"""DuckDB driver over the `duckdb` package with bootstrap statements and shared-memory Arrow IPC helpers."""`; delete `from __future__ import annotations`.

## Edge Cases & Error Semantics

- `_vertebra` exports no `encode_tns_connect_packet`, `encode_tns_data_packet`, or `redis_hash_slot_native`, so the deleted branches never ran; outputs are byte-identical.
- Without the future import, any self-reference left unquoted inside its own class body raises `NameError` at import; `python -c "import vertebra.db.adapters.oracle.config"` catches it.
- `msgspec.Struct` subclasses resolve string annotations on methods without effect on encoding.
- If ch01 already rewrote `build_session_context_sql` or removed `normalize_sql`, keep ch01's text and apply only the remaining edits.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_redis_valkey_resp3.py` (in-process; no test edits).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| TNS Data packet | `encode_tns_data_packet(0x0040, b"TTC_QUERY")` | `b"\x00\x13\x00\x00\x06\x00\x00\x00\x00\x40TTC_QUERY"`; `decode_tns_packet` returns `(6, 0x40, b"TTC_QUERY")` |
| TNS Connect packet | `encode_tns_connect_packet("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))")` | `decode_tns_packet(...)` returns `(1, 0, <descriptor utf-8 bytes>)` |
| Hash slot with native | `hash_slot("123456789")` | `12739` |
| Hash tag | `hash_slot("user:{42}:profile") == hash_slot(b"42")` | `True` |
| Import without future | `uv run --no-sync python -c "import vertebra.db.adapters.oracle.config, vertebra.db.adapters.oracle.core, vertebra.db.adapters.duckdb"` | exits 0 |
| Derived pool config | `OracleConfig(service_name="FREEPDB1").derive_pool(service_name="HRPDB").service_name` | `"HRPDB"` |
| Descriptor classmethod | `OracleConnectDescriptor.from_tns_descriptor("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=adb.example.com)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=svc_tp)))").port` | `1522` |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q` passes.
- Final: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/redis/core.py src/py/vertebra/db/adapters/duckdb/__init__.py && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/redis/core.py src/py/vertebra/db/adapters/duckdb/__init__.py`
- Residue: `grep -rn "redis_hash_slot_native\|hasattr(native, \"encode_tns" src/py` prints nothing; `grep -n "from __future__" src/py/vertebra/db/adapters/oracle/{__init__,core,config}.py src/py/vertebra/db/adapters/duckdb/__init__.py` prints nothing.

## Acceptance Criteria

- [ ] No Python branch probes `encode_tns_connect_packet`, `encode_tns_data_packet`, or `redis_hash_slot_native` (R9.2).
- [ ] Oracle and DuckDB module and class docstrings describe helpers and codecs, not native Thin or SHM-brokered drivers (R9.2).
- [ ] No future import remains in the four touched Oracle/DuckDB modules; the five self-referencing return annotations in `oracle/config.py`, including `OracleConfig.derive_pool`, are string-quoted (R9.2).
- [ ] Oracle codec, TNS descriptor, and Redis hash-slot tests pass unchanged.

## Notes & Discoveries

- [2026-11-07 00:00] `RedisCore.parse_cluster_redirect` probes `parse_cluster_redirect`, which `_vertebra` does export; that probe stays.

[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle TNS helpers with native probes and VECTOR/OSON codecs.
[^oracle-config]: `src/py/vertebra/db/adapters/oracle/config.py::OracleConfig` — Oracle configuration, descriptors, and wallets.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot` — Redis hash slot with redis_hash_slot_native probe.
[^duckdb-init]: `src/py/vertebra/db/adapters/duckdb/__init__.py` — DuckDB adapter package docstring.
[^olap-test]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs` — Oracle codec and TNS framing test.
