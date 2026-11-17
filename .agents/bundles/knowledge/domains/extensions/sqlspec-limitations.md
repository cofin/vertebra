---
type: Knowledge
title: SQLSpec Adapter, Proxy & Store Limitations
description: Operational boundaries for SQLSpec 0.72.0 adapter pinning, data dictionary bases, native Oracle sessionless hooks, session reset semantics, storage alias registration, ADK extras, and TxEventQ acknowledgment.
status: stable
tags:
  - domains
  - extensions
  - sqlspec
  - adk
  - oracle
  - storage
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T06:55:00Z"
last_reviewed_commit: 5a833832
verified:
  - by: agent:flow-executor
    at: "2026-11-17T06:55:00Z"
    commit: 5a833832
sources:
  - id: src-pyproject
    resource: pyproject.toml
    title: SQLSpec >=0.72.0 dependency floor and git commit pin c2886c3d
  - id: src-sqlspec-mysql-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py
    title: MySQL data dictionary subclassing upstream sqlspec.data_dictionary.dialects.mysql.dictionary
  - id: src-sqlspec-pg-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py
    title: Direct SyncDataDictionaryBase/AsyncDataDictionaryBase subclassing when upstream dialect dictionary is absent
  - id: src-sqlspec-oracle-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
    title: Oracle SQLSpec driver sessionless transaction FFI checks
  - id: src-sqlspec-oracle-events
    resource: src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py
    title: Oracle AQ and TxEventQ dequeue-commit and nack republish behavior
  - id: src-sqlspec-bigquery-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py
    title: BigQuery reset_session_on_release and stateless REST no-op behavior
  - id: src-sqlspec-spanner-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py
    title: Spanner reset_session_on_release local flag cleanup
  - id: src-sqlspec-duckdb-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py
    title: DuckDB reset_session_on_release temporary object cleanup and baseline pragma restoration
  - id: src-sqlspec-mssql-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py
    title: MSSQL reset_session_on_release EXEC sp_reset_connection batch fallback
  - id: src-sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: SQLSpec StorageRegistry.register_alias delegation and direct ObjectStoreBackend construction
  - id: src-sqlspec-postgres-adk
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py
    title: ADK memory and artifact store google-adk extra enforcement via ensure_module
  - id: src-sqlspec-bigquery-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
    title: BigQuery tenant isolation rejection guard
  - id: src-sqlspec-spanner-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
    title: Spanner tenant isolation rejection guard
---

# SQLSpec Adapter, Proxy & Store Limitations

<!-- truth: start -->
- `pyproject.toml` pins `sqlspec` under `[tool.uv.sources]` to commit `c2886c3d02b8aca705d137dd2640996488a8336a` while declaring `"sqlspec>=0.72.0"` until `0.72.0` publishes to PyPI [^src-pyproject].
- At pinned `sqlspec` `c2886c3d`, `sqlspec.data_dictionary.dialects.<dialect>.dictionary` exists only for MySQL; `MySQLSyncDataDictionary` and `MySQLAsyncDataDictionary` subclass it, while the PostgreSQL, Oracle, SQLite, MSSQL, DuckDB, BigQuery, and Spanner dictionaries subclass `SyncDataDictionaryBase` and `AsyncDataDictionaryBase` directly [^src-sqlspec-mysql-dict] [^src-sqlspec-pg-dict].
- Native pure-Rust `vertebra._oracle` does not expose FFI `begin_sessionless_transaction`, `resume_sessionless_transaction`, or `suspend_sessionless_transaction` hooks (nor `suspend_on_success` execution), so `OracleSyncDriver`, `OracleAsyncDriver`, and the Oracle sessionless context managers raise `DialectNotSupportedError` against `vertebra._oracle` until native FFI support lands [^src-sqlspec-oracle-driver].
- `reset_session_on_release=True` is a no-op on stateless BigQuery REST connections lacking an active `session_id` (falling back to `CALL BQ.ABORT_SESSION(@session_id)` only when a session exists), clears only local transaction flags on Spanner when no native `reset_session` hook exists, drops only temporary objects plus non-baseline attached databases while re-applying baseline pragmas on DuckDB, and executes `EXEC sp_reset_connection` as a T-SQL batch rather than a TDS `RESETCONNECTION` packet flag when an MSSQL connection lacks a native reset hook [^src-sqlspec-bigquery-typing] [^src-sqlspec-spanner-typing] [^src-sqlspec-duckdb-typing] [^src-sqlspec-mssql-typing].
- Upstream `StorageRegistry.register_alias` accepts only `"local"`, `"obstore"`, and `"fsspec"` backend names and rejects custom `ObjectStoreBackend` instances or pre-configured `SyncObjectStore` / `AsyncObjectStore` signing state (`SigningConfig`, credentials, custom endpoints); `register_storage_alias` forwards only the URI string without touching `_alias_configs`, so callers needing custom native stores or signing must construct `ObjectStoreBackend(store=...)` directly [^src-sqlspec-storage].
- All copied fallback ADK memory and artifact stores are deleted; `<Engine>{Sync,Async}ADKMemoryStore` and `<Engine>{Sync,Async}ADKArtifactStore` load `sqlspec.extensions.adk` via `ensure_module(..., package="google-adk", extra="adk")` and fail with `MissingDependencyError` when `google-adk` is absent [^src-sqlspec-postgres-adk].
- `OracleSyncEventsBackend` and `OracleAsyncEventsBackend` commit `DBMS_AQ.DEQUEUE` (TxEventQ) messages at dequeue time and re-enqueue via `publish` on `nack()`, DuckDB isolates plain `:memory:` instances per config while sharing `:memory:<name>` catalogs across drivers, and BigQuery and Spanner configs and drivers reject non-None `tenant_predicate` or `tenant_id` with `NotSupportedError` [^src-sqlspec-oracle-events] [^src-sqlspec-bigquery-driver] [^src-sqlspec-spanner-driver].
<!-- truth: end -->

## Operational Boundaries

- **Upstream `sqlspec` `0.72.0` Git Pin**: [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject] declares `"sqlspec>=0.72.0"` while pinning commit `c2886c3d02b8aca705d137dd2640996488a8336a` in `[tool.uv.sources]` until `0.72.0` is released on PyPI.
- **Data Dictionary Upstream Base Coverage**: At `c2886c3d`, upstream `sqlspec.data_dictionary.dialects` ships a shared `dictionary` base class only for MySQL ([`mysql/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py)[^src-sqlspec-mysql-dict]). PostgreSQL ([`postgres/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py)[^src-sqlspec-pg-dict]), Oracle, SQLite, MSSQL, DuckDB, BigQuery, and Spanner implement their dialect queries directly on `SyncDataDictionaryBase` and `AsyncDataDictionaryBase` without cross-importing `sqlspec.adapters.*`.
- **Oracle 26ai Sessionless Transaction FFI Gap**: [`OracleSyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py)[^src-sqlspec-oracle-driver] and `OracleAsyncDriver` implement `begin_sessionless_transaction`, `resume_sessionless_transaction`, `suspend_sessionless_transaction`, `sessionless_transaction(...)`, and `execute(..., suspend_on_success=True)`. Because pure-Rust `vertebra._oracle` does not expose native sessionless FFI hooks yet, calls against `vertebra._oracle` connections fail loudly with `DialectNotSupportedError`, while external or test connections that expose those hooks run them directly.
- **`reset_session_on_release` Engine & Transport Caveats**:
  - **BigQuery**: [`bigquery/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py)[^src-sqlspec-bigquery-typing] invokes `reset_session()` / `reset()` if present or runs `CALL BQ.ABORT_SESSION(@session_id)` when `session_id` / `_session_id` is set; stateless REST connections without an active BigQuery session perform no remote call.
  - **Spanner**: [`spanner/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py)[^src-sqlspec-spanner-typing] invokes `reset_session()` / `reset()` if present and clears local `_active_transaction` / `_in_transaction` flags because Cloud Spanner exposes no SQL `RESET ALL` command.
  - **DuckDB**: [`duckdb/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py)[^src-sqlspec-duckdb-typing] drops temporary macros, views, tables, and sequences in `temp.main`, restores the baseline default database while detaching non-baseline databases, and re-applies baseline `PRAGMA` / `SET` statements; non-temporary tables or views created in file-backed catalogs persist across releases. Plain `:memory:` databases stay isolated per config, while `:memory:<name>` shares named in-memory catalogs across drivers.
  - **SQL Server (`sp_reset_connection`)**: [`mssql/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py)[^src-sqlspec-mssql-typing] prefers `reset_session()` / `reset()` when provided by the connection and otherwise executes `EXEC sp_reset_connection` as a SQL batch rather than setting the TDS `RESETCONNECTION` (`0x08`) wire-packet flag, so transport permissions and an open connection are required.
- **`StorageRegistry.register_alias` vs. Native Stores**: [`sqlspec/storage.py`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-sqlspec-storage] delegates `register_storage_alias` to upstream `StorageRegistry.register_alias`, which validates `backend` against `"local"`, `"obstore"`, and `"fsspec"` only and discards custom `ObjectStoreBackend` instances or `SyncObjectStore` / `AsyncObjectStore` `SigningConfig` state. Instantiate `ObjectStoreBackend(store=...)` directly whenever custom native store configuration or presigned URL signing is needed.
- **Mandatory `google-adk` Extra**: ADK session, memory, and artifact stores across all eight engines (`adk.py`) carry no fallback implementations and load `sqlspec.extensions.adk` via `ensure_module(..., package="google-adk", extra="adk")` ([`postgres/adk.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py)[^src-sqlspec-postgres-adk]), raising `MissingDependencyError` when `google-adk` (`vertebra-core[adk]`) is absent.
- **Oracle TxEventQ Acknowledgment & Cloud Tenant Guards**: [`OracleSyncEventsBackend`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py)[^src-sqlspec-oracle-events] and `OracleAsyncEventsBackend` commit `DBMS_AQ.DEQUEUE` messages upon dequeue and re-publish on `nack()`. [`BigQuery`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py)[^src-sqlspec-bigquery-driver] and [`Spanner`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py)[^src-sqlspec-spanner-driver] configs and drivers reject `tenant_predicate` and `tenant_id` with `NotSupportedError` before base initialization or native calls.

[^src-pyproject]: `pyproject.toml` — SQLSpec `>=0.72.0` dependency floor and `[tool.uv.sources]` commit pin `c2886c3d02b8aca705d137dd2640996488a8336a`.
[^src-sqlspec-mysql-dict]: `src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py` — MySQL data dictionary subclassing `sqlspec.data_dictionary.dialects.mysql.dictionary`.
[^src-sqlspec-pg-dict]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py` — Direct `SyncDataDictionaryBase` / `AsyncDataDictionaryBase` subclassing without an upstream dialect dictionary.
[^src-sqlspec-oracle-driver]: `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py` — Oracle SQLSpec driver sessionless transaction FFI checks.
[^src-sqlspec-oracle-events]: `src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py` — Oracle AQ and TxEventQ dequeue-commit and `nack()` republish behavior.
[^src-sqlspec-bigquery-typing]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py` — BigQuery `reset_session_on_release` and stateless REST no-op behavior.
[^src-sqlspec-spanner-typing]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py` — Spanner `reset_session_on_release` local flag cleanup.
[^src-sqlspec-duckdb-typing]: `src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py` — DuckDB `reset_session_on_release` temporary object cleanup and baseline pragma restoration.
[^src-sqlspec-mssql-typing]: `src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py` — MSSQL `reset_session_on_release` `EXEC sp_reset_connection` batch fallback.
[^src-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — `StorageRegistry.register_alias` delegation and direct `ObjectStoreBackend` construction.
[^src-sqlspec-postgres-adk]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py` — ADK memory and artifact store `google-adk` extra enforcement via `ensure_module`.
[^src-sqlspec-bigquery-driver]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` — BigQuery tenant isolation rejection guard.
[^src-sqlspec-spanner-driver]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` — Spanner tenant isolation rejection guard.
