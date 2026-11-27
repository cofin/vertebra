---
type: Knowledge
title: SQLSpec Adapter, Proxy & Store Limitations
description: Operational boundaries for SQLSpec 0.74.1+ cached statement executor registration, nested schema mapping, shared dialect data dictionary bases, native Oracle sessionless hooks, session reset semantics, custom storage backend/instance aliases, ADK extras, and TxEventQ acknowledgment.
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
last_reviewed_commit: 9077dd3e
verified:
  - by: agent:flow-executor
    at: "2026-11-22T01:02:00Z"
    commit: 9077dd3e
  - by: agent:bb-executor
    at: "2026-11-21T01:17:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-18T14:25:00Z"
    commit: 7ae5ae50
sources:
  - id: src-sqlspec-driver
    resource: src/py/vertebra/extensions/sqlspec/_driver.py
    title: BaseSyncDriver and BaseAsyncDriver native cursor dispatch and statement config pass-through
  - id: src-pyproject
    resource: pyproject.toml
    title: SQLSpec >=0.74.1 PyPI dependency floor without upstream git pins
  - id: src-sqlspec-mysql-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py
    title: MySQL data dictionary subclassing upstream sqlspec.data_dictionary.dialects.mysql.dictionary
  - id: src-sqlspec-pg-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py
    title: PostgreSQL data dictionary subclassing upstream sqlspec.data_dictionary.dialects.postgres.dictionary
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
  - id: src-sqlspec-postgres-config
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py
    title: PostgresSyncConfig and PostgresAsyncConfig derive_pool cloud auth preservation
  - id: src-sqlspec-mysql-config
    resource: src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py
    title: MySQLSyncConfig and MySQLAsyncConfig derive_pool cloud auth and ssl_ca preservation
  - id: src-dbapi-sync
    resource: src/py/vertebra/db/dbapi/_sync.py
    title: PEP 249 connect_args allowlists and +vtb scheme normalization
---

# SQLSpec Adapter, Proxy & Store Limitations

<!-- truth: start -->
- **Native Cursor & Cache-Hit Seam**: `BaseSyncDriver.with_cursor` and `BaseAsyncDriver.with_cursor` yield the engine's native cursor so a SQLSpec cache hit performs one native `execute`; SQL and parameters pass through unchanged, and extra value conversions live in statement-config type converters [^src-sqlspec-driver].
- **Dependency Floor & Schema Normalization**: `pyproject.toml` pins `"sqlspec>=0.74.1"` from PyPI with no git sources. Driver `to_schema` and tabular row hydrators default to `normalize_nested=True`, whereas standalone `sqlspec.utils.schema.to_schema` and `vertebra.utils.schema.to_schema` default to `normalize_nested=False` [^src-pyproject].
- **Data Dictionaries & ADK**: All eight adapters subclass both sync and async upstream `sqlspec.data_dictionary.dialects.<dialect>.dictionary` classes [^src-sqlspec-mysql-dict] [^src-sqlspec-pg-dict]. Copied fallback ADK stores are removed; ADK memory and artifact stores require `google-adk` (`vertebra-core[adk]`) via `ensure_module` [^src-sqlspec-postgres-adk].
- **Oracle Sessionless Gap & TxEventQ**: `vertebra._oracle` lacks native sessionless transaction FFI hooks, so `OracleSyncDriver` / `OracleAsyncDriver` raise `DialectNotSupportedError` on sessionless calls against `vertebra._oracle` [^src-sqlspec-oracle-driver]. `Oracle*EventsBackend` commits `DBMS_AQ.DEQUEUE` on dequeue and re-publishes on `nack()` [^src-sqlspec-oracle-events].
- **Session Reset Bounds (`reset_session_on_release=True`)**: BigQuery skips stateless REST connections and calls `BQ.ABORT_SESSION` only when a `session_id` exists; Spanner clears only local transaction flags; DuckDB drops temporary objects/non-baseline catalogs and restores pragmas; MSSQL executes `EXEC sp_reset_connection` as a T-SQL batch rather than setting a TDS packet flag [^src-sqlspec-bigquery-typing] [^src-sqlspec-spanner-typing] [^src-sqlspec-duckdb-typing] [^src-sqlspec-mssql-typing].
- **Storage Aliases, Tenant Guards & Replica Cloud Auth**: `register_storage_alias` preserves pre-configured store instances across `clear_cache()` (rejecting extra kwargs on pre-configured `ObjectStoreBackend`) [^src-sqlspec-storage]. BigQuery and Spanner reject `tenant_predicate` / `tenant_id` with `NotSupportedError` [^src-sqlspec-bigquery-driver] [^src-sqlspec-spanner-driver]. `derive_pool` preserves `token_provider` and CA bundles across PostgreSQL/MySQL replicas, though AWS SigV4 providers bind one `endpoint_host` and need per-replica overrides [^src-sqlspec-postgres-config] [^src-sqlspec-mysql-config] [^src-dbapi-sync].
<!-- truth: end -->

## Operational Boundaries

### Cached-Statement Executor & Reconnect Caveats

- **Executor Registration**: [`BaseSyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/_driver.py)[^src-sqlspec-driver] and `BaseAsyncDriver` register `_run_cached_statement` via `set_cached_statement_executor(..., owner=self._cache_hit_owner())` across all eight adapters. Subclasses overriding any of SQLSpec's eight watched hooks fall back to the cursor path.
- **SQLite & DuckDB Reconnect Caveat**: `SQLite*Driver` and `DuckDB*Driver` bind the native client's `_fetch_rows` and `_run_dml` callables once at construction when no dynamic DuckDB secrets are configured. Reconnecting the native driver under an existing adapter leaves cached statements bound to the closed client until a new session adapter is constructed. With dynamic DuckDB secrets, cache hits route through `execute_with_description` / `execute_dml` so secret refresh still runs.
- **Insert IDs & Cancellation**: Both executor and cursor cache-hit paths preserve `last_inserted_id` on `INSERT` when rows change, normalize empty-string `lastrowid` to `None`, and reject `bool` sentinels (`BigQuery*`, `Spanner*`, and `Proxy*` override `resolve_last_inserted_id()` to return `None`). In `BaseAsyncDriver._run_cached_statement`, `asyncio.CancelledError` propagates unmapped.

### Schema Normalization, Data Dictionaries & ADK

- **Nested `msgspec.Struct` Defaults**: Driver `to_schema` and Vertebra's row hydrators (`_hydrate_rows_with_schema`, `BigQueryCore.hydrate_query_rows`, `SpannerCore.hydrate_query_rows`) default to `normalize_nested=True` (opt-out with `False`), whereas standalone `to_schema` helpers default to `False` [^src-pyproject].
- **Upstream Data Dictionaries & Reconcilers**: All eight adapters inherit upstream sync/async dialect dictionaries ([`mysql/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py)[^src-sqlspec-mysql-dict], [`postgres/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py)[^src-sqlspec-pg-dict]). All eight `reconcile.py` modules use public `sqlspec.extensions.events.resolve_backend_name` and `sqlspec.extensions.adk` helpers with zero private imports.
- **Mandatory `google-adk` Extra**: ADK stores load `sqlspec.extensions.adk` through `ensure_module(..., package="google-adk", extra="adk")` ([`postgres/adk.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py)[^src-sqlspec-postgres-adk]) and raise `MissingDependencyError` when absent.

### Engine Session Reset, Oracle & Tenant Guards

- **Oracle 26ai Sessionless Gap**: [`OracleSyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py)[^src-sqlspec-oracle-driver] and `OracleAsyncDriver` expose `begin/resume/suspend_sessionless_transaction` and `suspend_on_success=True`. Against `vertebra._oracle` (which lacks native sessionless FFI hooks), these raise `DialectNotSupportedError`; external or test connections exposing the hooks execute directly.
- **`reset_session_on_release=True` per Engine**:
  - **BigQuery** ([`bigquery/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py)[^src-sqlspec-bigquery-typing]): Calls `reset_session()`/`reset()` if present or `CALL BQ.ABORT_SESSION(@session_id)` when a session is active; no remote call on stateless REST.
  - **Spanner** ([`spanner/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py)[^src-sqlspec-spanner-typing]): Calls `reset_session()`/`reset()` if present and clears local `_active_transaction`/`_in_transaction` flags (Spanner has no `RESET ALL`).
  - **DuckDB** ([`duckdb/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py)[^src-sqlspec-duckdb-typing]): Drops temporary objects in `temp.main`, detaches non-baseline databases, and re-applies baseline pragmas; permanent file-backed tables persist. Plain `:memory:` stays isolated per config, while `:memory:<name>` shares named catalogs.
  - **SQL Server** ([`mssql/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py)[^src-sqlspec-mssql-typing]): Calls `reset_session()`/`reset()` if present, else runs `EXEC sp_reset_connection` as a T-SQL batch (requires an open connection and execute permission; does not set the TDS `0x08` packet flag).
- **Oracle TxEventQ & Cloud Tenant Guards**: [`OracleSyncEventsBackend`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py)[^src-sqlspec-oracle-events] and `OracleAsyncEventsBackend` commit on dequeue and republish on `nack()`. [`BigQuery`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py)[^src-sqlspec-bigquery-driver] and [`Spanner`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py)[^src-sqlspec-spanner-driver] reject `tenant_predicate` and `tenant_id` with `NotSupportedError` before initialization.

### Storage Aliases & Cloud Auth Inheritance

- **Storage Aliases**: [`sqlspec/storage.py`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-sqlspec-storage] registers `"vertebra"` via `StorageRegistry.register_backend()`. Passing a live store or `ObjectStoreBackend` to `register_storage_alias` registers an instance alias surviving `clear_cache()`; passing `base_path` or extra kwargs with a pre-configured `ObjectStoreBackend` raises `ImproperConfigurationError`.
- **Connector Retries & Replica Cloud Auth**: PostgreSQL ([`postgres/config.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py)[^src-sqlspec-postgres-config]), MySQL ([`mysql/config.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py)[^src-sqlspec-mysql-config]), and MSSQL configs forward `connector_retry_*` into Python Cloud SQL/AlloyDB dialers (Rust `vertebra-postgres` `CloudConnector` issues a single bounded request within the connect deadline). `derive_pool` preserves `token_provider` and CA bundles (`ca_pem` on PG, `ssl_ca` on MySQL unless overridden in the replica URL). Distinct AWS replica endpoints require explicit SigV4 `token_provider` overrides because SigV4 binds one `endpoint_host:port`. In PEP 249 `connect_args` ([`dbapi/_sync.py`](../../../../../src/py/vertebra/db/dbapi/_sync.py)[^src-dbapi-sync]), PG `ca_pem` and MySQL `ssl_ca` accept `str`, while MSSQL `ca_pem` requires `bytes`.

[^src-sqlspec-driver]: `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` native cursor dispatch and statement config pass-through.
[^src-pyproject]: `pyproject.toml` — SQLSpec `>=0.74.1` PyPI dependency floor without upstream git pins.
[^src-sqlspec-mysql-dict]: `src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py` — MySQL data dictionary subclassing `sqlspec.data_dictionary.dialects.mysql.dictionary`.
[^src-sqlspec-pg-dict]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py` — PostgreSQL data dictionary subclassing `sqlspec.data_dictionary.dialects.postgres.dictionary`.
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
[^src-sqlspec-postgres-config]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py` — `PostgresSyncConfig` and `PostgresAsyncConfig` `derive_pool` cloud auth preservation.
[^src-sqlspec-mysql-config]: `src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py` — `MySQLSyncConfig` and `MySQLAsyncConfig` `derive_pool` cloud auth and `ssl_ca` preservation.
[^src-dbapi-sync]: `src/py/vertebra/db/dbapi/_sync.py` — PEP 249 `connect_args` allowlists and `+vtb` scheme normalization.
